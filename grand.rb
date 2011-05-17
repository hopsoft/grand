require 'rubygems'
require 'bundler/setup'
require 'mysql2'
require 'fastercsv'
require 'yaml'
require 'active_support/all'
require 'fileutils'
require File.expand_path(File.join(File.dirname(__FILE__), 'grand_logger'))

Mysql2::Client.default_query_options[:database_timezone] = :utc
Mysql2::Client.default_query_options[:application_timezone] = :local
Mysql2::Client.default_query_options[:cache_rows] = false

# Returns a Hash for the configuration saved in config.yml
def config
  return @config if @config
  @config = YAML.load_file(File.expand_path(File.join(File.dirname(__FILE__), 'config.yml'))).symbolize_keys
  @config[:database].symbolize_keys!
  @config
end

def logger
  return @logger if @logger
  log_path = File.expand_path(File.join(File.dirname(__FILE__), 'log'))
  FileUtils.mkdir_p(log_path)
  @logger ||= GrandLogger.new(File.join(log_path, 'grand.log'))
end

# Returns a Mysql2::Client instance.
def db_client
  @db_client ||= Mysql2::Client.new(config[:database])
end

# Returns a lightweight Hash of meta data about the schema.
# This is just enough information to create the archive files.
#
# Example output:
#    { :my_table => {
#        :primary_key => {:name => 'id', :exists => true, :auto_increment => true, :ok => true, :columns => ['id', ...]},
#        :timestamp => {:name => 'updated_at', :exists => true, :indexed => true, :ok => true, :columns => ['id', ...]}}}
def schema
  logger.info "Obtaining schema information."
  return @schema if @schema

  @schema = {:tables => {}}
  db_client.query("show tables").each(:as => :array) do |row|
    @schema[:tables][row.first] = {}
  end

  @schema[:tables].keys.each do |table|
    info = @schema[:tables][table]
    info[:primary_key] = {:name => config[:primary_key]}
    info[:timestamp] = {:name => config[:time_stamp]}
    info[:columns] = []

    db_client.query("show columns from #{table}").each do |row|
      info[:columns] << row['Field']
      if row['Field'] == config[:primary_key]
        info[:primary_key][:exists] = true
        if row["Key"] == "PRI" && row["Extra"] == "auto_increment"
          info[:primary_key][:auto_increment] = true
          info[:primary_key][:ok] = true
        end
      end

      if row["Field"] == config[:timestamp] && row["Type"] == "datetime"
        info[:timestamp][:exists] = true
      end
    end

    info[:columns].sort!
    info[:queryable_columns] = info[:columns].map {|c| '`' + c + '`'}

    db_client.query("show indexes from #{table}").each do |row|
      if row["Column_name"] == config[:timestamp]
        info[:timestamp][:indexed] = true
        info[:timestamp][:ok] = true
      end
    end
  end

  @schema
end

# Creates the delimited files that have been updated within the time dimension specified.
def create_files(options={})
  start = Time.now
  logger.info "Archiving started."
  options[:minutes] ||= 5
  options[:end_date] ||= Time.now
  options[:start_date] ||= options[:end_date].advance(:minutes => (options[:minutes] * -1))
  options[:start_date] = options[:start_date].change(:seconds => 0)
  options[:end_date] = options[:end_date].change(:seconds => 0)

  schema[:tables].each do |table, info|
    if !info[:primary_key][:ok]
      msg = ["Unable to create file for #{table}!"]
      msg << "The auto incrementing primary key '#{config[:primary_key]}' does not exist for '#{table}'."
      logger.error msg.join(' ')
      next
    end

    if !info[:timestamp][:exists]
      msg = ["Unable to create file for #{table}!"]
      msg << "The timestamp column '#{config[:timestamp]}' does not exist for '#{table}'."
      logger.error msg.join(' ')
      next
    end

    path = File.expand_path(File.join(config[:path], table))
    file_name = "#{options[:start_date].strftime('%Y%m%d%H%M')}-#{options[:end_date].strftime('%Y%m%d%H%M')}"
    file_path = File.join(path, file_name)
    FileUtils.mkdir_p(path)
    records = get_records(table, info, options)

    FasterCSV.open(file_path, "w", :col_sep => config[:csv_delimiter]) do |csv|
      csv << info[:columns]
      records.each do |row|
        values = []
        info[:columns].each {|c| values << row[c]}
        csv << values
      end
    end

  end

  logger.info "Archiving completed in #{Time.now - start} seconds."
  nil
end

# @param [Integer] minutes Indicates the number of minutes to pull updates for.
def get_records(table, info, options={})
  records = []
  ids = []

  if info[:timestamp][:ok]
    query = "select #{config[:primary_key]} from #{table} where updated_at between '#{options[:start_date].to_s(:db)}' and '#{options[:end_date].to_s(:db)}'"
    logger.info "Querying #{table} with timestamp strategy."
    logger.info query
    db_client.query(query).each {|r| ids << r[config[:primary_key]]}
  else
    query = "select #{config[:primary_key]} from #{table} order by #{config[:primary_key]} desc limit 1"
    logger.info "Querying #{table} with primary key strategy."
    last_id = @db_client.query(query).first.values.first rescue nil

    while last_id
      query = "select #{config[:primary_key]}, #{config[:timestamp]} from #{table} where id < #{last_id} order by id desc limit #{config[:batch_size]}"
      logger.info query
      last_id = nil

      db_client.query(query).each do |row|
        next if row[config[:timestamp]].nil?
        if row[config[:timestamp]] >= options[:start_date]
          last_id = row[config[:primary_key]]
          ids << last_id
        end
      end
    end
  end

  if ids.length > 0
    chunk_count = ids.length / 1000
    if chunk_count == 0
      query = "select #{info[:queryable_columns].join(', ')} from #{table} where id in (#{ids.join(',')})"
      logger.info query
      db_client.query(query).each {|r| records << r}
    else
      chunk_size = ids.length / chunk_count
      ids.each_slice(chunk_size).map.each do |chunk|
        query = "select #{info[:queryable_columns].join(', ')} from #{table} where id in (#{chunk.join(',')})"
        logger.info query
        db_client.query(query).each {|r| records << r}
      end
    end
  end

  records
end

