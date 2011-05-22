require 'rubygems'
require 'bundler/setup'
require 'mysql2'
require 'fastercsv'
require 'yaml'
require 'active_support/all'
require 'fileutils'
require File.expand_path(File.join(File.dirname(__FILE__), 'logger'))
require File.expand_path(File.join(File.dirname(__FILE__), 'schema'))

# Configure the Mysql2 client
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

# Returns a logger for this script.
def logger
  return @logger if @logger
  log_path = File.expand_path(File.join(File.dirname(__FILE__), 'log'))
  FileUtils.mkdir_p(log_path)
  @logger ||= Grand::Logger.new(File.join(log_path, 'grand.log'))
end

# Returns a Mysql2::Client instance.
def db_client
  # @db_client ||= Mysql2::Client.new(config[:database])
  Mysql2::Client.new(config[:database])
end

# Returns a Hash containing enough schema meta data to build the archive files.
def schema
  @schema ||= Grand::Schema.new(config.merge(:db_client => db_client))
end

# Archives database inserts and updates to the file system.
# @param [Hash] options
# @option options [Fixnum] :minutes    The number of minutes to reach back for updates. (defaults to 5)
# @option options [Time]   :start_date The oldest date. (default is calculated using :minutes)
# @option options [Time]   :end_date   The most recent date. (defaults to Time.now)
def archive(options={})
  record_count = 0
  start = Time.now
  logger.info "Archiving started."
  options[:minutes] ||= 5
  options[:end_date] ||= Time.now
  options[:start_date] ||= options[:end_date].advance(:minutes => (options[:minutes] * -1))
  options[:start_date] = options[:start_date].change(:sec => 0)
  options[:end_date] = options[:end_date].change(:sec => 0)

  schema.tables.each do |table, info|
    path = config[:path]
    path = File.join(path, table) if config[:storage_strategy] == "structured"
    FileUtils.mkdir_p(path)

    # save schema information
    schemas_path = File.join(path, "schemas")
    file_name = "#{info[:version]}.yml"
    file_name = "#{table}-#{file_name}" if config[:storage_strategy] == "flat"
    file_path = File.join(schemas_path, file_name)
    if !File.exists?(file_path)
      FileUtils.mkdir_p(schemas_path)
      File.open(file_path, "w") do |file|
        file.write(info.to_yaml)
      end
    end

    if !info[:expected_primary_key][:valid]
      msg = ["Unable to create file for #{table}!"]
      msg << "The auto incrementing primary key '#{config[:primary_key]}' does not exist for '#{table}'."
      logger.error msg.join(' ')
      next
    end

    if !info[:expected_timestamp][:exists]
      msg = ["Unable to create file for #{table}!"]
      msg << "The timestamp column '#{config[:timestamp]}' does not exist for '#{table}'."
      logger.error msg.join(' ')
      next
    end

    file_name = "#{options[:start_date].strftime('%Y%m%d%H%M%S')}-#{options[:end_date].strftime('%Y%m%d%H%M%S')}-#{info[:version]}.csv"
    file_name = "#{table}-#{file_name}" if config[:storage_strategy] == "flat"
    file_path = File.join(path, file_name)
    ids, records = get_records(table, options)

    if records.length > 0
      logger.info "Creating archive file(s) for #{table}."

      # save inserts and updates to a file
      FasterCSV.open(file_path, "w", :col_sep => config[:csv_delimiter]) do |csv|
        csv << info[:column_names]
        records.each do |row|
          values = []
          info[:column_names].each {|c| values << prepare(row[c])}
          csv << values
          record_count += 1
        end
      end

      # save missing ids to a file as well
      # TODO: chunk the get_missing_ids queries so they run faster
      missing_ids = get_missing_ids(table, ids.first, ids.last)
      if missing_ids.length > 0
        file_path = File.join(path, "missing-#{file_name}")

        FasterCSV.open(file_path, "w", :col_sep => config[:csv_delimiter]) do |csv|
          csv << ["id"]
          missing_ids.each do |id|
            csv << [id]
          end
        end
      end

      # create a control file that indicaets that the csv is ready for pickup
      File.open(file_path.gsub(/\.csv/, ".ctl"), "w") {|f| f.write("#{records.length}\n")}
    end
  end

  logger.info "Archived #{record_count} records in #{Time.now - start} seconds."
  nil
end

private

def get_missing_ids(table, start_id, end_id)
  expected_ids = (start_id..end_id).to_a
  query = "select #{backtick(config[:primary_key])} from #{backtick(table)} where #{backtick(config[:primary_key])} between #{start_id} and #{end_id}"
  found_ids = db_client.query(query, :as => :array).map {|r| r.first}
  expected_ids - found_ids
end

def get_records(table, options={})
  records = []
  ids = []

  # escape names for use in queries
  tbl = backtick(table)
  pk = backtick(config[:primary_key])
  timestamp = backtick(config[:timestamp])
  columns = schema.tables[table][:column_names].map {|c| backtick(c)}


  if schema.tables[table][:expected_timestamp][:valid]
    # TODO: think about batching this query
    query = "select #{pk} from #{tbl} where updated_at between '#{options[:start_date].to_s(:db)}' and '#{options[:end_date].to_s(:db)}'"
    logger.info "Querying #{table} with timestamp strategy."
    logger.info query
    db_client.query(query).each {|r| ids << r[config[:primary_key]]}
  else
    query = "select #{pk} from #{tbl} order by #{pk} desc limit 1"
    logger.info "Querying #{table} with primary key strategy."
    logger.info query
    last_id = db_client.query(query, :as => :array).first.first rescue nil
    last_queried_id = nil

    empty_loop_count = 0
    while last_id && empty_loop_count <= config[:max_empty_loops]
      break if last_id == last_queried_id
      query = "select #{pk}, #{timestamp} from #{tbl} where id < #{last_id} order by id desc limit #{config[:batch_size]}"
      logger.info query
      current_id = last_id
      last_queried_id = last_id
      last_id = nil

      db_client.query(query).each do |row|
        current_id = row[config[:primary_key]]
        next if row[config[:timestamp]].nil?
        if row[config[:timestamp]] >= options[:start_date] && row[config[:timestamp]] <= options[:end_date]
          last_id = current_id
          ids << last_id
          empty_loop_count = 0
        end
      end

      if last_id.nil?
        empty_loop_count += 1
        last_id = current_id
      end
    end
  end

  if ids.length > 0
    chunk_count = ids.length / config[:batch_size]
    if chunk_count == 0
      query = "select #{columns.join(', ')} from #{tbl} where id in (#{ids.join(',')})"
      logger.info query
      db_client.query(query).each {|r| records << r}
    else
      chunk_size = ids.length / chunk_count
      ids.each_slice(chunk_size).map.each do |chunk|
        query = "select #{columns.join(', ')} from #{tbl} where id in (#{chunk.join(',')})"
        logger.info query
        db_client.query(query).each {|r| records << r}
      end
    end
  end

  logger.info "#{table}: ids=#{ids.length} records=#{records.count}"

  return ids, records
end

def backtick(value)
  '`' + value.to_s + '`'
end

# Preparse a value for storage in the CSV file.
# @param [Object] value The value to prepare
# @return [Object] The prepared value
def prepare(value)
  return value.to_s(:db) if value.is_a?(Time) || value.is_a?(DateTime)
  return value unless value.is_a?(String)
  value.gsub(/\n/, " ").gsub(config[:csv_delimiter], "\\#{config[:csv_delimiter]}")
end
