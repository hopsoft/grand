require "mysql2"
require "digest/sha1"
require "fileutils"

module Grand
  class Schema
    attr_reader :tables, :version, :config

    def initialize(config={})
      @config = config

      # attempt to use a cached version of the schema
      # caches the schema definition for 2 hours
      path = File.expand_path(File.join(File.dirname(__FILE__), "tmp"))
      FileUtils.mkdir_p(path)
      file_name = Digest::SHA1.hexdigest(config[:database].to_s)
      file_path = File.join(path, file_name)
      if File.exists?(file_path) && (Time.now - File.mtime(file_path)) < 7200
        data = nil
        File.open(file_path, "r") { |f| data = Marshal.load(f.read) }
        @tables = data.first
        @version = data.last
        return
      end rescue nil

      db_client = config[:db_client] || Mysql2::Client.new(config[:database])
      table_versions = []

      @tables = {}
      db_client.query("show tables").each(:as => :array) do |row|
        @tables[row.first] = {}
      end

      @tables.keys.each do |table|
        @tables[table][:columns] = []
        @tables[table][:indexes] = []
        @tables[table][:column_names] = []
        @tables[table][:expected_primary_key] = {}
        @tables[table][:expected_primary_key][:name] = config[:primary_key]
        @tables[table][:expected_timestamp] = {}
        @tables[table][:expected_timestamp][:name] = config[:timestamp]
        timestamp_ok = false

        db_client.query("show columns from #{table}").each do |row|
          @tables[table][:columns] << row
          @tables[table][:column_names] << row["Field"]

          if row['Field'] == config[:primary_key]
            if row["Key"] == "PRI" && row["Extra"] == "auto_increment"
              @tables[table][:expected_primary_key][:valid] = true
            end
          end

          if row["Field"] == config[:timestamp] && row["Type"] == "datetime"
            timestamp_ok = true
            @tables[table][:expected_timestamp][:exists] = true
          end
        end

        @tables[table][:column_names].sort!
        @tables[table][:version] = Digest::SHA1.hexdigest(@tables[table][:column_names].to_s)[0, 8]
        table_versions << @tables[table][:version]

        db_client.query("show indexes from #{table}").each do |row|
          @tables[table][:indexes] = row
          if row["Column_name"] == config[:timestamp] && timestamp_ok
            @tables[table][:expected_timestamp][:valid] = true
          end
        end
      end

      @version = Digest::SHA1.hexdigest(table_versions.join)[0, 8]

      # save the schema to the file system
      File.open(file_path, "w") do |f|
        f.write(::Marshal.dump([@tables, @version]))
      end rescue nil

      @tables
    end

  end
end
