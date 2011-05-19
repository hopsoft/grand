require "mysql2"
require "digest/md5"

module Grand
  class Schema
    attr_reader :tables, :version, :config

    def initialize(config={})
      @config = config
      db_client = config[:db_client] || Mysql2::Client.new(config[:database])
      table_versions = []

      @tables = {}
      db_client.query("show tables").each(:as => :array) do |row|
        @tables[row.first] = {}
      end

      @tables.keys.each do |table|
        @tables[table][:column_names] = []
        @tables[table][:expected_primary_key] = {}
        @tables[table][:expected_primary_key][:name] = config[:primary_key]
        @tables[table][:expected_timestamp] = {}
        @tables[table][:expected_timestamp][:name] = config[:timestamp]
        timestamp_ok = false

        db_client.query("show columns from #{table}").each do |row|
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
        @tables[table][:version] = Digest::MD5.hexdigest(@tables[table][:column_names].to_s)
        table_versions << @tables[table][:version]

        db_client.query("show indexes from #{table}").each do |row|
          if row["Column_name"] == config[:timestamp] && timestamp_ok
            @tables[table][:expected_timestamp][:valid] = true
          end
        end
      end

      @version = Digest::MD5.hexdigest(table_versions.join)
      @tables
    end

  end
end
