require File.expand_path(File.join(File.dirname(__FILE__), "grand"))

task :default do
  puts "Welcome to Grand! A file archiver for MySQL.\nHave a look at the 'archive' task."
end

desc <<-DESC
  Archives data in the database to CSV files.
  This operation does not change or delete data in the database.

  Options: 
  * end_date [Time.now] - The date where archiving terminates (or begins depending on how you look at it).
  * minutes  [10] ------- The number of minutes to archive.
                          The archiver reaches back this many minutes from the end_date.

  Examples: 
    rake archive
    rake archive[2011-05-15]
    rake archive[2011-05-15,30]
DESC
task :archive, :end_date, :minutes do |t, args|
  end_date = Time.parse(args[:end_date]) rescue nil
  end_date ||= Time.now
  end_date = end_date.change(:sec => 0)
  minutes = args[:minutes].to_i
  minutes = 10 if minutes == 0
  archive(:end_date => end_date, :minutes => minutes)
end
