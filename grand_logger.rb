require 'logger'

class GrandLogger < Logger
  def format_message(severity, timestamp, progname, msg)
    "#{timestamp.to_s(:db)} #{severity} #{msg}\n"
  end 
end

