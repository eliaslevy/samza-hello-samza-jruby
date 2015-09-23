require 'java'

require 'eventmachine'
require 'isaac/bot'
require 'set'
require 'json'

java_package 'samza.examples.wikipedia.system'

java_import 'java.io.IOException'
java_import 'java.lang.InterruptedException'
java_import 'org.apache.samza.SamzaException'

class WikipediaFeedEvent

  def initialize(time, channel, source, msg)
    @time    = time
    @channel = channel
    @source  = source
    @msg     = msg

    strip_colors
  end

  # Sigh. The JRuby compiler doesn't support attr_reader.
  def time   ; @time   ; end
  def channel; @channel; end 
  def source ; @source ; end
  def msg    ; @msg    ; end

  def to_string
    "WikipediaFeedEvent [time=#{ time }, channel=#{ channel }, source=#{ source }, msg=#{ msg }]"
  end

  def to_hash
    {
      'time'    => time,
      'channel' => channel,
      'source'  => source,
      'msg'     => msg
    }
  end

  private

  def strip_colors
    @msg.gsub!(/[\x02\x0f\x16\x1f\x12]|\x03(\d{1,2}(,\d{1,2})?)?/, '')
  end
end

class WikipediaFeed

  def initialize(host, port)
    @host       = host
    @port       = port
    @clisteners = cls = {}

    # Callbacks are executed in the context of the Isaac::Bot instance.
    feed = self
    @irc = Isaac::Bot.new do
      configure do |c|
        c.server = host
        c.port   = port
        c.nick   = "samza-bot-#{ Random.rand 2**31 }"
        #c.environment = :test
        #c.verbose     = true
      end

      on(:connect) { join(*cls.keys) }
      on(:channel) { feed.handle_msg channel, nick, message }
    end
  end

  def start
    Thread.new do
      begin
        EventMachine.run { @irc.start }
      rescue Exception => e
        puts "#{e.class}: #{e}"
        puts e.backtrace
        raise
      end
    end
    sleep 5
  end

  def stop
    @irc.quit
  end

  def listen(channel, listener)
    listeners = @clisteners[channel]

    if listeners.nil?
      listeners = Set.new
      @clisteners[channel] = listeners
      @irc.join channel
    end

    listeners << listener
  end

  def unlisten(channel, listener)
    listeners = @clisteners[channel]

    if listeners.nil?
      raise 'Trying to unlisten to a channel that has no listeners in it.'
    elsif ! listeners.include? listener
      raise 'Trying to unlisten to a channel that listener is not listening to.'
    end

    listeners.delete listener

    if listeners.empty?
      @clisteners.delete channel
      @irc.part channel
    end
  end

  def handle_msg(channel, sender, msg)
    listeners = @clisteners[channel]
    unless listeners.nil?
      event = WikipediaFeedEvent.new (Time.now.to_f*1000).to_i, channel, sender, msg
      listeners.each { |listener| listener.on_event event }
    end
  end

  java_signature 'static void main(String[]) throws InterruptedException'
  def self.main(args)
    feed = WikipediaFeed.new 'irc.wikimedia.org', 6667
    feed.start

    listener = Object.new.tap do |l|
      def l.on_event(event)
        puts event.to_string
      end
    end

    feed.listen '#en.wikipedia', listener

    sleep 20
    feed.stop
  end
end
