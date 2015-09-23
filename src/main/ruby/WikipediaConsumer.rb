require 'java'

java_package 'samza.examples.wikipedia.system'

java_import 'org.apache.samza.Partition'
java_import 'org.apache.samza.metrics.MetricsRegistry'
java_import 'org.apache.samza.system.IncomingMessageEnvelope'
java_import 'org.apache.samza.system.SystemConsumer'
java_import 'org.apache.samza.system.SystemStreamPartition'
java_import 'org.apache.samza.system.filereader.FileReaderSystemConsumer'
java_import 'org.apache.samza.util.BlockingEnvelopeMap'

# The JRuby compiler does not support defining a Ruby class that inherits from a Java class.
# We therefore implement the SystemConsumer interface directly on this class and abuse the 
# FileReaderSystemConsumer class as a proxy for the BlockingEnvelopeMap poll implementation.

class WikipediaConsumer #< BlockingEnvelopeMap
  java_implements SystemConsumer

  def initialize(systemName, feed, registry)
    @channels   = []
    @systemName = systemName
    @feed       = feed
    @parent = FileReaderSystemConsumer.new systemName, registry, 10000, 500
  end

  def on_event(event)
    systemStreamPartition = SystemStreamPartition.new @systemName, event.channel, Partition.new(0)

    begin
      @parent.put systemStreamPartition, IncomingMessageEnvelope.new(systemStreamPartition, nil, nil, event)
    rescue Exception => e
      STDERR.puts e
    end
  end

  java_signature 'void register(SystemStreamPartition, String)'
  def register(systemStreamPartition, startingOffset)
    @parent.register systemStreamPartition, startingOffset
    @channels << systemStreamPartition.getStream
  end

  java_signature 'void start()'
  def start
    @feed.start
    @channels.each { |channel| @feed.listen channel, self }
  end

  java_signature 'void stop()'
  def stop
    @channels.each { |channel| @feed.unlisten channel, self }
    @feed.stop
  end

  java_signature 'java.util.Map<SystemStreamPartition, java.util.List<IncomingMessageEnvelope>> poll(java.util.Set<SystemStreamPartition>, long)'
  def poll(systemStreamPartition, timeout)
    @parent.poll systemStreamPartition, timeout
  end
end
