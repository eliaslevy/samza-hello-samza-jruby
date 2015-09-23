require 'java'

java_package 'samza.examples.wikipedia.task'

java_import 'org.apache.samza.system.IncomingMessageEnvelope'
java_import 'org.apache.samza.system.OutgoingMessageEnvelope'
java_import 'org.apache.samza.system.SystemStream'
java_import 'org.apache.samza.task.MessageCollector'
java_import 'org.apache.samza.task.StreamTask'
java_import 'org.apache.samza.task.TaskCoordinator'
java_import 'samza.examples.wikipedia.system.WikipediaFeedEvent'

 # This task is very simple. All it does is take messages that it receives, and
 # sends them to a Kafka topic called wikipedia-raw.

class WikipediaFeedStreamTask 
	java_implements StreamTask

  OUTPUT_STREAM = SystemStream.new 'kafka', 'wikipedia-raw'

  java_signature 'void process(IncomingMessageEnvelope, MessageCollector, TaskCoordinator)'
  def process(envelope, collector, coordinator)
    outgoingMap = envelope.getMessage.to_hash
    collector.send OutgoingMessageEnvelope.new(OUTPUT_STREAM, outgoingMap)
  end
end
