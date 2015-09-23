require 'java'
require 'set'

java_package 'samza.examples.wikipedia.task'

java_import 'org.apache.samza.config.Config'
java_import 'org.apache.samza.system.IncomingMessageEnvelope'
java_import 'org.apache.samza.system.OutgoingMessageEnvelope'
java_import 'org.apache.samza.system.SystemStream'
java_import 'org.apache.samza.task.InitableTask'
java_import 'org.apache.samza.task.MessageCollector'
java_import 'org.apache.samza.task.StreamTask'
java_import 'org.apache.samza.task.TaskContext'
java_import 'org.apache.samza.task.TaskCoordinator'
java_import 'org.apache.samza.task.WindowableTask'

class WikipediaStatsStreamTask
  java_implements StreamTask, InitableTask, WindowableTask

  def initialize
    @edits    = 0
    @byteDiff = 0
    @titles   = Set.new
    @counts   = {}
  end

  java_signature 'void init(Config, TaskContext)'
  def init(config, context)
    @store = context.getStore 'wikipedia-stats'
  end

  java_signature 'void process(IncomingMessageEnvelope, MessageCollector, TaskCoordinator)'
  def process(envelope, collector, coordinator)
    edit  = envelope.getMessage
    flags = edit['flags']

    editsAllTime = @store.get('count-edits-all-time') || 0
    @store.put 'count-edits-all-time', (editsAllTime + 1).to_java(:int)

    @edits    += 1
    @titles   << edit['title']
    @byteDiff += edit['diff-bytes']

    flags.each do |key,value|
      @counts[key] = (@counts[key] || 0) + 1 if value
    end
  end

  java_signature 'void window(MessageCollector, TaskCoordinator)'
  def window(collector, coordinator)
    @counts['edits'         ] = @edits
    @counts['bytes-added'   ] = @byteDiff
    @counts['unique-titles' ] = @titles.size
    @counts['edits-all-time'] = @store.get 'count-edits-all-time'

    collector.send OutgoingMessageEnvelope.new(SystemStream.new("kafka", "wikipedia-stats"), @counts)

    # Reset counts after windowing.
    @edits    = 0
    @byteDiff = 0
    @titles   = Set.new
    @counts   = {}
  end
end
