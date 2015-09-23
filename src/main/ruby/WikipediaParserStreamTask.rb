# Encoding: utf-8
require 'java'

java_package 'samza.examples.wikipedia.task'

java_import 'org.apache.samza.system.IncomingMessageEnvelope'
java_import 'org.apache.samza.system.OutgoingMessageEnvelope'
java_import 'org.apache.samza.system.SystemStream'
java_import 'org.apache.samza.task.MessageCollector'
java_import 'org.apache.samza.task.StreamTask'
java_import 'org.apache.samza.task.TaskCoordinator'
java_import 'samza.examples.wikipedia.system.WikipediaFeedEvent'

class WikipediaParserStreamTask
  java_implements StreamTask

  java_signature 'void process(IncomingMessageEnvelope, MessageCollector, TaskCoordinator)'
  def process(envelope, collector, coordinator)
    #jsonObject = envelope.getMessage()
    #event = new WikipediaFeedEvent.from_json jsonObject
    event = envelope.getMessage()

    begin
      parsedJsonObject = self.class.parse event['msg']

      parsedJsonObject['channel'] = event['channel']
      parsedJsonObject['source' ] = event['source' ]
      parsedJsonObject['time'   ] = event['time'   ]

      collector.send OutgoingMessageEnvelope.new(SystemStream.new('kafka', 'wikipedia-edits'), parsedJsonObject)
    rescue Exception
      STDERR.puts "Unable to parse line: #{ event.to_string }"
    end
  end

  def  self.parse(line)
    p = Regexp.new "\\[\\[(.*)\\]\\]\\s(.*)\\s(.*)\\s\\*\\s(.*)\\s\\*\\s\\(\\+?(.\\d*)\\)\\s(.*)"
    m = p.match line

    if m && m.captures.size == 6
      title, flags, diffUrl, user, byteDiff, summary = m.captures

      {
        'title'           => title,
        'user'            => user,
        'unparsed-flags'  => flags,
        'diff-bytes'      => byteDiff.to_i,
        'diff-url'        => diffUrl,
        'summary'         => summary,
        'flags'           => {
          'is-minor'       => flags.include?('M'),
          'is-new'         => flags.include?('N'),
          'is-unpatrolled' => flags.include?('!'),
          'is-bot-edit'    => flags.include?('B'),
          'is-special'     => title.start_with?('Special:'),
          'is-talk'        => title.start_with?('Talk:')
        }
      }
    else
      raise ArgumentError
    end
  end

  java_signature 'static void main(String[])'
  def self.main(args)
    lines = [
      "[[Wikipedia talk:Articles for creation/Lords of War]]  http://en.wikipedia.org/w/index.php?diff=562991653&oldid=562991567 * BBGLordsofWar * (+95) /* Lords of War: Elves versus Lizardmen */]", 
      "[[David Shepard (surgeon)]] M http://en.wikipedia.org/w/index.php?diff=562993463&oldid=562989820 * Jacobsievers * (+115) /* American Revolution (1775�1783) */  Added to note regarding David Shepard's brothers"
    ]

    lines.each { |line| puts parse line }
  end
end
