
require 'java'

java_package 'samza.examples.wikipedia.system'

java_import 'org.apache.samza.SamzaException'
java_import 'org.apache.samza.config.Config'
java_import 'org.apache.samza.metrics.MetricsRegistry'
java_import 'org.apache.samza.system.SystemAdmin'
java_import 'org.apache.samza.system.SystemConsumer'
java_import 'org.apache.samza.system.SystemFactory'
java_import 'org.apache.samza.system.SystemProducer'
java_import 'org.apache.samza.util.SinglePartitionWithoutOffsetsSystemAdmin'
java_import 'samza.examples.wikipedia.system.WikipediaFeed'
java_import 'samza.examples.wikipedia.system.WikipediaConsumer'

class WikipediaSystemFactory
  java_implements SystemFactory

  java_signature 'SystemAdmin getAdmin(String, Config)'
  def getAdmin(systemName, config)
    SinglePartitionWithoutOffsetsSystemAdmin.new
  end

  java_signature 'SystemConsumer getConsumer(String, Config, MetricsRegistry)'
  def getConsumer(systemName, config, registry)
    host = config.get    "systems.#{ systemName }.host"
    port = config.getInt "systems.#{ systemName }.port"
    feed = WikipediaFeed.new host, port

    WikipediaConsumer.new systemName, feed, registry
  rescue Exception => e
    puts "#{e.class}: #{e}"
    raise
  end

  java_signature 'SystemProducer getProducer(String, Config, MetricsRegistry)'
  def getProducer(systemName, config, registry)
    raise SamzaException.new "You can't produce to a Wikipedia feed! How about making some edits to a Wiki, instead?"
  end
end
