hello-samza-jruby
=================

Hello Samza is a starter project for [Apache Samza](http://samza.apache.org/) jobs.  Hello Samza [JRuby](http://jruby.org) is a port of the project that demonstrates how to write Samza jobs in Ruby.

Please see [Hello Samza](http://samza.apache.org/startup/hello-samza/0.9/) to get started.

You'll need to use JRuby's 1.7 latest snapshot, as I fixed [an issue](https://github.com/jruby/jruby/commit/0c4eb281bf17f6e307e3055fa5b8dd6dd6151eee) that resulted in certain tasks implemented in Ruby failing to compile.  There doesn't appear to be a JRuby Maven repository with snapshot artifacts, so you will have to build your own JRuby artifacts from source until version 1.7.23 is released.

This example also demonstrates how to use Ruby gems within jobs.  See the POM file for how it's done.

### Pull requests and questions

[Hello Samza](http://samza.apache.org/startup/hello-samza/0.9/) is developed as part of the [Apache Samza](http://samza.apache.org) project. Please direct questions, improvements and bug fixes there. Questions about [Hello Samza](http://samza.apache.org/startup/hello-samza/0.9/) are welcome on the [dev list](http://samza.apache.org/community/mailing-lists.html) and the [Samza JIRA](https://issues.apache.org/jira/browse/SAMZA) has a hello-samza component for filing tickets.

### Contribution

To start contributing on [Hello Samza](http://samza.apache.org/startup/hello-samza/0.9/) first read [Rules](http://samza.apache.org/contribute/rules.html) and [Contributor Corner](https://cwiki.apache.org/confluence/display/SAMZA/Contributor%27s+Corner). Notice that [Hello Samza](http://samza.apache.org/startup/hello-samza/0.9/) git repository does not support git pull request.