# Log config creator

A tool that generates ingest configurations given _only_ a sample log file and
_no other hints_ about the format.

## What it does

The tool will recognize an appropriate character encoding (UTF-8, UTF-16LE,
ISO-8859-2, etc.) for the sample file.

Then the tool will recognize the high level structure of the sample file as one
of:

* ND-JSON
* XML
* CSV
* TSV
* Semi-structured textual log messages

For CSV and TSV the tool will guess whether the first row contains the field
names, based on similarity with the other rows in the file.  If the first row
of a CSV or TSV file doesn't contain the field names then they'll be named
generically using "column1", "column2", etc.

The tool then detects the most likely date field and format.  For
semi-structured textual log messages it makes an assumption that the way to
divide the lines into messages is that the first line of each message contains
the date; lines not containing the date are assumed to be extensions of the
previous message.

For structured formats the tool makes a guess at the best mapping type for each
field.

For semi-structured textual log messages the tool checks whether some
distinctive Grok patterns match in addition to the date.  It then creates
mappings for these fields, plus the date.  (It's obviously nowhere near as good
as a human would be at recognizing the most appropriate field boundaries.)

Finally, if the beats repo or a Filebeat installation is available, the tool
checks whether any of the pre-defined Filebeat modules match the sample file,
and incorporates the appropriate configurations if it finds a match.

The outputs consist of:

* Filebeat and Logstash configurations for ingesting the sample file via
  Filebeat and Logstash
* A Logstash configuration for ingesting the sample file directly
* Filebeat and Ingest Pipeline configurations for ingesting the sample file via
  Filebeat and Ingest Pipeline (this is not possible for XML, CSV and TSV as
  Ingest Pipeline doesn't support these formats)
* Appropriate Elasticsearch mappings

The mappings and Ingest Pipeline configurations are written in both `curl` and
Kibana console formats.

All the generated configurations assume X-Pack Security is disabled, however, it
should be pretty robotic to add the necessary pieces if it is enabled.

## Usage

The tool works best if the Filebeat module directory is available to it.

One way to get this is to clone the
[beats repo](http://github.com/elastic/beats) as a sub-directory of your home
directory:

```
$ cd
$ git clone git@github.com:elastic/beats.git
```

Another way is to install Filebeat on your machine.  If this install is in the
default location on Windows or Linux, or is directly under your home directory
then the tool will find it automatically.

Alternatively, you can specify the path to the Filebeat module directory on the
command line of the tool using the `-b` option.

Or, if you really don't want do any of these things, the tool will still work,
but just won't be able to tell you when there's a Filebeat module that would
work well with your sample log file.

It's best to use Java 8, because then you'll be able to run `logstash` from the
same shell with the same `JAVA_HOME`.  However, you can use Java 10 for this
tool if you prefer (and run `logstash` in a different shell that's set to use
Java 8).

The most basic usage after extracting the tarball is:

```
$ export JAVA_HOME=/path/to/java/home
$ cd lcc/bin
$ ./log-config-creator my_sample_log_file.log
```

This will (hopefully) deduce the format of your log file and write out some
configuration files to ingest it.

The configuration files are prefixed with "xyz" by default.  You can change this
with the `-n` option.  For example:

```
$ ./log-config-creator -n farequote ~/farequote.csv
Wrote config file ./farequote-filebeat-to-logstash.yml
Wrote config file ./farequote-logstash-from-filebeat.conf
Wrote config file ./farequote-logstash-from-file.conf
Wrote config file ./farequote-index-mappings.console
Wrote config file ./farequote-index-mappings.sh
```

If you have Elasticsearch running locally on port 9200 without X-Pack Security
and have `curl` and `logstash` on your `PATH` you can then import the file as
follows:

```
$ ./farequote-index-mappings.sh
$ logstash -f farequote-logstash-from-file.conf
```

By default the "from-file" config imports into an index called "test".  If you
want to try the tool on multiple files you'll either need to delete the "test"
index between tests, or else use a different index name per test using the `-i`
option.  For example:

```
$ ./log-config-creator -n ls -i ls ~/logstash/logs/logstash-plain.log
An existing Filebeat module [logstash] looks appropriate; the sample file appears to be a [log] log
Wrote config file ./ls-filebeat-to-logstash.yml
Wrote config file ./ls-logstash-from-filebeat.conf
Wrote config file ./ls-logstash-from-file.conf
Wrote config file ./ls-filebeat-to-ingest-pipeline.yml
Wrote config file ./ls-ingest-pipeline-from-filebeat.console
Wrote config file ./ls-ingest-pipeline-from-filebeat.sh
Wrote config file ./ls-index-mappings.console
Wrote config file ./ls-index-mappings.sh
```

This time if you look in the "mappings" and "from-file" files you'll notice that
they're using an index called "ls".  Another point to note from this example is
that compared to the CSV example we have extra outputs.  This is because it's
possible to ingest semi-structured log files via Ingest Pipeline, but Ingest
Pipeline cannot understand CSV.

An example of importing the sample file to Elasticsearch using Filebeat and
Ingest Pipeline (assuming you've installed Filebeat 6.3 in your home directory)
is as follows:

```
$ ./log-config-creator -n syslog -o /tmp ~/Downloads/messages
An existing Filebeat module [system] looks appropriate; the sample file appears to be a [syslog] log
Wrote config file /tmp/syslog-index-mappings.console
Wrote config file /tmp/syslog-index-mappings.sh
Wrote config file /tmp/syslog-filebeat-to-logstash.yml
Wrote config file /tmp/syslog-logstash-from-filebeat.conf
Wrote config file /tmp/syslog-logstash-from-file.conf
Wrote config file /tmp/syslog-filebeat-to-ingest-pipeline.yml
Wrote config file /tmp/syslog-ingest-pipeline-from-filebeat.console
Wrote config file /tmp/syslog-ingest-pipeline-from-filebeat.sh
$ /tmp/syslog-ingest-pipeline-from-filebeat.sh
$ cd ~/filebeat-6.3.0-darwin-x86_64
$ rm -f data/registry
$ ./filebeat run --once -c /tmp/syslog-filebeat-to-ingest-pipeline.yml
```

If you get weird results for a particular file you might be able to narrow down
where it went wrong by running with `-v`.  For example:

```
$ ./log-config-creator ~/farequote.csv -v
Using character encoding [UTF-8], which matched the input with [15%] confidence - first [8kB] of input was pure ASCII
Not JSON because there was a parsing exception: [Unrecognized token 'time': was expecting 'null', 'true', 'false' or NaN at [Source: "time,airline,responsetime,sourcetype"; line: 1, column: 5]]
Not XML because there was a parsing exception: [ParseError at [row,col]:[1,1] Message: Content is not allowed in prolog.]
First row is unusual based on length test: [33.0] and [count=999, min=36.000000, average=39.648649, max=42.000000]
First sample timestamp match [Tuple [v1=time, v2=index = 4, preface = '', date formats = [ 'YYYY-MM-dd HH:mm:ssZ' ], simple pattern = '\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', grok pattern = 'TIMESTAMP_ISO8601', epilogue = '', has fractional component smaller than millisecond = false]]
Guessing timestamp field is [time] with format [index = 4, preface = '', date formats = [ 'YYYY-MM-dd HH:mm:ssZ' ], simple pattern = '\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}', grok pattern = 'TIMESTAMP_ISO8601', epilogue = '', has fractional component smaller than millisecond = false]
Rejecting type 'long' for field [responsetime] due to parse failure: [For input string: "132.2046"]
---
Wrote config file ./xyz-index-mappings.console
Wrote config file ./xyz-index-mappings.sh
Wrote config file ./xyz-filebeat-to-logstash.yml
Wrote config file ./xyz-logstash-from-filebeat.conf
Wrote config file ./xyz-logstash-from-file.conf
```

From this the skilled observer can deduce that farequote.csv is pure ASCII text
(so reading it as UTF-8 will work), is neither JSON nor XML, that the first row
contains the field names, that the timestamp format is ISO8601 and that
`responsetime` would be better typed as a `double` than a `long`.

But, better still, if you have a log file the tool doesn't work on send it to me
and I'll try to make it work.

There's a `-h` option for help:

```
$ ./log-config-creator -h
Log config creator

Non-option arguments:
file to be processed

Option                  Description
------                  -----------
-b, --beats-module-dir  path to filebeat module directory (default:
                          $HOME/beats/filebeat/module or from installed
                          filebeat)
-h, --help              show help
-i, --index             index for logstash direct from file config (default:
                          test)
-n, --name              name for this type of log file (default: xyz)
-o, --output            output directory (default: .)
-s, --silent            show minimal output
-v, --verbose           show verbose output
-z, --timezone          timezone for logstash direct from file input (default:
                          logstash server timezone)
```

The `-z` option is most useful if you've obtained a log file from a different
timezone and it contains timestamps that aren't valid in your current timezone.
For example, 1:30 AM on the last Sunday in March doesn't exist in the UK
(because the time jumps forward from 1 AM to 2 AM as daylight saving time
begins).  So if you're in the UK but are trying to import an Indian log file
from the last Sunday in March then the `-z` option can avoid the timestamp
parsing errors you'd otherwise get.

