# Metrics in Elasticsearch

Elasticsearch has the metrics API available in server's package
`org.elasticsearch.telemetry.metric`.
This package contains base classes/interfaces for creating and working with metrics.
Please refer to the javadocs provided in these classes in that package for more details.
The entry point for working with metrics is `MeterRegistry`.

## Implementation
We use elastic's apm-java-agent as an implementation of the API we expose.
the implementation can be found in `:modules:apm`
The apm-java-agent is responsible for buffering metrics and upon metrics_interval
send them over to apm server.
Metrics_interval is configured via a `telemetry.agent.metrics_interval` setting
The agent also collects a number of JVM metrics.
see https://www.elastic.co/guide/en/apm/agent/java/current/metrics.html#metrics-jvm


## How to choose an instrument

The choice of the right instrument is not always easy as often differences are subtle.
The simplified algorithm could be as follows:

1. You want to measure something (absolute value)
    1. values are non-additive
        1.  use a gauge
        2.  Example: a cpu temperature
    2. values are additive
        1. use asynchronous counter
        2. Example: total number of requests
2. You want to count something
    1. values are monotonously increasing
        1. use a counter
        2. Example: Recording a failed authentication count
    2. values can be decreased
        1. use UpDownCounter
        2. Example: Number of orders in a queue
3. You want to record a statistics
    1. use a histogram
        1. Example: A statistics about how long it took to access a value from cache

refer to https://opentelemetry.io/docs/specs/otel/metrics/supplementary-guidelines/#instrument-selection
for more details

## How to name an instrument
See the naming guidelines for metrics:
[NAMING GUIDE](NAMING.md)

### Restarts and overflows
if the instrument is correctly chosen, the apm server will be able to determine if the metrics
were restarted (i.e. node was restarted) or there was a counter overflow
(the metric in ES might use an int internally, but apm backend might have a long )

## How to use an instrument
There are 2 types of usages of an instrument depending on a type.
- For synchronous instrument (counter/UpDownCounter) we need to register an instrument with
  `MeterRegistry` and use the returned value to increment a value of that instrument
```java
  MeterRegistry registry;
  LongCounter longCounter = registry.registerLongCounter("es.test.requests.count", "a test counter", "count");
  longCounter.increment();
  longCounter.incrementBy(1, Map.of("name", "Alice"));
  longCounter.incrementBy(1, Map.of("name", "Bob"));
```

- For asynchronous instrument (gauge/AsynchronousCounter) we register an instrument
  and have to provide a callback that will report the absolute measured value.
  This callback has to be provided upon registration and cannot be changed.
```java
MeterRegistry registry;
long someValue = 1;
registry.registerLongGauge("es.test.cpu.temperature", "the current CPU temperature as measured by psensor", "degrees Celsius",
() -> new LongWithAttributes(someValue, Map.of("cpuNumber", 1)));
```

If we don’t have access to ‘state’ that will be fetched on metric event (when callback is executed)
we can use a utility LongGaugeMetric or LongGaugeMetric
```java
MeterRegistry meterRegistry ;
LongGaugeMetric longGaugeMetric = LongGaugeMetric.create(meterRegistry, "es.test.gauge", "a test gauge", "total value");
longGaugeMetric.set(123L);
```
### The use of attributes aka dimensions
Each instrument can attach attributes to a reported value. This helps drilling down into the details
of value that was reported during the metric event


## Development

### Mock http server

The quickest way to verify that your metrics are working is to run `./gradlew run --with-apm-server`.
This will run ES node (or nodes in serverless) and also start a mock http server that will act
as an apm server. This fake http server will log all the http messages it receives from apm-agent

### With APM server in cloud
You can also run local ES node with an apm server in cloud.
Create a new deployment in cloud, then click the 'hamburger' on the left, scroll to Observability and click APM under it.
At the upper right corner there is `Add data` link, then scroll down to `ApmAgents` section and pick Java
There you should be able to see `elastic.apm.secret_token` and `elastic.apm.server_url. You will use them in the next step.

edit your `~/.gradle/init.d/apm.gradle` and replace the secret_token and the server_url.
```groovy
rootProject {
    if (project.name == 'elasticsearch' && Boolean.getBoolean('metrics.enabled')) {
        afterEvaluate {
            testClusters.matching { it.name == "runTask" }.configureEach {
                setting 'xpack.security.audit.enabled', 'true'
                keystore 'telemetry.secret_token', 'TODO-REPLACE'
                setting 'telemetry.metrics.enabled', 'true'
                setting 'telemetry.agent.server_url', 'https://TODO-REPLACE-URL.apm.eastus2.staging.azure.foundit.no:443'
            }
        }
    }
}
```

If you would like to add tracing add following settings to the above configuration:
```groovy
setting 'telemetry.tracing.enabled', 'true'
setting 'telemetry.agent.transaction_sample_rate', '1.0' //ensure every transaction is sampled
```

The example use:
```
./gradlew :run -Dmetrics.enabled=true
```

#### Logging
with any approach you took to run your ES with APM you will find apm-agent.json file
in ES's logs directory. If there are any problems with connecting to APM you will see WARN/ERROR messages.
We run apm-agent with logs at WARN level, so normally you should not see any logs there.

When running ES in cloud, logs are being also indexed in a logging cluster, so you will be able to find them
in kibana. The `datastream.dataset` is `elasticsearch.apm_agent`


### Testing
We currently provide a base `TestTelemetryPlugin` which should help you write an integration test.
See an example `S3BlobStoreRepositoryTests`




# Links and further reading
https://opentelemetry.io/docs/specs/otel/metrics/supplementary-guidelines/

https://www.elastic.co/guide/en/apm/guide/current/data-model-metrics.html
