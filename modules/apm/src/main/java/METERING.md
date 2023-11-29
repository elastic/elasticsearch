# Metrics in Elasticsearch

Elasticsearch has the metrics API available in server's (perhaps we should move to lib?) package
`org.elasticsearch.telemetry.metric`.
This package contains base classes for creating and working with metrics.
Please refer to the javadocs provided in classes in that package for more details.
The entry point for working with metrics is `MeterRegistry`.

## Implementation
we use elastic's apm-java-agent as an implementation of the API we expose.
the implementation can be found in `:modules:apm`
The apm-java-agent is responsible for collecting metrics and upon metric_interval (todo setting)
send them over to apm server.

## How to choose an instrument

We support various instruments and might be adding more as we go.
The choice of the right instrument is not always easy as often differences are subtle.
The simplified algorithm could be as follows:
- want to measure something (absolute value)
  - non-additive values (like a CPUs temperature, it doesn't make sense to add them up) -> use a gauge
  - additive values (like total number of requests) - AsynchronousCounter (which expects a total to be provided)
- want to count something -> Counter/ UpDownCounter
- record statistics - use a histogram

refer to https://opentelemetry.io/docs/specs/otel/metrics/supplementary-guidelines/#instrument-selection
for more details

#### restarts and overflows
if the instrument is correctly chosen, the apm server will be able to determine if the metrics
were restarted (i.e. node was restarted) or there was a counter overflow
(the metric in ES might use an int internally, but apm backend might have a long )

### how to use an instrument
there are 2 types of usages of an instrument depending on a type.
- For synchronous instrument (counter/UpDownCounter) we need to register an instrument with
`MeterRegistry` and use the returned value to increment a value of that instrument
- For asynchronous instrument (gauge/AsynchronousCounter) we register an instrument
and have to provide a callback that will report the absolute measured value.
This callback has to be provided upon registration and cannot be changed. There is in fact
little use of keeping that reference later in code.

## Development
The quickest way to verify that your metrics are working is to run `./gradlew run --with-apm-server`.
This will run ES node (or nodes in serverless) and also start a fake http server that will act
as an apm server. This fake http server will log all the http messages it receives from apm-agent

You can also run local ES node with an apm server in cloud.
Create a new deployment in cloud, then click the 'hamburger :)' on the left, scroll to Observability and click APM under it.
At the upper right corner there is `Add data` link, then scroll down to `ApmAgents` section and pick Java
There you shoudl be able to see `elastic.apm.secret_token` and `elastic.apm.server_url. You will use them in next step.

Next you should create a file `apm_server_ess.gradle`
in a different directory than your elasticsearch checkout (so that branch changes don't remove it)
The content of the file:
```
rootProject {
  if (project.name == 'elasticsearch') {
    afterEvaluate {
      testClusters.matching { it.name == "runTask" }.configureEach {
        setting 'xpack.security.audit.enabled', 'true'
        keystore 'tracing.apm.secret_token', 'REDACTED'
        setting 'telemetry.metrics.enabled', 'true'

      setting 'tracing.apm.agent.server_url', 'https://REDACTED:443'
      }
    }
  }
}
```
Use the secret_token and server_url (REDACTED) from previous step.

you can run your local ES node with APM in ESS with this command
`./gradlew run -I ../apm_enable_statefull.gradle`

### serverless and cloud's apm server
For this you will have to use a slightly different content of your `apm_server_serverless.gradle` (I recommend creating 2 versions of the file)
```
rootProject {
  if (project.name == 'elasticsearch-serverless') {
    afterEvaluate {
      testClusters.matching { it.name == "runCluster" }.configureEach {
        setting 'tracing.apm.agent.server_url', 'https://REDACTED:443'
        extraConfigFile 'secrets/secrets.json', file("YOUR_PATH/secrets.json")
        setting 'telemetry.metrics.enabled', 'true'
      }
    }
  }
}
```
We don't use keystore in serverless so you will have to create a secret for this, so it can be read.
`secretes.json`:
```
{
  "metadata": {
    "version": "200",
    "compatibility": "8.4.0"
  },
    "string_secrets": {
      "tracing.apm.secret_token": "REDACTED"
    }
  }
}
```

Run it with `./gradlew run -I ../apm_server_serverless.gradle`

#### Logging
with any approach you took to run your ES with APM you will find apm-agent.json file
in ES's logs directory. If there are any problems with connecting to APM you will see WARN/ERROR messages.
We run apm-agent with logs at WARN level, so normally you should not see any logs there.

When running ES in cloud, logs are being also indexed in a logging cluster, so you will be able to find them
in kibana.

### Testing
We currently provide a base `TestTelemetryPlugin` which should help you write an integration test.
See an example `S3BlobStoreRepositoryTests`


# creating dashboards in kibana
..

# Links and further reading
https://opentelemetry.io/docs/specs/otel/metrics/supplementary-guidelines/

https://www.elastic.co/guide/en/apm/guide/current/data-model-metrics.html
