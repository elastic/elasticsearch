# Local Prometheus -> Elasticsearch remote_write

This setup runs Prometheus and Kibana in Docker and starts Elasticsearch from source.
Prometheus scrapes itself and forwards samples to Elasticsearch via the Prometheus remote write endpoint.

## Files

- `prometheus.yml` configures `scrape_configs` and `remote_write`.
- `docker-compose.yml` runs Prometheus on port `9090` and Kibana on port `5601`.

## Start

Start Elasticsearch from source and ensure it listens on a non-loopback interface.

```bash
./gradlew run --configuration-cache \
    -Dtests.es.http.host=0.0.0.0 \
    -Dtests.es.xpack.security.enabled=false \
    -Dtests.es.xpack.ml.enabled=false \
    -Drun.license_type=trial \
    -Dtests.heap.size=4G \
    -Dtests.jvm.argline="-da -dsa -Dio.netty.leakDetection.level=simple"
```

```bash
cd dev-tools/prometheus-local
docker compose up -d
```

## Verify

- Prometheus UI: `http://localhost:9090`
- Targets page: `http://localhost:9090/targets` (the `prometheus` job should be `UP`)
- Kibana UI: `http://localhost:5601`
- Remote write health in Prometheus UI (example query):
  - `rate(prometheus_remote_storage_samples_total[1m])`
- Query PromQL in Kibana Discover (example query):
  - `PROMQL step=1m rate(prometheus_remote_storage_samples_total[1m])`

## Notes

- The `remote_write` URL points at `host.docker.internal`, which resolves to the host from Docker Desktop (macOS/Windows).
- Your Elasticsearch instance must listen on a non-loopback interface (for example `0.0.0.0:9200`).
- Kibana uses `docker.elastic.co/kibana/kibana:9.4.0-SNAPSHOT` and points to Elasticsearch at `http://host.docker.internal:9200`.
