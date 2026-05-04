# Local Prometheus -> Elasticsearch remote_write

This setup runs Prometheus and Kibana in Docker and starts Elasticsearch from source.
Prometheus scrapes itself and forwards samples to Elasticsearch via the Prometheus remote write endpoint.

## Files

- `prometheus.yml` configures `scrape_configs` and `remote_write`.
- `docker-compose.yml` runs Prometheus, Kibana, and Grafana behind a [Traefik](https://traefik.io/) reverse proxy for convenient local URLs.

## Start

Start Elasticsearch from source and ensure it listens on a non-loopback interface
so that Prometheus in Docker can reach it.

```bash
./gradlew run --configuration-cache \
    -Dtests.es.http.host=0.0.0.0 \
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

- Prometheus UI: `http://prometheus.localhost`
- Targets page: `http://prometheus.localhost/targets` (the `prometheus` job should be `UP`)
- Kibana UI: `http://kibana.localhost`
- Grafana UI: `http://grafana.localhost` (admin/password)
  - Elasticsearch is pre-configured as a Prometheus data source
  - A Prometheus overview dashboard is automatically imported on first start
- Traefik dashboard: `http://traefik.localhost`
- Remote write health in Prometheus UI (example query):
  - `rate(prometheus_remote_storage_samples_total[1m])`
- Query PromQL in Kibana Discover (example query):
  - `PROMQL step=1m rate(prometheus_remote_storage_samples_total[1m])`

> **Note:** The `.localhost` hostnames are resolved by most browsers to `127.0.0.1` without any `/etc/hosts` changes.
