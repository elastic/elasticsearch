# serverless-gitops — Deep Dive

## What It Is

`elastic/serverless-gitops` is the single source of truth for all services deployed to the Elastic Services Platform (serverless). It manages service definitions, versions, configurations, and per-project overrides — all deployed via ArgoCD using CUE configuration language.

Every change merged to this repo is applied to the entirety of the target environment in one shot. No progressive rollout at the gitops level — that's handled by the service's own deployment pipeline.

## Repository Structure

```
serverless-gitops/
  services/                    # One folder per service
    elasticsearch/             # ES service definition
    elasticsearch-controller/  # ES controller (manages ES instances)
    elasticsearch-autoscaler/  # Autoscaler service
    kibana/
    apm-ingest-service/
    ... (~100+ services)
  schemas/                     # CUE schemas defining what's valid
  global-values/               # Cross-service configuration
  overrides.default.yaml       # Local development overrides
  docs/
```

Each service folder contains:
- `service.yaml` — helm chart location, deployment targets, promotion config
- `versions.yaml` — git SHA per deployment target (dev, qa, staging, production)
- `values/` — per-environment and per-project helm value overrides

## How It Works

1. **Service definition** (`service.yaml`): points to a Helm chart in a source repo (e.g., `elastic/elasticsearch-controller`)
2. **Version pinning** (`versions.yaml`): each environment (dev, qa, staging, production) pins to a specific git SHA
3. **ArgoCD** picks up changes, renders Helm charts with the values, and deploys to Kubernetes
4. **Progressive deployment**: `gpctl` handles automatic promotion across deployment slices (canary → production)

## Per-Project Overrides — The Key Mechanism

This is what Jim was using. You can override Elasticsearch configuration per individual serverless project by project ID:

```yaml
# services/elasticsearch-controller/values/project-overrides/qa/aws/eu-west-1.yaml
projectOverrides:
  ba959d1ba9bb48388e41433fca592db2: |      # <-- project UUID
    jvmOptions:
      - "-Des.failure_store_feature_flag_enabled=true"
    fileSettings:
      cluster-settings:
        serverless.search:
          search_power_min: 100
          search_power_max: 100
```

This allows:
- **Per-project JVM options** — feature flags, debug settings
- **Per-project cluster settings** — autoscaling thresholds, limits, experimental features
- **Per-project resource allocation** — memory, replicas per tier (search/index)
- **Per-project autoscaler config** — custom scaling behavior

Overrides are layered by priority: cluster name → CSP + region → per-project.

## Autoscaler Configuration

The autoscaler has its own per-project overrides for resource allocation:

```yaml
# services/elasticsearch-autoscaler/values/project-overrides/qa/aws/eu-west-1.yaml
projectOverrides:
  fdcc15fa50bf4752aa5ccfcbcb8b7289: |
    elasticsearch:
      tiers:
        search:
          replicas: 1
          resources:
            memory: 8Gi
```

Key autoscaling settings (from the elasticsearch-controller default values):
- `serverless.autoscaling.search.shard_read_load.threshold: 0.05` — when to scale up search
- `serverless.autoscaling.search.sampler.shard_read_load_ewma_alpha: 0.05` — smoothing factor
- `serverless.autoscaling.memory_metrics.shard_memory_overhead: -1` — adaptive memory estimation
- `serverless.autoscaling.memory_metrics.adaptive_min_threshold.enabled: true` — adaptive thresholds

## What Jim Was Doing

Jim's PRs (#78207, #78222, #79062, #79946, #80427) were setting up a QA project with specific overrides to test how the autoscaler responds to search load — specifically testing whether his ES|QL search load attribution PRs (#141709, #141819, #142841) actually trigger autoscaling. He was:

1. Creating a QA project with fixed search tier resources
2. Lowering `read_load` thresholds to make the autoscaler more sensitive
3. Running vector search benchmarks against it
4. Observing whether the autoscaler scales up in response

This is the same mechanism that would need to work for external data source queries — the autoscaler needs to detect external I/O load and scale accordingly.

## Relevance to External Data Sources

For external data sources in serverless:
- **ES configuration** lives here — any feature flags, settings, or JVM options for external data sources would be toggled via this repo
- **Autoscaler** — external data source queries create I/O and CPU load that the autoscaler doesn't currently understand. Jim's work on search load attribution is the foundation, but external source I/O goes through `esql_worker` thread pool (not Lucene), so it may need separate load metrics.
- **Per-project overrides** — could be used to enable/disable external data sources per project, set resource limits, or configure S3/GCS credentials at the infrastructure level
- **Cluster state storage** — our CRUD API stores datasources/datasets in `Metadata.ProjectCustom`, which is per-project in serverless. This fits naturally.

## Key Files

- `services/elasticsearch-controller/service.yaml` — ES controller service definition
- `services/elasticsearch-controller/values/qa/default.yaml` — QA default settings (contains all cluster settings)
- `services/elasticsearch-controller/values/project-overrides/` — per-project overrides by env/csp/region
- `services/elasticsearch-autoscaler/service.yaml` — autoscaler service
- `services/elasticsearch-autoscaler/values/project-overrides/` — per-project autoscaler overrides
- `schemas/services/services.cue` — schema for service definitions
