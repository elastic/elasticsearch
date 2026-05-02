## Usage


### Get query corpus

```sh
git clone git@github.com:elastic/grafana-dashboards-analysis.git
```

### Run checker

```sh
uv run promcheck /path/to/grafana-dashboards-analysis/results/grafana_dashboards_100_distinct_queries.csv
```

Useful options:

[--filter-err]: Mark errors that match the given regex as `skipped` instead of failing the test. E.g.:

```sh
uv run promcheck /path/to/grafana_X_distinct.csv --samples 120 --seed 42 --filter-err '(not yet implemented|at this time)'
```

[--list]: List corpus queries

```sh
uv run promcheck /path/to/grafana-dashboards-analysis/results/grafana_dashboards_100_distinct_queries.csv --list
```


### Run tests

```sh
uv run pytest
```



