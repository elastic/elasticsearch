## Usage

```sh
uv run promcheck /path/to/grafana_X_distinct.csv
```

### Filtering

Mark errors that match the given regex as `skipped` instead of failing the test.

```sh
uv run promcheck /path/to/grafana_X_distinct.csv --samples 120 --seed 42 --filter-err '(not yet implemented|at this time)'
```



