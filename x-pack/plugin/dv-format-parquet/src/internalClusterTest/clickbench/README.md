# Helper scripts for the ClickBench checks of the Parquet doc values format

These scripts compute, with DuckDB (and pyarrow), the results that `ParquetClickBenchManualIT` compares with. The test is manual: it needs
the real `hits.parquet` of ClickBench (99,997,497 rows, about 14.8 GB) and it does not run in `check`. Nothing in this directory runs as
part of the build.

```
python3 -m venv venv && venv/bin/pip install -r requirements.txt
```

## Our own ClickBench-style queries (`testQueriesFromFileGiveWhatDuckDbComputes`)

```
venv/bin/python clickbench_expected.py ~/temp/hits.parquet queries.json [name-prefix ...]
./gradlew :x-pack:plugin:dv-format-parquet:internalClusterTest --tests '*ParquetClickBenchManualIT.testQueriesFromFileGiveWhatDuckDbComputes' \
  "-Dtests.jvm.argline=-Dtests.parquet.hits=$HOME/temp/hits.parquet -Dtests.parquet.queries=$PWD/queries.json" -Dtests.heap.size=16G
```

## The queries of the internal Rally track (`testRallyTrackQueriesGiveWhatDuckDbComputes`)

The ES|QL queries come from `clickbench/operations/02_all_queries.json` of the private `elastic/rally-internal-tracks` repository. We do not
copy that file here, so fetch and parse it first. The SQL for DuckDB (in `internal_expected.py`) is ours and it keeps the ties of the sort keys,
so the check accepts every valid choice among them.

```
gh api repos/elastic/rally-internal-tracks/contents/clickbench/operations/02_all_queries.json \
  -H "Accept: application/vnd.github.raw" > 02_all_queries.json
python3 parse_rally_queries.py 02_all_queries.json rally_queries.json
venv/bin/python internal_expected.py ~/temp/hits.parquet rally_queries.json internal_expected.json [name-prefix ...]
./gradlew :x-pack:plugin:dv-format-parquet:internalClusterTest --tests '*ParquetClickBenchManualIT.testRallyTrackQueriesGiveWhatDuckDbComputes' \
  "-Dtests.jvm.argline=-Dtests.parquet.hits=$HOME/temp/hits.parquet -Dtests.parquet.internal=$PWD/internal_expected.json" -Dtests.heap.size=16G
```

Add `-Dtests.timeoutSuite=3600000` when the test repeats queries a lot. More options of the test (all system properties, passed in the
same `-Dtests.jvm.argline`): `tests.parquet.queries.only`, `.repeat`, `.parallel`, `tests.parquet.pragmas`, `tests.parquet.noQueryCache`,
`tests.parquet.noPartial`, `tests.parquet.profile`.

## `constants/`

Small scripts that print the numbers that some tests of the plugin hard-code for `hits.parquet` (row counts, footer statistics, counts for a
few filters, checksums of string columns). Run them again when we change which file or which row groups a test uses.
