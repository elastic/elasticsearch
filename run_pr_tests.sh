#!/bin/bash
set -e

./gradlew :x-pack:plugin:esql:test \
  --tests "org.elasticsearch.xpack.esql.analysis.AnalyzerUnmappedGoldenTests" \
  --tests "org.elasticsearch.xpack.esql.analysis.AnalyzerUnmappedTests" \
  --tests "org.elasticsearch.xpack.esql.analysis.VerifierTests"

./gradlew :x-pack:plugin:esql:internalClusterTest \
  --tests "org.elasticsearch.xpack.esql.CsvIT"

./gradlew :x-pack:plugin:yamlRestTest \
  --tests "org.elasticsearch.xpack.test.rest.XPackRestIT.test {p0=esql/192_unmapped_load_partial_non_keyword/*}" \
  --tests "org.elasticsearch.xpack.test.rest.XPackRestIT.test {p0=esql/260_flattened_subfield/*}"
