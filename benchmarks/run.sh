#!/bin/bash
#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".
#

EXTRA=""
POSITIONAL_ARGS=()
while [[ $# -gt 0 ]]; do
  case $1 in
    --test)
      # Get inaccurate results quickly by shortening all measurements
      # to 50ms each and skip self tests.
      EXTRA="-r 50ms -w 50ms -jvmArgsAppend -DskipSelfTest=true"
      shift
      ;;
    *)
      POSITIONAL_ARGS+=("$1")
      shift
      ;;
  esac
done

set -- "${POSITIONAL_ARGS[@]}"

run() {
  ../gradlew run --args "$2 -rf json $EXTRA"
  mv jmh-result.json build/benchmarks/$1.json
}

cd "$(dirname "$0")"
mkdir -p build/benchmarks
run 'esql_agg' 'AggregatorBenchmark -pgrouping=none,longs -pfilter=none -pblockType=vector_longs,half_null_longs'
run 'esql_block_keep_mask' 'BlockKeepMaskBenchmark -pdataTypeAndBlockKind=BytesRef/array,BytesRef/vector,long/array,long/vector'
run 'esql_block_read' 'BlockReadBenchmark -paccessType=sequential'
run 'esql_eval' 'EvalBenchmark'
run 'esql_parse_ip' 'ParseIpBenchmark'
run 'esql_topn' 'TopNBenchmark'
run 'esql_values_agg' 'ValuesAggregatorBenchmark'
run 'esql_values_source_reader' 'ValuesSourceReaderBenchmark'
