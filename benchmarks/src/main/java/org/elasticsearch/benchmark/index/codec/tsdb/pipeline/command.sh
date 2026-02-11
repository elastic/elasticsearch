#
# Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
# or more contributor license agreements. Licensed under the "Elastic License
# 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
# Public License v 1"; you may not use this file except in compliance with, at
# your election, the "Elastic License 2.0", the "GNU Affero General Public
# License v3.0 only", or the "Server Side Public License, v 1".
#

./benchmarks/src/main/java/org/elasticsearch/benchmark/index/codec/tsdb/run-pipeline-bench.sh \
-o "bench-run-20260207-022122-class-subset-512" \
-d sensor-2dp-double,realistic-gauge-double,sparse-gauge-double,step-hold-double,timestamp-as-double,percentage-rounded-1dp-double,mixed-sign-double,burst-spike-double,stable-sensor-double,counter-with-resets-double,financial-2dp-double,random-double,monotonic-double \
-p quantize-1e2-delta-offset-gcd-bitpack,quantize-1e6-delta-offset-gcd-bitpack,quantize-1e12-delta-offset-gcd-bitpack,quantize-1e6-alp_double_stage-offset-gcd-bitpack \
-b 512 -w 5 -i 5 -f 1

./benchmarks/src/main/java/org/elasticsearch/benchmark/index/codec/tsdb/run-pipeline-bench.sh \
-o "bench-run-20260207-140302-class-subset-128" \
-d sensor-2dp-double,realistic-gauge-double,sparse-gauge-double,step-hold-double,timestamp-as-double,percentage-rounded-1dp-double,mixed-sign-double,burst-spike-double,stable-sensor-double,counter-with-resets-double,financial-2dp-double,random-double,monotonic-double \
-p quantize-1e2-delta-offset-gcd-bitpack,quantize-1e6-delta-offset-gcd-bitpack,quantize-1e12-delta-offset-gcd-bitpack,quantize-1e6-alp_double_stage-offset-gcd-bitpack \
-b 128 -w 5 -i 5 -f 1

### ZSTD Variants

./benchmarks/src/main/java/org/elasticsearch/benchmark/index/codec/tsdb/run-pipeline-bench.sh \
-o "bench-run-20260207-022122-class-subset-512" \
-d sensor-2dp-double,realistic-gauge-double,sparse-gauge-double,step-hold-double,timestamp-as-double,percentage-rounded-1dp-double,mixed-sign-double,burst-spike-double,stable-sensor-double,counter-with-resets-double,financial-2dp-double,random-double,monotonic-double \
-p zstd,alp_double_stage-offset-gcd-zstd,alp_double_stage-zstd,quantize-1e6-delta-offset-gcd-zstd,quantize-1e6-alp_double_stage-zstd \
-b 512 -w 5 -i 5 -f 1

./benchmarks/src/main/java/org/elasticsearch/benchmark/index/codec/tsdb/run-pipeline-bench.sh \
-o "bench-run-20260207-140302-class-subset-128" \
-d sensor-2dp-double,realistic-gauge-double,sparse-gauge-double,step-hold-double,timestamp-as-double,percentage-rounded-1dp-double,mixed-sign-double,burst-spike-double,stable-sensor-double,counter-with-resets-double,financial-2dp-double,random-double,monotonic-double \
-p zstd,alp_double_stage-offset-gcd-zstd,alp_double_stage-zstd,quantize-1e6-delta-offset-gcd-zstd,quantize-1e6-alp_double_stage-zstd \
-b 128 -w 5 -i 5 -f 1
