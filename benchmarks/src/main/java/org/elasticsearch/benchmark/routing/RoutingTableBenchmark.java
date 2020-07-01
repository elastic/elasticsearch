/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.benchmark.routing;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.concurrent.TimeUnit;

@Fork(3)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@SuppressWarnings("unused") // invoked by benchmarking framework
public class RoutingTableBenchmark {
    // Do NOT make any field final (even if it is not annotated with @Param)! See also
    // http://hg.openjdk.java.net/code-tools/jmh/file/tip/jmh-samples/src/main/java/org/openjdk/jmh/samples/JMHSample_10_ConstantFold.java

    // we cannot use individual @Params as some will lead to invalid combinations which do not let the benchmark terminate. JMH offers no
    // support to constrain the combinations of benchmark parameters and we do not want to rely on OptionsBuilder as each benchmark would
    // need its own main method and we cannot execute more than one class with a main method per JAR.
    @Param({
        //  indices|  shards|  replicas
        "       100|      10|        0",
        "       100|      30|        0",
        "       100|     100|        0",
        "      1000|      10|        0",
        "      1000|      30|        0",
        "      1000|     100|        0",

        "       100|      10|        0",
        "       100|      30|        0",
        "       100|     100|        0",
        "      1000|      10|        0",
        "      1000|      30|        0",
        "      1000|     100|        0",

        "       100|      10|        1",
        "       100|      30|        1",
        "       100|     100|        1",
        "      1000|      10|        1",
        "      1000|      30|        1",
        "      1000|     100|        1",

        "       100|      10|        2",
        "       100|      30|        2",
        "       100|     100|        2",
        "      1000|      10|        2",
        "      1000|      30|        2",
        "      1000|     100|        2",

        "       100|      10|        0",
        "       100|      30|        0",
        "       100|     100|        0",
        "      1000|      10|        0",
        "      1000|      30|        0",
        "      1000|     100|        0",

        "       100|      10|        1",
        "       100|      30|        1",
        "       100|     100|        1",
        "      1000|      10|        1",
        "      1000|      30|        1",
        "      1000|     100|        1",

        "       100|      10|        2",
        "       100|      30|        2",
        "       100|     100|        2",
        "      1000|      10|        2",
        "      1000|      30|        2",
        "      1000|     100|        2"})
    public String indicesShardsReplicas = "10|1|0";
    private RoutingTable initialRoutingTable;

    @Setup
    public void setUp() throws Exception {
        final String[] params = indicesShardsReplicas.split("\\|");

        int numIndices = toInt(params[0]);
        int numShards = toInt(params[1]);
        int numReplicas = toInt(params[2]);

        Metadata.Builder mb = Metadata.builder();
        for (int i = 1; i <= numIndices; i++) {
            mb.put(
                IndexMetadata.builder("test_" + i)
                    .settings(Settings.builder().put("index.version.created", Version.CURRENT))
                    .numberOfShards(numShards)
                    .numberOfReplicas(numReplicas)
            );
        }
        Metadata metadata = mb.build();
        RoutingTable.Builder rb = RoutingTable.builder();
        for (int i = 1; i <= numIndices; i++) {
            rb.addAsNew(metadata.index("test_" + i));
        }
        initialRoutingTable = rb.build();
    }

    private int toInt(String v) {
        return Integer.valueOf(v.trim());
    }

    @Benchmark
    public void shardsWithState() {
        RoutingTable routingTable = initialRoutingTable;
        routingTable.shardsWithState(ShardRoutingState.UNASSIGNED);
    }
}
