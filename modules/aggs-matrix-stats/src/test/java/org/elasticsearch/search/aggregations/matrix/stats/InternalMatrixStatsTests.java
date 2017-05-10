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
package org.elasticsearch.search.aggregations.matrix.stats;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class InternalMatrixStatsTests extends InternalAggregationTestCase<InternalMatrixStats> {

    @Override
    protected InternalMatrixStats createTestInstance(String name, List<PipelineAggregator> pipelineAggregators,
                                                     Map<String, Object> metaData) {
        int numFields = randomInt(128);
        String[] fieldNames = new String[numFields];
        double[] fieldValues = new double[numFields];
        for (int i = 0; i < numFields; i++) {
            fieldNames[i] = Integer.toString(i);
            fieldValues[i] = randomDouble();
        }
        RunningStats runningStats = new RunningStats();
        runningStats.add(fieldNames, fieldValues);
        MatrixStatsResults matrixStatsResults = randomBoolean() ? new MatrixStatsResults(runningStats) : null;
        return new InternalMatrixStats("_name", 1L, runningStats, matrixStatsResults, Collections.emptyList(), Collections.emptyMap());
    }

    @Override
    protected Writeable.Reader<InternalMatrixStats> instanceReader() {
        return InternalMatrixStats::new;
    }

    @Override
    public void testReduceRandom() {
        int numValues = 10000;
        int numShards = randomIntBetween(1, 20);
        int valuesPerShard = (int) Math.floor(numValues / numShards);

        List<Double> aValues = new ArrayList<>();
        List<Double> bValues = new ArrayList<>();

        RunningStats runningStats = new RunningStats();
        List<InternalAggregation> shardResults = new ArrayList<>();

        int valuePerShardCounter = 0;
        for (int i = 0; i < numValues; i++) {
            double valueA = randomDouble();
            aValues.add(valueA);
            double valueB = randomDouble();
            bValues.add(valueB);

            runningStats.add(new String[]{"a", "b"}, new double[]{valueA, valueB});
            if (++valuePerShardCounter == valuesPerShard) {
                shardResults.add(new InternalMatrixStats("_name", 1L, runningStats, null, Collections.emptyList(), Collections.emptyMap()));
                runningStats = new RunningStats();
                valuePerShardCounter = 0;
            }
        }

        if (valuePerShardCounter != 0) {
            shardResults.add(new InternalMatrixStats("_name", 1L, runningStats, null, Collections.emptyList(), Collections.emptyMap()));
        }
        MultiPassStats multiPassStats = new MultiPassStats("a", "b");
        multiPassStats.computeStats(aValues, bValues);

        ScriptService mockScriptService = mockScriptService();
        MockBigArrays bigArrays = new MockBigArrays(Settings.EMPTY, new NoneCircuitBreakerService());
        InternalAggregation.ReduceContext context =
            new InternalAggregation.ReduceContext(bigArrays, mockScriptService, true);
        InternalMatrixStats reduced = (InternalMatrixStats) shardResults.get(0).reduce(shardResults, context);
        multiPassStats.assertNearlyEqual(reduced.getResults());
    }

    @Override
    protected void assertReduced(InternalMatrixStats reduced, List<InternalMatrixStats> inputs) {
        throw new UnsupportedOperationException();
    }
}
