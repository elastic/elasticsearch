/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.aggregations.metric;

import org.elasticsearch.aggregations.AggregationsPlugin;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class InternalMatrixStatsTests extends InternalAggregationTestCase<InternalMatrixStats> {

    private String[] fields;
    private boolean hasMatrixStatsResults;

    @Override
    protected SearchPlugin registerPlugin() {
        return new AggregationsPlugin();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        hasMatrixStatsResults = frequently();
        int numFields = hasMatrixStatsResults ? randomInt(128) : 0;
        fields = new String[numFields];
        for (int i = 0; i < numFields; i++) {
            fields[i] = "field_" + i;
        }
    }

    @Override
    protected InternalMatrixStats createTestInstance(String name, Map<String, Object> metadata) {
        double[] values = new double[fields.length];
        for (int i = 0; i < fields.length; i++) {
            values[i] = randomDouble();
        }

        RunningStats runningStats = new RunningStats();
        runningStats.add(fields, values);
        MatrixStatsResults matrixStatsResults = hasMatrixStatsResults ? new MatrixStatsResults(runningStats) : null;
        return new InternalMatrixStats(name, 1L, runningStats, matrixStatsResults, metadata);
    }

    @Override
    protected InternalMatrixStats mutateInstance(InternalMatrixStats instance) {
        String name = instance.getName();
        long docCount = instance.getDocCount();
        RunningStats runningStats = instance.getStats();
        MatrixStatsResults matrixStatsResults = instance.getResults();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 3)) {
            case 0:
                name += randomAlphaOfLength(5);
                break;
            case 1:
                String[] fieldsCopy = Arrays.copyOf(this.fields, this.fields.length + 1);
                fieldsCopy[fieldsCopy.length - 1] = "field_" + (fieldsCopy.length - 1);
                double[] values = new double[fieldsCopy.length];
                for (int i = 0; i < fieldsCopy.length; i++) {
                    values[i] = randomDouble() * 200;
                }
                runningStats = new RunningStats();
                runningStats.add(fieldsCopy, values);
                break;
            case 2:
                if (matrixStatsResults == null) {
                    matrixStatsResults = new MatrixStatsResults(runningStats);
                } else {
                    matrixStatsResults = null;
                }
                break;
            case 3:
            default:
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
                break;
        }
        return new InternalMatrixStats(name, docCount, runningStats, matrixStatsResults, metadata);
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

            runningStats.add(new String[] { "a", "b" }, new double[] { valueA, valueB });
            if (++valuePerShardCounter == valuesPerShard) {
                shardResults.add(new InternalMatrixStats("_name", 1L, runningStats, null, Collections.emptyMap()));
                runningStats = new RunningStats();
                valuePerShardCounter = 0;
            }
        }

        if (valuePerShardCounter != 0) {
            shardResults.add(new InternalMatrixStats("_name", 1L, runningStats, null, Collections.emptyMap()));
        }
        MultiPassStats multiPassStats = new MultiPassStats("a", "b");
        multiPassStats.computeStats(aValues, bValues);

        ScriptService mockScriptService = mockScriptService();
        MockBigArrays bigArrays = new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService());
        AggregationReduceContext context = new AggregationReduceContext.ForFinal(
            bigArrays,
            mockScriptService,
            () -> false,
            mock(AggregationBuilder.class),
            b -> {},
            PipelineTree.EMPTY
        );
        InternalMatrixStats reduced = (InternalMatrixStats) InternalAggregationTestCase.reduce(shardResults, context);
        multiPassStats.assertNearlyEqual(reduced.getResults());
    }

    @Override
    protected void assertReduced(InternalMatrixStats reduced, List<InternalMatrixStats> inputs) {
        throw new UnsupportedOperationException();
    }
}
