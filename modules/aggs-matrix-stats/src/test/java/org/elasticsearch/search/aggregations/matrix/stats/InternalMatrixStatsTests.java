/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.matrix.stats;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.xcontent.ContextParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.matrix.MatrixAggregationPlugin;
import org.elasticsearch.search.aggregations.matrix.stats.InternalMatrixStats.Fields;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.test.InternalAggregationTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class InternalMatrixStatsTests extends InternalAggregationTestCase<InternalMatrixStats> {

    private String[] fields;
    private boolean hasMatrixStatsResults;

    @Override
    protected SearchPlugin registerPlugin() {
        return new MatrixAggregationPlugin();
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
    protected List<NamedXContentRegistry.Entry> getNamedXContents() {
        ContextParser<Object, Aggregation> parser = (p, c) -> ParsedMatrixStats.fromXContent(p, (String) c);
        return CollectionUtils.appendToCopy(
            getDefaultNamedXContents(),
            new NamedXContentRegistry.Entry(Aggregation.class, new ParseField(MatrixStatsAggregationBuilder.NAME), parser)
        );
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
                String[] fields = Arrays.copyOf(this.fields, this.fields.length + 1);
                fields[fields.length - 1] = "field_" + (fields.length - 1);
                double[] values = new double[fields.length];
                for (int i = 0; i < fields.length; i++) {
                    values[i] = randomDouble() * 200;
                }
                runningStats = new RunningStats();
                runningStats.add(fields, values);
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
                    metadata = new HashMap<>(1);
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
        InternalAggregation.ReduceContext context = InternalAggregation.ReduceContext.forFinalReduction(
            bigArrays,
            mockScriptService,
            b -> {},
            PipelineTree.EMPTY
        );
        InternalMatrixStats reduced = (InternalMatrixStats) shardResults.get(0).reduce(shardResults, context);
        multiPassStats.assertNearlyEqual(reduced.getResults());
    }

    @Override
    protected void assertReduced(InternalMatrixStats reduced, List<InternalMatrixStats> inputs) {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void assertFromXContent(InternalMatrixStats expected, ParsedAggregation parsedAggregation) throws IOException {
        assertTrue(parsedAggregation instanceof ParsedMatrixStats);
        ParsedMatrixStats actual = (ParsedMatrixStats) parsedAggregation;

        assertEquals(expected.getDocCount(), actual.getDocCount());

        for (String field : fields) {
            assertEquals(expected.getFieldCount(field), actual.getFieldCount(field));
            assertEquals(expected.getMean(field), actual.getMean(field), 0.0);
            assertEquals(expected.getVariance(field), actual.getVariance(field), 0.0);
            assertEquals(expected.getSkewness(field), actual.getSkewness(field), 0.0);
            assertEquals(expected.getKurtosis(field), actual.getKurtosis(field), 0.0);

            for (String other : fields) {
                assertEquals(expected.getCovariance(field, other), actual.getCovariance(field, other), 0.0);
                assertEquals(expected.getCorrelation(field, other), actual.getCorrelation(field, other), 0.0);
            }
        }

        final String unknownField = randomAlphaOfLength(3);
        final String other = randomValueOtherThan(unknownField, () -> randomAlphaOfLength(3));

        for (MatrixStats matrix : Arrays.asList(actual)) {

            // getFieldCount returns 0 for unknown fields
            assertEquals(0.0, matrix.getFieldCount(unknownField), 0.0);

            expectThrows(IllegalArgumentException.class, () -> matrix.getMean(unknownField));
            expectThrows(IllegalArgumentException.class, () -> matrix.getVariance(unknownField));
            expectThrows(IllegalArgumentException.class, () -> matrix.getSkewness(unknownField));
            expectThrows(IllegalArgumentException.class, () -> matrix.getKurtosis(unknownField));

            expectThrows(IllegalArgumentException.class, () -> matrix.getCovariance(unknownField, unknownField));
            expectThrows(IllegalArgumentException.class, () -> matrix.getCovariance(unknownField, other));
            expectThrows(IllegalArgumentException.class, () -> matrix.getCovariance(other, unknownField));

            assertEquals(1.0, matrix.getCorrelation(unknownField, unknownField), 0.0);
            expectThrows(IllegalArgumentException.class, () -> matrix.getCorrelation(unknownField, other));
            expectThrows(IllegalArgumentException.class, () -> matrix.getCorrelation(other, unknownField));
        }
    }

    @Override
    protected Predicate<String> excludePathsFromXContentInsertion() {
        return path -> path.endsWith(Fields.CORRELATION) || path.endsWith(Fields.COVARIANCE);
    }
}
