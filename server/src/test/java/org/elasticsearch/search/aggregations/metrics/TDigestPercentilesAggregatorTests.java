/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static org.elasticsearch.search.aggregations.AggregationBuilders.percentiles;
import static org.elasticsearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

public class TDigestPercentilesAggregatorTests extends AggregatorTestCase {

    private static final double DEFAULT_COMPRESSION = 100.0;

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        var tdigestConfig = new PercentilesConfig.TDigest();
        if (randomBoolean()) {
            tdigestConfig.setCompression(randomDoubleBetween(50, 200, true));
        }
        if (randomBoolean()) {
            tdigestConfig.parseExecutionHint(randomFrom(TDigestExecutionHint.values()).toString());
        }
        return new PercentilesAggregationBuilder("tdist_percentiles").field(fieldName).percentilesConfig(tdigestConfig);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN);
    }

    public void testNoDocs() throws IOException {
        testCase(Queries.ALL_DOCS_INSTANCE, iw -> {
            // Intentionally not writing any docs
        }, tdigest -> {
            assertEquals(0L, tdigest.getState().size());
            assertFalse(AggregationInspectionHelper.hasValue(tdigest));
        });
    }

    public void testNoMatchingField() throws IOException {
        testCase(Queries.ALL_DOCS_INSTANCE, iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 7)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("wrong_number", 1)));
        }, tdigest -> {
            assertEquals(0L, tdigest.getState().size());
            assertFalse(AggregationInspectionHelper.hasValue(tdigest));
        });
    }

    public void testSomeMatchesSortedNumericDocValues() throws IOException {
        testCase(new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 8)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 5)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 1)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 1)));
            iw.addDocument(singleton(new SortedNumericDocValuesField("number", 0)));
        }, tdigest -> {
            assertEquals(7L, tdigest.getState().size());
            assertEquals(7L, tdigest.getState().centroids().size());
            assertEquals(4.0d, tdigest.percentile(75), 0.0d);
            assertEquals("4.0", tdigest.percentileAsString(75));
            assertEquals(2.0d, tdigest.percentile(50), 0.0d);
            assertEquals("2.0", tdigest.percentileAsString(50));
            assertEquals(1.0d, tdigest.percentile(22), 0.0d);
            assertEquals("1.0", tdigest.percentileAsString(22));
            assertTrue(AggregationInspectionHelper.hasValue(tdigest));
        });
    }

    public void testSomeMatchesNumericDocValues() throws IOException {
        testCase(new FieldExistsQuery("number"), iw -> {
            iw.addDocument(singleton(new NumericDocValuesField("number", 8)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 5)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 3)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 2)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 1)));
            iw.addDocument(singleton(new NumericDocValuesField("number", 0)));
        }, tdigest -> {
            assertEquals(tdigest.getState().size(), 7L);
            assertEquals(tdigest.getState().centroids().size(), 7L);
            assertEquals(8.0d, tdigest.percentile(100), 0.0d);
            assertEquals("8.0", tdigest.percentileAsString(100));
            assertEquals(4.0d, tdigest.percentile(75), 0.0d);
            assertEquals("4.0", tdigest.percentileAsString(75));
            assertEquals(1.0d, tdigest.percentile(33), 0.0d);
            assertEquals("1.0", tdigest.percentileAsString(33));
            assertEquals(1.0d, tdigest.percentile(25), 0.0d);
            assertEquals("1.0", tdigest.percentileAsString(25));
            assertEquals(0.06d, tdigest.percentile(1), 0.0d);
            assertEquals("0.06", tdigest.percentileAsString(1));
            assertTrue(AggregationInspectionHelper.hasValue(tdigest));
        });
    }

    public void testQueryFiltering() throws IOException {
        final CheckedConsumer<RandomIndexWriter, IOException> docs = iw -> {
            iw.addDocument(asList(new LongPoint("row", 7), new SortedNumericDocValuesField("number", 8)));
            iw.addDocument(asList(new LongPoint("row", 6), new SortedNumericDocValuesField("number", 5)));
            iw.addDocument(asList(new LongPoint("row", 5), new SortedNumericDocValuesField("number", 3)));
            iw.addDocument(asList(new LongPoint("row", 4), new SortedNumericDocValuesField("number", 2)));
            iw.addDocument(asList(new LongPoint("row", 3), new SortedNumericDocValuesField("number", 1)));
            iw.addDocument(asList(new LongPoint("row", 2), new SortedNumericDocValuesField("number", 1)));
            iw.addDocument(asList(new LongPoint("row", 1), new SortedNumericDocValuesField("number", 0)));
        };

        testCase(LongPoint.newRangeQuery("row", 1, 4), docs, tdigest -> {
            assertEquals(4L, tdigest.getState().size());
            assertEquals(4L, tdigest.getState().centroids().size());
            assertEquals(2.0d, tdigest.percentile(100), 0.0d);
            assertEquals(1.0d, tdigest.percentile(50), 0.0d);
            assertEquals(0.75d, tdigest.percentile(25), 0.0d);
            assertTrue(AggregationInspectionHelper.hasValue(tdigest));
        });

        testCase(LongPoint.newRangeQuery("row", 100, 110), docs, tdigest -> {
            assertEquals(0L, tdigest.getState().size());
            assertEquals(0L, tdigest.getState().centroids().size());
            assertFalse(AggregationInspectionHelper.hasValue(tdigest));
        });
    }

    public void testTdigestThenHdrSettings() throws Exception {
        int sigDigits = randomIntBetween(1, 5);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> {
            percentiles("percentiles").compression(100.0)
                .method(PercentilesMethod.TDIGEST)
                .numberOfSignificantValueDigits(sigDigits) // <-- this should trigger an exception
                .field("value");
        });
        assertThat(
            e.getMessage(),
            equalTo("Cannot set [numberOfSignificantValueDigits] because the " + "method has already been configured for TDigest")
        );
    }

    public void testBreakerBytesReleasedAfterSuccessfulAggregation() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        PercentilesAggregationBuilder aggBuilder = new PercentilesAggregationBuilder("p").field("number")
            .percentilesConfig(new PercentilesConfig.TDigest());

        HierarchyCircuitBreakerService breakerService = requestBreakerService("10mb");
        withSequentialIndex(100, reader -> {
            try (
                AggregationContext context = createAggregationContext(
                    reader,
                    createIndexSettings(),
                    Queries.ALL_DOCS_INSTANCE,
                    breakerService,
                    AggregationBuilder.DEFAULT_PREALLOCATION,
                    DEFAULT_MAX_BUCKETS,
                    false,
                    false,
                    fieldType
                )
            ) {
                Aggregator aggregator = createAggregator(aggBuilder, context);
                aggregator.preCollection();
                context.searcher().search(Queries.ALL_DOCS_INSTANCE, aggregator.asCollector());
                aggregator.postCollection();
                aggregator.buildTopLevel();
            }
            assertThat(breakerService.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
        });
    }

    public void testBreakerTripsOnHighCardinalityTermsPercentiles() throws IOException {
        // Reproduces the OOM scenario from the issue: a terms agg with many distinct values
        // creates one TDigest sketch per bucket. With our fix each sketch charges the REQUEST
        // breaker, so a tight limit trips the breaker instead of exhausting heap.
        HierarchyCircuitBreakerService breakerService = requestBreakerService("50kb");
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);

        PercentilesAggregationBuilder percBuilder = new PercentilesAggregationBuilder("p").field("number")
            .percentilesConfig(new PercentilesConfig.TDigest());
        TermsAggregationBuilder termsBuilder = new TermsAggregationBuilder("terms").field("number").size(10000).subAggregation(percBuilder);

        withSequentialIndex(500, reader -> {
            expectThrows(CircuitBreakingException.class, () -> {
                try (
                    AggregationContext context = createAggregationContext(
                        reader,
                        createIndexSettings(),
                        Queries.ALL_DOCS_INSTANCE,
                        breakerService,
                        0,
                        DEFAULT_MAX_BUCKETS,
                        false,
                        false,
                        fieldType
                    )
                ) {
                    Aggregator aggregator = createAggregator(termsBuilder, context);
                    aggregator.preCollection();
                    context.searcher().search(Queries.ALL_DOCS_INSTANCE, aggregator.asCollector());
                    aggregator.postCollection();
                    aggregator.buildTopLevel();
                }
            });
            assertThat(breakerService.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
        });
    }

    public void testBreakerTripReleasesAllBytes() throws IOException {
        HierarchyCircuitBreakerService breakerService = requestBreakerService("1kb");
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        PercentilesAggregationBuilder aggBuilder = new PercentilesAggregationBuilder("p").field("number")
            .percentilesConfig(new PercentilesConfig.TDigest());

        withSequentialIndex(500, reader -> {
            expectThrows(CircuitBreakingException.class, () -> collectWithBreaker(reader, breakerService, aggBuilder, fieldType));
            assertThat(breakerService.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
        });
    }

    public void testReduceAfterAggregationContextClosed() throws IOException {
        HierarchyCircuitBreakerService breakerService = requestBreakerService("100mb");
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        PercentilesAggregationBuilder aggBuilder = percentilesBuilder(DEFAULT_COMPRESSION);

        // A partial reduce over shard results that collected nothing emits HistogramUnionState.EMPTY, whose compression
        // is hard-coded to 1.0 and therefore always below the request's. The coordinator consumes partials ahead of raw
        // shard results, so such a partial used to make the reducer adopt the first data-bearing result as its
        // accumulator. Both shard results read the same 1500 document index, which keeps each below the HybridDigest
        // sorting-to-merging threshold of 20 * 100 = 2000 that their combined 3000 values cross, so the allocating
        // transition happens during reduction, against the already-closed preallocated breaker of the aggregation
        // context that built the adopted result.
        withSequentialIndex(1500, reader -> {
            InternalAggregations emptyPartial = InternalAggregations.topLevelReduce(
                List.of(InternalAggregations.from(List.of(emptyShardResult())), InternalAggregations.from(List.of(emptyShardResult()))),
                partialReduceContext(aggBuilder)
            );
            assertThat(((InternalTDigestPercentiles) emptyPartial.copyResults().get(0)).state.compression(), equalTo(1.0));

            InternalAggregation first = buildPreallocatedShardResult(reader, breakerService, fieldType, DEFAULT_COMPRESSION);
            InternalAggregation second = buildPreallocatedShardResult(reader, breakerService, fieldType, DEFAULT_COMPRESSION);

            InternalAggregations reduced = InternalAggregations.topLevelReduce(
                List.of(emptyPartial, InternalAggregations.from(List.of(first)), InternalAggregations.from(List.of(second))),
                finalReduceContext(aggBuilder)
            );

            InternalTDigestPercentiles result = (InternalTDigestPercentiles) reduced.copyResults().get(0);
            assertThat(result.state.size(), equalTo(3000L));
            assertThat(result.percentile(50), closeTo(749.5, 30.0));
        });
        assertThat(breakerService.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    private static InternalAggregation emptyShardResult() {
        return InternalTDigestPercentiles.empty("p", new double[] { 50 }, false, DocValueFormat.RAW, null);
    }

    private InternalAggregation buildPreallocatedShardResult(
        DirectoryReader reader,
        CircuitBreakerService breakerService,
        MappedFieldType fieldType,
        double compression
    ) throws IOException {
        PercentilesAggregationBuilder aggBuilder = percentilesBuilder(compression);
        try (
            AggregationContext context = createAggregationContext(
                reader,
                createIndexSettings(),
                Queries.ALL_DOCS_INSTANCE,
                breakerService,
                AggregationBuilder.DEFAULT_PREALLOCATION,
                DEFAULT_MAX_BUCKETS,
                false,
                false,
                fieldType
            )
        ) {
            Aggregator aggregator = createAggregator(aggBuilder, context);
            aggregator.preCollection();
            context.searcher().search(Queries.ALL_DOCS_INSTANCE, aggregator.asCollector());
            aggregator.postCollection();
            return aggregator.buildTopLevel();
        }
    }

    private static PercentilesAggregationBuilder percentilesBuilder(double compression) {
        return new PercentilesAggregationBuilder("p").field("number").percentilesConfig(new PercentilesConfig.TDigest(compression));
    }

    private static AggregationReduceContext finalReduceContext(AggregationBuilder builder) {
        return new AggregationReduceContext.ForFinal(
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            () -> false,
            new AggregatorFactories.Builder().addAggregator(builder),
            bucketCount -> {},
            null
        );
    }

    private static AggregationReduceContext partialReduceContext(AggregationBuilder builder) {
        return new AggregationReduceContext.ForPartial(
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            () -> false,
            new AggregatorFactories.Builder().addAggregator(builder),
            bucketCount -> {},
            null
        );
    }

    private static void withSequentialIndex(int docCount, CheckedConsumer<DirectoryReader, IOException> body) throws IOException {
        try (Directory directory = newDirectory()) {
            try (RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
                for (int i = 0; i < docCount; i++) {
                    iw.addDocument(singleton(new SortedNumericDocValuesField("number", i)));
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                body.accept(reader);
            }
        }
    }

    private void collectWithBreaker(
        DirectoryReader reader,
        CircuitBreakerService breakerService,
        PercentilesAggregationBuilder aggBuilder,
        MappedFieldType fieldType
    ) throws IOException {
        try (
            AggregationContext context = createAggregationContext(
                reader,
                createIndexSettings(),
                Queries.ALL_DOCS_INSTANCE,
                breakerService,
                0,
                DEFAULT_MAX_BUCKETS,
                false,
                false,
                fieldType
            )
        ) {
            Aggregator aggregator = createAggregator(aggBuilder, context);
            aggregator.preCollection();
            context.searcher().search(Queries.ALL_DOCS_INSTANCE, aggregator.asCollector());
            aggregator.postCollection();
            aggregator.buildTopLevel();
        }
    }

    private static HierarchyCircuitBreakerService requestBreakerService(String requestLimit) {
        Settings settings = Settings.builder()
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), requestLimit)
            .put(HierarchyCircuitBreakerService.USE_REAL_MEMORY_USAGE_SETTING.getKey(), false)
            .build();
        return new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
            settings,
            List.of(),
            new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        );
    }

    private void testCase(
        Query query,
        CheckedConsumer<RandomIndexWriter, IOException> buildIndex,
        Consumer<InternalTDigestPercentiles> verify
    ) throws IOException {
        PercentilesAggregationBuilder builder;
        // TODO this randomization path should be removed when the old settings are removed
        if (randomBoolean()) {
            builder = new PercentilesAggregationBuilder("test").field("number").method(PercentilesMethod.TDIGEST);
        } else {
            PercentilesConfig hdr = new PercentilesConfig.TDigest();
            builder = new PercentilesAggregationBuilder("test").field("number").percentilesConfig(hdr);
        }

        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        testCase(buildIndex, verify, new AggTestConfig(builder, fieldType).withQuery(query));
    }
}
