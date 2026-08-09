/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static java.util.Collections.singleton;
import static org.elasticsearch.test.InternalAggregationTestCase.DEFAULT_MAX_BUCKETS;
import static org.hamcrest.Matchers.equalTo;

public class TDigestPercentileRanksAggregatorTests extends AggregatorTestCase {

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        var tdigestConfig = new PercentilesConfig.TDigest();
        if (randomBoolean()) {
            tdigestConfig.setCompression(randomDoubleBetween(50, 200, true));
        }
        if (randomBoolean()) {
            tdigestConfig.parseExecutionHint(randomFrom(TDigestExecutionHint.values()).toString());
        }
        return new PercentileRanksAggregationBuilder("tdigest_ranks", new double[] { 0.1, 0.5, 12 }).field(fieldName)
            .percentilesConfig(tdigestConfig);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN);
    }

    public void testEmpty() throws IOException {
        PercentileRanksAggregationBuilder aggBuilder = new PercentileRanksAggregationBuilder("my_agg", new double[] { 0.5 }).field("field")
            .method(PercentilesMethod.TDIGEST);
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.DOUBLE);
        try (IndexReader reader = new MultiReader()) {
            PercentileRanks ranks = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
            Percentile rank = ranks.iterator().next();
            assertEquals(Double.NaN, rank.percent(), 0d);
            assertEquals(0.5, rank.value(), 0d);
            assertFalse(AggregationInspectionHelper.hasValue(((InternalTDigestPercentileRanks) ranks)));
        }
    }

    public void testSimple() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (double value : new double[] { 3, 0.2, 10 }) {
                Document doc = new Document();
                doc.add(new SortedNumericDocValuesField("field", NumericUtils.doubleToSortableLong(value)));
                w.addDocument(doc);
            }

            PercentileRanksAggregationBuilder aggBuilder = new PercentileRanksAggregationBuilder("my_agg", new double[] { 0.1, 0.5, 12 })
                .field("field")
                .method(PercentilesMethod.TDIGEST);
            MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("field", NumberFieldMapper.NumberType.DOUBLE);
            try (IndexReader reader = w.getReader()) {
                PercentileRanks ranks = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                Iterator<Percentile> rankIterator = ranks.iterator();
                Percentile rank = rankIterator.next();
                assertEquals(0.1, rank.value(), 0d);
                // TODO: Fix T-Digest: this assertion should pass but we currently get ~15
                // https://github.com/elastic/elasticsearch/issues/14851
                // assertThat(rank.getPercent(), Matchers.equalTo(0d));
                rank = rankIterator.next();
                assertEquals(0.5, rank.value(), 0d);
                assertThat(rank.percent(), Matchers.greaterThan(0d));
                assertThat(rank.percent(), Matchers.lessThan(100d));
                rank = rankIterator.next();
                assertEquals(12, rank.value(), 0d);
                // TODO: Fix T-Digest: this assertion should pass but we currently get ~59
                // https://github.com/elastic/elasticsearch/issues/14851
                // assertThat(rank.getPercent(), Matchers.equalTo(100d));
                assertFalse(rankIterator.hasNext());
                assertTrue(AggregationInspectionHelper.hasValue(((InternalTDigestPercentileRanks) ranks)));
            }
        }
    }

    public void testNullValues() throws IOException {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new PercentileRanksAggregationBuilder("my_agg", null).field("field").method(PercentilesMethod.TDIGEST)
        );
        assertThat(e.getMessage(), Matchers.equalTo("[values] must not be null: [my_agg]"));
    }

    public void testEmptyValues() throws IOException {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new PercentileRanksAggregationBuilder("my_agg", new double[0]).field("field").method(PercentilesMethod.TDIGEST)
        );

        assertThat(e.getMessage(), Matchers.equalTo("[values] must not be an empty array: [my_agg]"));
    }

    public void testBreakerBytesReleasedAfterSuccessfulAggregation() throws IOException {
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        PercentileRanksAggregationBuilder aggBuilder = new PercentileRanksAggregationBuilder("ranks", new double[] { 50.0, 250.0 })
            .field("number")
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

    public void testBreakerTripReleasesAllBytes() throws IOException {
        HierarchyCircuitBreakerService breakerService = requestBreakerService("1kb");
        MappedFieldType fieldType = new NumberFieldMapper.NumberFieldType("number", NumberFieldMapper.NumberType.LONG);
        PercentileRanksAggregationBuilder aggBuilder = new PercentileRanksAggregationBuilder("ranks", new double[] { 50.0, 250.0 })
            .field("number")
            .percentilesConfig(new PercentilesConfig.TDigest());

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
                    Aggregator aggregator = createAggregator(aggBuilder, context);
                    aggregator.preCollection();
                    context.searcher().search(Queries.ALL_DOCS_INSTANCE, aggregator.asCollector());
                    aggregator.postCollection();
                    aggregator.buildTopLevel();
                }
            });
            assertThat(breakerService.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
        });
    }

    private void withSequentialIndex(int docCount, CheckedConsumer<DirectoryReader, IOException> body) throws IOException {
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
}
