/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test;

import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.search.aggregations.ParsedMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.aggregations.support.SamplingContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.IntConsumer;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

public abstract class InternalMultiBucketAggregationTestCase<T extends InternalAggregation & MultiBucketsAggregation> extends
    InternalAggregationTestCase<T> {

    private static final int DEFAULT_MAX_NUMBER_OF_BUCKETS = 10;

    private Supplier<InternalAggregations> subAggregationsSupplier;
    private int maxNumberOfBuckets = DEFAULT_MAX_NUMBER_OF_BUCKETS;

    protected int randomNumberOfBuckets() {
        return randomIntBetween(minNumberOfBuckets(), maxNumberOfBuckets());
    }

    protected int minNumberOfBuckets() {
        return 0;
    }

    protected int maxNumberOfBuckets() {
        return maxNumberOfBuckets;
    }

    public void setMaxNumberOfBuckets(int maxNumberOfBuckets) {
        this.maxNumberOfBuckets = maxNumberOfBuckets;
    }

    public void setSubAggregationsSupplier(Supplier<InternalAggregations> subAggregationsSupplier) {
        this.subAggregationsSupplier = subAggregationsSupplier;
    }

    public final InternalAggregations createSubAggregations() {
        return subAggregationsSupplier.get();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (randomBoolean()) {
            subAggregationsSupplier = () -> InternalAggregations.EMPTY;
        } else {
            subAggregationsSupplier = () -> {
                int numSubAggs = randomIntBetween(1, 3);
                List<InternalAggregation> aggs = new ArrayList<>();
                for (int i = 0; i < numSubAggs; i++) {
                    aggs.add(createTestInstanceForXContent(randomAlphaOfLength(5), emptyMap(), InternalAggregations.EMPTY));
                }
                return InternalAggregations.from(aggs);
            };
        }
    }

    @Override
    protected final T createTestInstance(String name, Map<String, Object> metadata) {
        T instance = createTestInstance(name, metadata, subAggregationsSupplier.get());
        assert instance.getBuckets().size() <= maxNumberOfBuckets()
            : "Maximum number of buckets exceeded for " + instance.getClass().getSimpleName() + " aggregation";
        return instance;
    }

    protected abstract T createTestInstance(String name, Map<String, Object> metadata, InternalAggregations aggregations);

    /**
     * The parsed version used by the deprecated high level rest client or
     * {@code null} if the deprecated high level rest client isn't supported
     * by this agg.
     */
    protected abstract Class<? extends ParsedMultiBucketAggregation<?>> implementationClass();

    @Override
    protected final void assertFromXContent(T aggregation, ParsedAggregation parsedAggregation) {
        assertMultiBucketsAggregations(aggregation, parsedAggregation, false);
    }

    @Override
    public final T createTestInstanceForXContent() {
        return createTestInstanceForXContent(randomAlphaOfLength(5), createTestMetadata(), createSubAggregations());
    }

    protected T createTestInstanceForXContent(String name, Map<String, Object> metadata, InternalAggregations subAggs) {
        return createTestInstance(name, metadata, subAggs);
    }

    public void testIterators() throws IOException {
        final T aggregation = createTestInstanceForXContent();
        assertMultiBucketsAggregations(aggregation, parseAndAssert(aggregation, false, false), true);
    }

    @Override
    protected <P extends ParsedAggregation> P parseAndAssert(InternalAggregation aggregation, boolean shuffled, boolean addRandomFields)
        throws IOException {
        assumeFalse("deprecated high level rest client not supported", implementationClass() == null);
        return super.parseAndAssert(aggregation, shuffled, addRandomFields);
    }

    @Override
    protected void assertSampled(T sampled, T reduced, SamplingContext samplingContext) {
        assertBucketCountsScaled(sampled.getBuckets(), reduced.getBuckets(), samplingContext);
    }

    protected void assertBucketCountsScaled(
        List<? extends MultiBucketsAggregation.Bucket> sampled,
        List<? extends MultiBucketsAggregation.Bucket> reduced,
        SamplingContext samplingContext
    ) {
        assertEquals(sampled.size(), reduced.size());
        Iterator<? extends MultiBucketsAggregation.Bucket> sampledIt = sampled.iterator();
        for (MultiBucketsAggregation.Bucket reducedBucket : reduced) {
            MultiBucketsAggregation.Bucket sampledBucket = sampledIt.next();
            assertEquals(sampledBucket.getDocCount(), samplingContext.scaleUp(reducedBucket.getDocCount()));
        }
    }

    private void assertMultiBucketsAggregations(Aggregation expected, Aggregation actual, boolean checkOrder) {
        assertTrue(expected instanceof MultiBucketsAggregation);
        MultiBucketsAggregation expectedMultiBucketsAggregation = (MultiBucketsAggregation) expected;

        assertTrue(actual instanceof MultiBucketsAggregation);
        MultiBucketsAggregation actualMultiBucketsAggregation = (MultiBucketsAggregation) actual;

        assertMultiBucketsAggregation(expectedMultiBucketsAggregation, actualMultiBucketsAggregation, checkOrder);

        List<? extends MultiBucketsAggregation.Bucket> expectedBuckets = expectedMultiBucketsAggregation.getBuckets();
        List<? extends MultiBucketsAggregation.Bucket> actualBuckets = actualMultiBucketsAggregation.getBuckets();
        assertEquals(expectedBuckets.size(), actualBuckets.size());

        if (checkOrder) {
            Iterator<? extends MultiBucketsAggregation.Bucket> expectedIt = expectedBuckets.iterator();
            Iterator<? extends MultiBucketsAggregation.Bucket> actualIt = actualBuckets.iterator();
            while (expectedIt.hasNext()) {
                MultiBucketsAggregation.Bucket expectedBucket = expectedIt.next();
                MultiBucketsAggregation.Bucket actualBucket = actualIt.next();
                assertBucket(expectedBucket, actualBucket, true);
            }
        } else {
            for (MultiBucketsAggregation.Bucket expectedBucket : expectedBuckets) {
                final Object expectedKey = expectedBucket.getKey();
                boolean found = false;

                for (MultiBucketsAggregation.Bucket actualBucket : actualBuckets) {
                    final Object actualKey = actualBucket.getKey();
                    if ((actualKey != null && actualKey.equals(expectedKey)) || (actualKey == null && expectedKey == null)) {
                        found = true;
                        assertBucket(expectedBucket, actualBucket, false);
                        break;
                    }
                }
                assertTrue("Failed to find bucket with key [" + expectedBucket.getKey() + "]", found);
            }
        }
    }

    protected void assertMultiBucketsAggregation(MultiBucketsAggregation expected, MultiBucketsAggregation actual, boolean checkOrder) {
        Class<? extends ParsedMultiBucketAggregation<?>> parsedClass = implementationClass();
        assertNotNull("Parsed aggregation class must not be null", parsedClass);
        assertTrue(
            "Unexpected parsed class, expected instance of: " + actual + ", but was: " + parsedClass,
            parsedClass.isInstance(actual)
        );

        assertTrue(expected instanceof InternalAggregation);
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.getMetadata(), actual.getMetadata());
        assertEquals(expected.getType(), actual.getType());
    }

    protected void assertBucket(MultiBucketsAggregation.Bucket expected, MultiBucketsAggregation.Bucket actual, boolean checkOrder) {
        assertTrue(expected instanceof InternalMultiBucketAggregation.InternalBucket);
        assertTrue(actual instanceof ParsedMultiBucketAggregation.ParsedBucket);

        assertEquals(expected.getKey(), actual.getKey());
        assertEquals(expected.getKeyAsString(), actual.getKeyAsString());
        assertEquals(expected.getDocCount(), actual.getDocCount());

        Aggregations expectedAggregations = expected.getAggregations();
        Aggregations actualAggregations = actual.getAggregations();
        assertEquals(expectedAggregations.asList().size(), actualAggregations.asList().size());

        if (checkOrder) {
            Iterator<Aggregation> expectedIt = expectedAggregations.iterator();
            Iterator<Aggregation> actualIt = actualAggregations.iterator();

            while (expectedIt.hasNext()) {
                Aggregation expectedAggregation = expectedIt.next();
                Aggregation actualAggregation = actualIt.next();
                assertMultiBucketsAggregations(expectedAggregation, actualAggregation, true);
            }
        } else {
            for (Aggregation expectedAggregation : expectedAggregations) {
                Aggregation actualAggregation = actualAggregations.get(expectedAggregation.getName());
                assertNotNull(actualAggregation);
                assertMultiBucketsAggregations(expectedAggregation, actualAggregation, false);
            }
        }
    }

    @Override
    public void doAssertReducedMultiBucketConsumer(Aggregation agg, MultiBucketConsumerService.MultiBucketConsumer bucketConsumer) {
        /*
         * No-op.
         */
    }

    /**
     * Expect that reducing this aggregation will pass the bucket limit.
     */
    protected static void expectReduceUsesTooManyBuckets(InternalAggregation agg, int bucketLimit) {
        AggregationReduceContext reduceContext = new AggregationReduceContext.ForFinal(
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            () -> false,
            mock(AggregationBuilder.class),
            new IntConsumer() {
                int buckets;

                @Override
                public void accept(int value) {
                    buckets += value;
                    if (buckets > bucketLimit) {
                        throw new IllegalArgumentException("too big!");
                    }
                }
            },
            PipelineTree.EMPTY
        );
        Exception e = expectThrows(IllegalArgumentException.class, () -> agg.reduce(List.of(agg), reduceContext));
        assertThat(e.getMessage(), equalTo("too big!"));
    }

    /**
     * Expect that reducing this aggregation will break the real memory breaker.
     */
    protected static void expectReduceThrowsRealMemoryBreaker(InternalAggregation agg) {
        HierarchyCircuitBreakerService breaker = new HierarchyCircuitBreakerService(
            Settings.builder().put(HierarchyCircuitBreakerService.TOTAL_CIRCUIT_BREAKER_LIMIT_SETTING.getKey(), "50%").build(),
            List.of(),
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
        ) {
            @Override
            public void checkParentLimit(long newBytesReserved, String label) throws CircuitBreakingException {
                super.checkParentLimit(newBytesReserved, label);
            }
        };
        AggregationReduceContext reduceContext = new AggregationReduceContext.ForFinal(
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            () -> false,
            mock(AggregationBuilder.class),
            v -> breaker.getBreaker("request").addEstimateBytesAndMaybeBreak(0, "test"),
            PipelineTree.EMPTY
        );
        Exception e = expectThrows(CircuitBreakingException.class, () -> agg.reduce(List.of(agg), reduceContext));
        assertThat(e.getMessage(), startsWith("[parent] Data too large, data for [test] "));
    }
}
