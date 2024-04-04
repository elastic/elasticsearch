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
import org.elasticsearch.indices.breaker.CircuitBreakerMetrics;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.aggregations.support.SamplingContext;

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

    @Override
    public final T createTestInstanceForXContent() {
        return createTestInstanceForXContent(randomAlphaOfLength(5), createTestMetadata(), createSubAggregations());
    }

    protected T createTestInstanceForXContent(String name, Map<String, Object> metadata, InternalAggregations subAggs) {
        return createTestInstance(name, metadata, subAggs);
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
        Exception e = expectThrows(IllegalArgumentException.class, () -> InternalAggregationTestCase.reduce(List.of(agg), reduceContext));
        assertThat(e.getMessage(), equalTo("too big!"));
    }

    /**
     * Expect that reducing this aggregation will break the real memory breaker.
     */
    protected static void expectReduceThrowsRealMemoryBreaker(InternalAggregation agg) {
        HierarchyCircuitBreakerService breaker = new HierarchyCircuitBreakerService(
            CircuitBreakerMetrics.NOOP,
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
            v -> breaker.getBreaker("request").checkRealMemoryUsage("test"),
            PipelineTree.EMPTY
        );
        Exception e = expectThrows(CircuitBreakingException.class, () -> InternalAggregationTestCase.reduce(List.of(agg), reduceContext));
        assertThat(e.getMessage(), startsWith("[parent] Data too large, data for [test] "));
    }
}
