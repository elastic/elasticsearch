/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.metrics;

import com.carrotsearch.hppc.BitMixer;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class InternalCardinalityTests extends InternalAggregationTestCase<InternalCardinality> {
    private static List<HyperLogLogPlusPlus> algos;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        algos = new ArrayList<>();
    }

    @After // we force @After to have it run before ESTestCase#after otherwise it fails
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Releasables.close(algos);
        algos.clear();
        algos = null;
    }

    @Override
    protected InternalCardinality createTestInstance(String name, Map<String, Object> metadata) {
        return createTestInstance(name, metadata, randomIntBetween(AbstractHyperLogLog.MIN_PRECISION, AbstractHyperLogLog.MAX_PRECISION));
    }

    private InternalCardinality createTestInstance(String name, Map<String, Object> metadata, int precision) {
        HyperLogLogPlusPlus hllpp = new HyperLogLogPlusPlus(
            precision,
            new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService()),
            1
        );
        algos.add(hllpp);
        int values = between(20, 1000);
        for (int i = 0; i < values; i++) {
            hllpp.collect(0, BitMixer.mix64(randomInt()));
        }
        return new InternalCardinality(name, hllpp, metadata);
    }

    @Override
    protected BuilderAndToReduce<InternalCardinality> randomResultsToReduce(String name, int size) {
        int precision = randomIntBetween(AbstractHyperLogLog.MIN_PRECISION, AbstractHyperLogLog.MAX_PRECISION);
        List<InternalCardinality> result = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            result.add(createTestInstance(name, createTestMetadata(), precision));
        }
        return new BuilderAndToReduce<>(mock(AggregationBuilder.class), result);
    }

    public void testReduceUsesRequestCircuitBreaker() {
        int precision = AbstractHyperLogLog.MAX_PRECISION;
        InternalCardinality input = createTestInstance("cardinality", null, precision);
        CircuitBreakerService breakerService = limitedBreakerService(ByteSizeValue.ofBytes(1));
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService).withCircuitBreaking();
        AggregationReduceContext reduceContext = new AggregationReduceContext.ForFinal(
            bigArrays,
            null,
            () -> false,
            AggregatorFactories.builder(),
            b -> {}
        );

        CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> {
            try (AggregatorReducer reducer = input.getReducer(reduceContext, 1)) {
                reducer.accept(input);
            }
        });
        assertThat(e.getMessage(), equalTo(MockBigArrays.ERROR_MESSAGE));
        assertThat(breakerService.getBreaker(CircuitBreaker.REQUEST).getUsed(), equalTo(0L));
    }

    public void testReduceAccountsForResultClonePeakMemory() {
        int precision = AbstractHyperLogLog.MAX_PRECISION;
        InternalCardinality input = createTestInstance("cardinality", null, precision);
        long reducedHllBytes;
        try (
            HyperLogLogPlusPlus reduced = new HyperLogLogPlusPlus(
                precision,
                new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService()).withCircuitBreaking(),
                1
            )
        ) {
            reduced.merge(0, input.getCounts(), 0);
            reducedHllBytes = reduced.ramBytesUsed();
        }
        CircuitBreakerService breakerService = limitedBreakerService(ByteSizeValue.ofBytes(reducedHllBytes * 3 / 2));
        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService).withCircuitBreaking();
        AggregationReduceContext reduceContext = new AggregationReduceContext.ForFinal(
            bigArrays,
            null,
            () -> false,
            AggregatorFactories.builder(),
            b -> {}
        );

        CircuitBreakingException e = expectThrows(CircuitBreakingException.class, () -> {
            try (AggregatorReducer reducer = input.getReducer(reduceContext, 1)) {
                reducer.accept(input);
                assertThat(breaker.getUsed(), greaterThan(0L));
                reducer.get();
            }
        });
        assertThat(e.getMessage(), equalTo(MockBigArrays.ERROR_MESSAGE));
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testReduceSucceedsWithBreakerLimitAboveResultClonePeakMemory() {
        int precision = AbstractHyperLogLog.MAX_PRECISION;
        InternalCardinality input = createTestInstance("cardinality", null, precision);
        long reducedHllBytes;
        try (
            HyperLogLogPlusPlus reduced = new HyperLogLogPlusPlus(
                precision,
                new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, new NoneCircuitBreakerService()).withCircuitBreaking(),
                1
            )
        ) {
            reduced.merge(0, input.getCounts(), 0);
            reducedHllBytes = reduced.ramBytesUsed();
        }
        CircuitBreakerService breakerService = limitedBreakerService(ByteSizeValue.ofBytes(reducedHllBytes * 3));
        CircuitBreaker breaker = breakerService.getBreaker(CircuitBreaker.REQUEST);
        BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, breakerService).withCircuitBreaking();
        AggregationReduceContext reduceContext = new AggregationReduceContext.ForFinal(
            bigArrays,
            null,
            () -> false,
            AggregatorFactories.builder(),
            b -> {}
        );

        try (AggregatorReducer reducer = input.getReducer(reduceContext, 1)) {
            reducer.accept(input);
            assertThat(breaker.getUsed(), greaterThan(0L));
            InternalCardinality result = (InternalCardinality) reducer.get();
            assertThat(result.value(), equalTo(input.value()));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    private static CircuitBreakerService limitedBreakerService(ByteSizeValue limit) {
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(new MockBigArrays.LimitedBreaker(CircuitBreaker.REQUEST, limit));
        return breakerService;
    }

    @Override
    protected void assertReduced(InternalCardinality reduced, List<InternalCardinality> inputs) {
        HyperLogLogPlusPlus[] algos = inputs.stream().map(InternalCardinality::getState).toArray(size -> new HyperLogLogPlusPlus[size]);
        if (algos.length > 0) {
            HyperLogLogPlusPlus result = algos[0];
            for (int i = 1; i < algos.length; i++) {
                result.merge(0, algos[i], 0);
            }
            assertEquals(result.cardinality(0), reduced.value(), 0);
        }
    }

    @Override
    protected InternalCardinality mutateInstance(InternalCardinality instance) {
        String name = instance.getName();
        AbstractHyperLogLogPlusPlus state = instance.getState();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 2)) {
            case 0 -> name += randomAlphaOfLength(5);
            case 1 -> {
                HyperLogLogPlusPlus newState = new HyperLogLogPlusPlus(
                    state.precision(),
                    new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService()),
                    0
                );
                int values = between(0, 10);
                for (int i = 0; i < values; i++) {
                    newState.collect(0, BitMixer.mix64(randomIntBetween(500, 10000)));
                }
                algos.add(newState);
                state = newState;
            }
            case 2 -> {
                if (metadata == null) {
                    metadata = Maps.newMapWithExpectedSize(1);
                } else {
                    metadata = new HashMap<>(instance.getMetadata());
                }
                metadata.put(randomAlphaOfLength(15), randomInt());
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalCardinality(name, state, metadata);
    }
}
