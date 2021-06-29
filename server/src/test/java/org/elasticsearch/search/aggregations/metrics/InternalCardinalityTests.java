/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.metrics;

import com.carrotsearch.hppc.BitMixer;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.MockPageCacheRecycler;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.junit.After;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InternalCardinalityTests extends InternalAggregationTestCase<InternalCardinality> {
    private static List<HyperLogLogPlusPlus> algos;
    private static int p;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        algos = new ArrayList<>();
        p = randomIntBetween(AbstractHyperLogLog.MIN_PRECISION, AbstractHyperLogLog.MAX_PRECISION);
    }

    @After //we force @After to have it run before ESTestCase#after otherwise it fails
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        Releasables.close(algos);
        algos.clear();
        algos = null;
    }

    @Override
    protected InternalCardinality createTestInstance(String name, Map<String, Object> metadata) {
        HyperLogLogPlusPlus hllpp = new HyperLogLogPlusPlus(p,
                new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService()), 1);
        algos.add(hllpp);
        for (int i = 0; i < 100; i++) {
            hllpp.collect(0, BitMixer.mix64(randomIntBetween(1, 100)));
        }
        return new InternalCardinality(name, hllpp, metadata);
    }

    @Override
    protected void assertReduced(InternalCardinality reduced, List<InternalCardinality> inputs) {
        HyperLogLogPlusPlus[] algos = inputs.stream().map(InternalCardinality::getState)
                .toArray(size -> new HyperLogLogPlusPlus[size]);
        if (algos.length > 0) {
            HyperLogLogPlusPlus result = algos[0];
            for (int i = 1; i < algos.length; i++) {
                result.merge(0, algos[i], 0);
            }
            assertEquals(result.cardinality(0), reduced.value(), 0);
        }
    }

    @Override
    protected void assertFromXContent(InternalCardinality aggregation, ParsedAggregation parsedAggregation) {
        assertTrue(parsedAggregation instanceof ParsedCardinality);
        ParsedCardinality parsed = (ParsedCardinality) parsedAggregation;

        assertEquals(aggregation.getValue(), parsed.getValue(), Double.MIN_VALUE);
        assertEquals(aggregation.getValueAsString(), parsed.getValueAsString());
    }

    @Override
    protected InternalCardinality mutateInstance(InternalCardinality instance) {
        String name = instance.getName();
        AbstractHyperLogLogPlusPlus state = instance.getState();
        Map<String, Object> metadata = instance.getMetadata();
        switch (between(0, 2)) {
        case 0:
            name += randomAlphaOfLength(5);
            break;
        case 1:
            HyperLogLogPlusPlus newState = new HyperLogLogPlusPlus(state.precision(),
                    new MockBigArrays(new MockPageCacheRecycler(Settings.EMPTY), new NoneCircuitBreakerService()), 0);
            for (int i = 0; i < 10; i++) {
                newState.collect(0, BitMixer.mix64(randomIntBetween(500, 10000)));
            }
            algos.add(newState);
            state = newState;
            break;
        case 2:
            if (metadata == null) {
                metadata = new HashMap<>(1);
            } else {
                metadata = new HashMap<>(instance.getMetadata());
            }
            metadata.put(randomAlphaOfLength(15), randomInt());
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }
        return new InternalCardinality(name, state, metadata);
    }
}
