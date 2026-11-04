/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.lucene.query;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.operator.topn.SharedMinCompetitive;
import org.elasticsearch.compute.operator.topn.TopNEncoder;
import org.elasticsearch.index.mapper.DateFieldMapper.Resolution;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class SharedSortBoundTests extends ESTestCase {

    public void testNothingOffered() {
        SharedSortBound bound = new SharedSortBound();
        assertFalse(bound.canSkip(false, Resolution.MILLISECONDS, 0, 10));
        assertFalse(bound.canSkip(true, Resolution.MILLISECONDS, 0, 10));
    }

    public void testDescendingKeepsTheHighestOffer() {
        SharedSortBound bound = new SharedSortBound();
        bound.offer(false, Resolution.MILLISECONDS, 100);
        bound.offer(false, Resolution.MILLISECONDS, 50);
        assertTrue(bound.canSkip(false, Resolution.MILLISECONDS, 0, 99));
        assertFalse(bound.canSkip(false, Resolution.MILLISECONDS, 0, 100));
        assertFalse(bound.canSkip(false, Resolution.MILLISECONDS, 0, 150));
    }

    public void testAscendingKeepsTheLowestOffer() {
        SharedSortBound bound = new SharedSortBound();
        bound.offer(true, Resolution.MILLISECONDS, 50);
        bound.offer(true, Resolution.MILLISECONDS, 100);
        assertTrue(bound.canSkip(true, Resolution.MILLISECONDS, 51, 200));
        assertFalse(bound.canSkip(true, Resolution.MILLISECONDS, 50, 200));
        assertFalse(bound.canSkip(true, Resolution.MILLISECONDS, 0, 200));
    }

    public void testShardsWithoutValues() {
        SharedSortBound bound = new SharedSortBound();
        bound.offer(false, Resolution.MILLISECONDS, 100);
        assertTrue(bound.canSkip(false, Resolution.MILLISECONDS, Long.MAX_VALUE, Long.MIN_VALUE));
        assertFalse(bound.canSkip(false, Resolution.MILLISECONDS, Long.MIN_VALUE, Long.MAX_VALUE));
    }

    public void testOtherDirectionOrResolution() {
        SharedSortBound bound = new SharedSortBound();
        bound.offer(false, Resolution.MILLISECONDS, 100);
        assertFalse(bound.canSkip(true, Resolution.MILLISECONDS, 200, 300));
        assertFalse(bound.canSkip(false, Resolution.NANOSECONDS, 0, 10));
    }

    public void testValuesThatDontCompareDisableTheBound() {
        SharedSortBound bound = new SharedSortBound();
        bound.offer(false, Resolution.MILLISECONDS, 100);
        bound.offer(false, Resolution.NANOSECONDS, 100);
        assertFalse(bound.canSkip(false, Resolution.MILLISECONDS, 0, 10));
        bound.offer(false, Resolution.MILLISECONDS, 200);
        assertFalse(bound.canSkip(false, Resolution.MILLISECONDS, 0, 10));
    }

    public void testMinCompetitiveOutlivesItsOperators() {
        CircuitBreaker breaker = new NoopCircuitBreaker("test");
        List<SharedMinCompetitive.KeyConfig> keys = List.of(
            new SharedMinCompetitive.KeyConfig(ElementType.LONG, TopNEncoder.DEFAULT_SORTABLE, false, false)
        );
        try (SharedSortBound bound = new SharedSortBound()) {
            SharedMinCompetitive.Supplier supplier = bound.minCompetitive(breaker, keys);
            assertSame(supplier, bound.minCompetitive(breaker, keys));
            // The operators of a first batch publish a bound and close
            try (SharedMinCompetitive channel = supplier.get()) {
                assertTrue(channel.offer(new BytesRef(new byte[] { 1, 2, 3 })));
            }
            // The operators of the next batch get the same bound
            try (SharedMinCompetitive channel = supplier.get()) {
                assertThat(channel.generation(), equalTo(1L));
            }
            List<SharedMinCompetitive.KeyConfig> otherKeys = List.of(
                new SharedMinCompetitive.KeyConfig(ElementType.LONG, TopNEncoder.DEFAULT_SORTABLE, true, false)
            );
            assertNull(bound.minCompetitive(breaker, otherKeys));
        }
    }
}
