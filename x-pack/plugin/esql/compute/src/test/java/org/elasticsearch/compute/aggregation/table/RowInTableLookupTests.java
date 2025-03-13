/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.table;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.test.MockBlockFactory;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RowInTableLookupTests extends ESTestCase {
    final CircuitBreaker breaker = new MockBigArrays.LimitedBreaker("esql-test-breaker", ByteSizeValue.ofGb(1));
    final BigArrays bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, mockBreakerService(breaker));
    final MockBlockFactory blockFactory = new MockBlockFactory(breaker, bigArrays);

    public void testDuplicateInts() {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(2)) {
            builder.appendInt(1);
            builder.appendInt(1);
            try (Block b = builder.build()) {
                Exception e = expectThrows(IllegalArgumentException.class, () -> RowInTableLookup.build(blockFactory, new Block[] { b }));
                assertThat(e.getMessage(), equalTo("found a duplicate row"));
            }
        }
    }

    public void testMultivaluedInts() {
        try (IntBlock.Builder builder = blockFactory.newIntBlockBuilder(2)) {
            builder.beginPositionEntry();
            builder.appendInt(1);
            builder.appendInt(2);
            builder.endPositionEntry();
            try (Block b = builder.build()) {
                Exception e = expectThrows(IllegalArgumentException.class, () -> RowInTableLookup.build(blockFactory, new Block[] { b }));
                assertThat(e.getMessage(), equalTo("only single valued keys are supported"));
            }
        }
    }

    public void testDuplicateBytes() {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
            builder.appendBytesRef(new BytesRef("foo"));
            builder.appendBytesRef(new BytesRef("foo"));
            try (Block b = builder.build()) {
                Exception e = expectThrows(IllegalArgumentException.class, () -> RowInTableLookup.build(blockFactory, new Block[] { b }));
                assertThat(e.getMessage(), equalTo("found a duplicate row"));
            }
        }
    }

    public void testMultivaluedBytes() {
        try (BytesRefBlock.Builder builder = blockFactory.newBytesRefBlockBuilder(2)) {
            builder.beginPositionEntry();
            builder.appendBytesRef(new BytesRef("foo"));
            builder.appendBytesRef(new BytesRef("bar"));
            builder.endPositionEntry();
            try (Block b = builder.build()) {
                Exception e = expectThrows(IllegalArgumentException.class, () -> RowInTableLookup.build(blockFactory, new Block[] { b }));
                assertThat(e.getMessage(), equalTo("only single valued keys are supported"));
            }
        }
    }

    // A breaker service that always returns the given breaker for getBreaker(CircuitBreaker.REQUEST)
    static CircuitBreakerService mockBreakerService(CircuitBreaker breaker) {
        CircuitBreakerService breakerService = mock(CircuitBreakerService.class);
        when(breakerService.getBreaker(CircuitBreaker.REQUEST)).thenReturn(breaker);
        return breakerService;
    }

    @After
    public void checkBreaker() {
        blockFactory.ensureAllBlocksAreReleased();
        assertThat(breaker.getUsed(), is(0L));
    }
}
