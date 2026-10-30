/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class DocOriginTableTests extends ESTestCase {
    public void testNumbersEachOriginOnce() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        try (DocOriginTable table = new DocOriginTable(breaker)) {
            DocOrigin a = RandomBlock.randomDocOrigin();
            DocOrigin b = randomValueOtherThan(a, RandomBlock::randomDocOrigin);
            int ordinalA = table.ordinal(a);
            int ordinalB = table.ordinal(b);
            assertThat(table.ordinal(a), equalTo(ordinalA));
            assertThat(table.get(ordinalA), equalTo(a));
            assertThat(table.get(ordinalB), equalTo(b));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testReturnsItsBytesWhenClosed() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
        DocOriginTable table = new DocOriginTable(breaker);
        for (int i = 0; i < 10; i++) {
            table.ordinal(RandomBlock.randomDocOrigin());
        }
        assertThat(table.ramBytesUsed(), greaterThan(0L));
        assertThat(breaker.getUsed(), equalTo(table.ramBytesUsed()));

        table.close();
        assertThat(breaker.getUsed(), equalTo(0L));
        table.close();
        assertThat(breaker.getUsed(), equalTo(0L));
    }

    public void testBreaks() {
        CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofBytes(100));
        try (DocOriginTable table = new DocOriginTable(breaker)) {
            expectThrows(CircuitBreakingException.class, () -> table.ordinal(RandomBlock.randomDocOrigin()));
            assertThat(table.ramBytesUsed(), equalTo(0L));
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }
}
