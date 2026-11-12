/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

public class HashJoinTablesTests extends ESTestCase {
    public void testSameBlocksShareEntry() {
        Block a = longs(1, 2, 3);
        Block b = longs(4, 5, 6);
        AtomicInteger made = new AtomicInteger();
        try (HashJoinTables tables = new HashJoinTables()) {
            RefCounted first = tables.acquire(RefCounted.class, new Block[] { a, b }, e -> true, () -> make(made));
            RefCounted second = tables.acquire(RefCounted.class, new Block[] { a, b }, e -> true, () -> make(made));
            assertThat(second, sameInstance(first));
            assertThat(made.get(), equalTo(1));
            first.decRef();
            second.decRef();
            assertTrue("the tables keep the entry", first.hasReferences());
        }
    }

    public void testEqualValuesInOtherBlocksAreAnotherEntry() {
        try (HashJoinTables tables = new HashJoinTables()) {
            RefCounted first = tables.acquire(RefCounted.class, new Block[] { longs(1, 2) }, e -> true, () -> make(null));
            RefCounted second = tables.acquire(RefCounted.class, new Block[] { longs(1, 2) }, e -> true, () -> make(null));
            assertThat(second, not(sameInstance(first)));
            first.decRef();
            second.decRef();
        }
    }

    public void testBlockOrderAndTypeAreTheEntry() {
        Block a = longs(1);
        Block b = longs(2);
        try (HashJoinTables tables = new HashJoinTables()) {
            RefCounted ab = tables.acquire(RefCounted.class, new Block[] { a, b }, e -> true, () -> make(null));
            RefCounted ba = tables.acquire(RefCounted.class, new Block[] { b, a }, e -> true, () -> make(null));
            AbstractRefCounted other = tables.acquire(AbstractRefCounted.class, new Block[] { a, b }, e -> true, () -> make(null));
            assertThat(ba, not(sameInstance(ab)));
            assertThat(other, not(sameInstance(ab)));
            ab.decRef();
            ba.decRef();
            other.decRef();
        }
    }

    public void testCloseReleasesEntries() {
        HashJoinTables tables = new HashJoinTables();
        RefCounted entry = tables.acquire(RefCounted.class, new Block[] { longs(1) }, e -> true, () -> make(null));
        entry.decRef();
        assertTrue(entry.hasReferences());
        tables.close();
        assertFalse(entry.hasReferences());
    }

    public void testCallerReferenceOutlivesClose() {
        HashJoinTables tables = new HashJoinTables();
        RefCounted entry = tables.acquire(RefCounted.class, new Block[] { longs(1) }, e -> true, () -> make(null));
        tables.close();
        assertTrue("the caller's reference keeps it", entry.hasReferences());
        entry.decRef();
        assertFalse(entry.hasReferences());
    }

    public void testRejectedEntryIsReplaced() {
        Block a = longs(1);
        try (HashJoinTables tables = new HashJoinTables()) {
            RefCounted rejected = tables.acquire(RefCounted.class, new Block[] { a }, e -> true, () -> make(null));
            RefCounted replacement = tables.acquire(RefCounted.class, new Block[] { a }, e -> e != rejected, () -> make(null));
            assertThat(replacement, not(sameInstance(rejected)));
            assertTrue("the caller still holds the rejected entry", rejected.hasReferences());
            rejected.decRef();
            assertFalse("the tables dropped the rejected entry", rejected.hasReferences());
            RefCounted again = tables.acquire(RefCounted.class, new Block[] { a }, e -> true, () -> make(null));
            assertThat(again, sameInstance(replacement));
            replacement.decRef();
            again.decRef();
        }
    }

    public void testEntriesMadeAfterCloseAreNotKept() {
        Block a = longs(1);
        AtomicInteger made = new AtomicInteger();
        HashJoinTables tables = new HashJoinTables();
        tables.close();
        RefCounted first = tables.acquire(RefCounted.class, new Block[] { a }, e -> true, () -> make(made));
        RefCounted second = tables.acquire(RefCounted.class, new Block[] { a }, e -> true, () -> make(made));
        assertThat(made.get(), equalTo(2));
        first.decRef();
        second.decRef();
        assertFalse(first.hasReferences());
        assertFalse(second.hasReferences());
    }

    private static AbstractRefCounted make(AtomicInteger made) {
        if (made != null) {
            made.incrementAndGet();
        }
        return AbstractRefCounted.of(() -> {});
    }

    private static Block longs(long... values) {
        return TestBlockFactory.getNonBreakingInstance().newLongArrayVector(values, values.length).asBlock();
    }
}
