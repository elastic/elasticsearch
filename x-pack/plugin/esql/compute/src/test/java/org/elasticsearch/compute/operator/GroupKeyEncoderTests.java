/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.BytesRefHashTable;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.aggregation.blockhash.HashImplFactory;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.List;

public class GroupKeyEncoderTests extends ComputeTestCase {

    private GroupKeyEncoder encoder(int[] groupChannels, List<ElementType> elementTypes) {
        BlockFactory bf = blockFactory();
        return new GroupKeyEncoder(groupChannels, elementTypes, bf.breaker(), bf.bigArrays().recycler());
    }

    /** Returns the group ordinal for {@code pos}, adding to {@code table} if not already present. */
    private long group(GroupKeyEncoder encoder, BytesRefHashTable table, Page page, int pos) {
        return BlockHash.hashOrdToGroup(encoder.encodeAndAdd(page, pos, table));
    }

    public void testSameIntValuesSameKey() {
        BlockFactory bf = blockFactory();
        Page page = new Page(bf.newIntArrayVector(new int[] { 7, 7 }, 2).asBlock());
        try (
            GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.INT));
            BytesRefHashTable table = HashImplFactory.newBytesRefHash(bf)
        ) {
            assertEquals(group(encoder, table, page, 0), group(encoder, table, page, 1));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testDifferentIntValuesDifferentKeys() {
        BlockFactory bf = blockFactory();
        Page page = new Page(bf.newIntArrayVector(new int[] { 1, 2 }, 2).asBlock());
        try (
            GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.INT));
            BytesRefHashTable table = HashImplFactory.newBytesRefHash(bf)
        ) {
            assertNotEquals(group(encoder, table, page, 0), group(encoder, table, page, 1));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testLongType() {
        BlockFactory bf = blockFactory();
        Page page = new Page(bf.newLongArrayVector(new long[] { 100, 200, 100 }, 3).asBlock());
        try (
            GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.LONG));
            BytesRefHashTable table = HashImplFactory.newBytesRefHash(bf)
        ) {
            long g0 = group(encoder, table, page, 0);
            long g1 = group(encoder, table, page, 1);
            long g2 = group(encoder, table, page, 2);
            assertEquals(g0, g2);
            assertNotEquals(g0, g1);
        } finally {
            page.releaseBlocks();
        }
    }

    public void testDoubleType() {
        BlockFactory bf = blockFactory();
        Page page = new Page(bf.newDoubleArrayVector(new double[] { 1.5, 2.5, 1.5 }, 3).asBlock());
        try (
            GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.DOUBLE));
            BytesRefHashTable table = HashImplFactory.newBytesRefHash(bf)
        ) {
            long g0 = group(encoder, table, page, 0);
            long g1 = group(encoder, table, page, 1);
            long g2 = group(encoder, table, page, 2);
            assertEquals(g0, g2);
            assertNotEquals(g0, g1);
        } finally {
            page.releaseBlocks();
        }
    }

    public void testFloatType() {
        BlockFactory bf = blockFactory();
        Page page = new Page(bf.newFloatArrayVector(new float[] { 1.5f, 2.5f, 1.5f }, 3).asBlock());
        try (
            GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.FLOAT));
            BytesRefHashTable table = HashImplFactory.newBytesRefHash(bf)
        ) {
            long g0 = group(encoder, table, page, 0);
            long g1 = group(encoder, table, page, 1);
            long g2 = group(encoder, table, page, 2);
            assertEquals(g0, g2);
            assertNotEquals(g0, g1);
        } finally {
            page.releaseBlocks();
        }
    }

    public void testBooleanType() {
        BlockFactory bf = blockFactory();
        try (var builder = bf.newBooleanBlockBuilder(3)) {
            builder.appendBoolean(true);
            builder.appendBoolean(false);
            builder.appendBoolean(true);
            Page page = new Page(builder.build());
            try (
                GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.BOOLEAN));
                BytesRefHashTable table = HashImplFactory.newBytesRefHash(bf)
            ) {
                long gTrue = group(encoder, table, page, 0);
                long gFalse = group(encoder, table, page, 1);
                long gTrue2 = group(encoder, table, page, 2);
                assertEquals(gTrue, gTrue2);
                assertNotEquals(gTrue, gFalse);
            } finally {
                page.releaseBlocks();
            }
        }
    }

    public void testBytesRefType() {
        BlockFactory bf = blockFactory();
        try (var builder = bf.newBytesRefBlockBuilder(3)) {
            builder.appendBytesRef(new BytesRef("hello"));
            builder.appendBytesRef(new BytesRef("world"));
            builder.appendBytesRef(new BytesRef("hello"));
            Page page = new Page(builder.build());
            try (
                GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.BYTES_REF));
                BytesRefHashTable table = HashImplFactory.newBytesRefHash(bf)
            ) {
                long g0 = group(encoder, table, page, 0);
                long g1 = group(encoder, table, page, 1);
                long g2 = group(encoder, table, page, 2);
                assertEquals(g0, g2);
                assertNotEquals(g0, g1);
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /**
     * A null position must produce a key distinct from any non-null value,
     * including zero / empty.
     */
    public void testNullProducesDistinctKey() {
        BlockFactory bf = blockFactory();
        try (var builder = bf.newLongBlockBuilder(2)) {
            builder.appendNull();
            builder.appendLong(0);
            Page page = new Page(builder.build());
            try (
                GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.LONG));
                BytesRefHashTable table = HashImplFactory.newBytesRefHash(bf)
            ) {
                assertNotEquals(group(encoder, table, page, 0), group(encoder, table, page, 1));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /**
     * {@code [1, 2]} and {@code [2, 1]} are different groups because
     * multivalues use list (order-sensitive) semantics.
     */
    public void testMultivalueOrderMatters() {
        BlockFactory bf = blockFactory();
        try (var builder = bf.newLongBlockBuilder(2)) {
            builder.beginPositionEntry();
            builder.appendLong(1);
            builder.appendLong(2);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendLong(2);
            builder.appendLong(1);
            builder.endPositionEntry();
            Page page = new Page(builder.build());
            try (
                GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.LONG));
                BytesRefHashTable table = HashImplFactory.newBytesRefHash(bf)
            ) {
                assertNotEquals(group(encoder, table, page, 0), group(encoder, table, page, 1));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /**
     * Two positions with the same multivalue list in the same order
     * produce identical keys.
     */
    public void testMultivalueSameOrderSameKey() {
        BlockFactory bf = blockFactory();
        try (var builder = bf.newLongBlockBuilder(2)) {
            builder.beginPositionEntry();
            builder.appendLong(1);
            builder.appendLong(2);
            builder.endPositionEntry();
            builder.beginPositionEntry();
            builder.appendLong(1);
            builder.appendLong(2);
            builder.endPositionEntry();
            Page page = new Page(builder.build());
            try (
                GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.LONG));
                BytesRefHashTable table = HashImplFactory.newBytesRefHash(bf)
            ) {
                assertEquals(group(encoder, table, page, 0), group(encoder, table, page, 1));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /**
     * Composite keys: (1, "a"), (1, "b"), and (2, "a") are all distinct groups.
     */
    public void testCompositeKey() {
        BlockFactory bf = blockFactory();
        try (var longBuilder = bf.newLongBlockBuilder(3); var bytesBuilder = bf.newBytesRefBlockBuilder(3)) {
            longBuilder.appendLong(1);
            longBuilder.appendLong(1);
            longBuilder.appendLong(2);
            bytesBuilder.appendBytesRef(new BytesRef("a"));
            bytesBuilder.appendBytesRef(new BytesRef("b"));
            bytesBuilder.appendBytesRef(new BytesRef("a"));
            Page page = new Page(longBuilder.build(), bytesBuilder.build());
            try (
                GroupKeyEncoder encoder = encoder(new int[] { 0, 1 }, List.of(ElementType.LONG, ElementType.BYTES_REF));
                BytesRefHashTable table = HashImplFactory.newBytesRefHash(bf)
            ) {
                long g0 = group(encoder, table, page, 0);
                long g1 = group(encoder, table, page, 1);
                long g2 = group(encoder, table, page, 2);
                assertNotEquals(g0, g1);
                assertNotEquals(g0, g2);
                assertNotEquals(g1, g2);
            } finally {
                page.releaseBlocks();
            }
        }
    }

    /**
     * A single value and a multivalue with one element that matches
     * should produce the same key (both have value count 1).
     */
    public void testSingleValueMatchesOneElementMultivalue() {
        BlockFactory bf = blockFactory();
        try (var builder = bf.newLongBlockBuilder(2)) {
            builder.appendLong(42);
            builder.beginPositionEntry();
            builder.appendLong(42);
            builder.endPositionEntry();
            Page page = new Page(builder.build());
            try (
                GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.LONG));
                BytesRefHashTable table = HashImplFactory.newBytesRefHash(bf)
            ) {
                assertEquals(group(encoder, table, page, 0), group(encoder, table, page, 1));
            } finally {
                page.releaseBlocks();
            }
        }
    }
}
