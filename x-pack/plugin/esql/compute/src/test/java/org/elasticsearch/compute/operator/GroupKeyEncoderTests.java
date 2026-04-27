/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.test.ComputeTestCase;

import java.util.List;

public class GroupKeyEncoderTests extends ComputeTestCase {

    private GroupKeyEncoder encoder(int[] groupChannels, List<ElementType> elementTypes) {
        BlockFactory bf = blockFactory();
        PagedBytesBuilder row = new PagedBytesBuilder(bf.bigArrays().recycler(), bf.breaker(), "group-key-encoder-test", 64);
        return new GroupKeyEncoder(groupChannels, elementTypes, row);
    }

    public void testSameIntValuesSameKey() {
        BlockFactory bf = blockFactory();
        Page page = new Page(bf.newIntArrayVector(new int[] { 7, 7 }, 2).asBlock());
        try (GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.INT))) {
            assertEquals(copy(encoder.encode(page, 0)), copy(encoder.encode(page, 1)));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testDifferentIntValuesDifferentKeys() {
        BlockFactory bf = blockFactory();
        Page page = new Page(bf.newIntArrayVector(new int[] { 1, 2 }, 2).asBlock());
        try (GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.INT))) {
            assertNotEquals(copy(encoder.encode(page, 0)), copy(encoder.encode(page, 1)));
        } finally {
            page.releaseBlocks();
        }
    }

    public void testLongType() {
        BlockFactory bf = blockFactory();
        Page page = new Page(bf.newLongArrayVector(new long[] { 100, 200, 100 }, 3).asBlock());
        try (GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.LONG))) {
            BytesRef key0 = copy(encoder.encode(page, 0));
            BytesRef key1 = copy(encoder.encode(page, 1));
            BytesRef key2 = copy(encoder.encode(page, 2));
            assertEquals(key0, key2);
            assertNotEquals(key0, key1);
        } finally {
            page.releaseBlocks();
        }
    }

    public void testDoubleType() {
        BlockFactory bf = blockFactory();
        Page page = new Page(bf.newDoubleArrayVector(new double[] { 1.5, 2.5, 1.5 }, 3).asBlock());
        try (GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.DOUBLE))) {
            BytesRef key0 = copy(encoder.encode(page, 0));
            BytesRef key1 = copy(encoder.encode(page, 1));
            BytesRef key2 = copy(encoder.encode(page, 2));
            assertEquals(key0, key2);
            assertNotEquals(key0, key1);
        } finally {
            page.releaseBlocks();
        }
    }

    public void testFloatType() {
        BlockFactory bf = blockFactory();
        Page page = new Page(bf.newFloatArrayVector(new float[] { 1.5f, 2.5f, 1.5f }, 3).asBlock());
        try (GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.FLOAT))) {
            BytesRef key0 = copy(encoder.encode(page, 0));
            BytesRef key1 = copy(encoder.encode(page, 1));
            BytesRef key2 = copy(encoder.encode(page, 2));
            assertEquals(key0, key2);
            assertNotEquals(key0, key1);
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
            try (GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.BOOLEAN))) {
                BytesRef keyTrue = copy(encoder.encode(page, 0));
                BytesRef keyFalse = copy(encoder.encode(page, 1));
                BytesRef keyTrue2 = copy(encoder.encode(page, 2));
                assertEquals(keyTrue, keyTrue2);
                assertNotEquals(keyTrue, keyFalse);
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
            try (GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.BYTES_REF))) {
                BytesRef key0 = copy(encoder.encode(page, 0));
                BytesRef key1 = copy(encoder.encode(page, 1));
                BytesRef key2 = copy(encoder.encode(page, 2));
                assertEquals(key0, key2);
                assertNotEquals(key0, key1);
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
            try (GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.LONG))) {
                assertNotEquals(copy(encoder.encode(page, 0)), copy(encoder.encode(page, 1)));
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
            try (GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.LONG))) {
                assertNotEquals(copy(encoder.encode(page, 0)), copy(encoder.encode(page, 1)));
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
            try (GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.LONG))) {
                assertEquals(copy(encoder.encode(page, 0)), copy(encoder.encode(page, 1)));
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
            try (GroupKeyEncoder encoder = encoder(new int[] { 0, 1 }, List.of(ElementType.LONG, ElementType.BYTES_REF))) {
                BytesRef key0 = copy(encoder.encode(page, 0));
                BytesRef key1 = copy(encoder.encode(page, 1));
                BytesRef key2 = copy(encoder.encode(page, 2));
                assertNotEquals(key0, key1);
                assertNotEquals(key0, key2);
                assertNotEquals(key1, key2);
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
            try (GroupKeyEncoder encoder = encoder(new int[] { 0 }, List.of(ElementType.LONG))) {
                assertEquals(copy(encoder.encode(page, 0)), copy(encoder.encode(page, 1)));
            } finally {
                page.releaseBlocks();
            }
        }
    }

    private static BytesRef copy(PagedBytesCursor cursor) {
        byte[] bytes = new byte[cursor.remaining()];
        int pos = 0;
        while (cursor.remaining() > 0) {
            bytes[pos++] = cursor.readByte();
        }
        return new BytesRef(bytes);
    }
}
