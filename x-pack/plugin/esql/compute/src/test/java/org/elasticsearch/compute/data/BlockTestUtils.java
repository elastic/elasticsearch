/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomInt;
import static org.elasticsearch.test.ESTestCase.randomLong;
import static org.elasticsearch.test.ESTestCase.randomRealisticUnicodeOfCodepointLengthBetween;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class BlockTestUtils {
    /**
     * Generate a random value of the appropriate type to fit into blocks of {@code e}.
     */
    public static Object randomValue(ElementType e) {
        return switch (e) {
            case INT -> randomInt();
            case LONG -> randomLong();
            case DOUBLE -> randomDouble();
            case BYTES_REF -> new BytesRef(randomRealisticUnicodeOfCodepointLengthBetween(0, 5));   // TODO: also test spatial WKB
            case BOOLEAN -> randomBoolean();
            case DOC -> new BlockUtils.Doc(randomInt(), randomInt(), between(0, Integer.MAX_VALUE));
            case NULL -> null;
            case UNKNOWN -> throw new IllegalArgumentException("can't make random values for [" + e + "]");
        };
    }

    /**
     * Append {@code value} to {@code builder} or throw an
     * {@link IllegalArgumentException} if the types don't line up.
     */
    public static void append(Block.Builder builder, Object value) {
        if (value == null) {
            builder.appendNull();
        } else if (builder instanceof IntBlock.Builder b && value instanceof Integer v) {
            b.appendInt(v);
        } else if (builder instanceof LongBlock.Builder b && value instanceof Long v) {
            b.appendLong(v);
        } else if (builder instanceof DoubleBlock.Builder b && value instanceof Double v) {
            b.appendDouble(v);
        } else if (builder instanceof BytesRefBlock.Builder b && value instanceof BytesRef v) {
            b.appendBytesRef(v);
        } else if (builder instanceof BooleanBlock.Builder b && value instanceof Boolean v) {
            b.appendBoolean(v);
        } else if (builder instanceof DocBlock.Builder b && value instanceof BlockUtils.Doc v) {
            b.appendShard(v.shard()).appendSegment(v.segment()).appendDoc(v.doc());
        } else {
            throw new IllegalArgumentException("Can't append [" + value + "/" + value.getClass() + "] to [" + builder + "]");
        }
    }

    public static void readInto(List<List<Object>> values, Page page) {
        if (values.isEmpty()) {
            while (values.size() < page.getBlockCount()) {
                values.add(new ArrayList<>());
            }
        } else {
            if (values.size() != page.getBlockCount()) {
                throw new IllegalArgumentException("Can't load values from pages with different numbers of blocks");
            }
        }
        for (int i = 0; i < page.getBlockCount(); i++) {
            readInto(values.get(i), page.getBlock(i));
        }
        page.releaseBlocks();
    }

    public static void readInto(List<Object> values, Block block) {
        for (int p = 0; p < block.getPositionCount(); p++) {
            values.add(toJavaObject(block, p));
        }
    }

    /**
     * Assert that the values at a particular position match the provided {@link Matcher}.
     */
    @SuppressWarnings("unchecked")
    public static <T> void assertPositionValues(Block b, int p, Matcher<T> valuesMatcher) {
        List<Object> value = BasicBlockTests.valuesAtPositions(b, p, p + 1).get(0);
        assertThat((T) value, valuesMatcher);
        if (value == null) {
            assertThat(b.getValueCount(p), equalTo(0));
            assertThat(b.isNull(p), equalTo(true));
        }
    }

    public static Page deepCopyOf(Page page, BlockFactory blockFactory) {
        Block[] blockCopies = new Block[page.getBlockCount()];
        for (int i = 0; i < blockCopies.length; i++) {
            blockCopies[i] = BlockUtils.deepCopyOf(page.getBlock(i), blockFactory);
        }
        return new Page(blockCopies);
    }

    public static List<Page> deepCopyOf(List<Page> pages, BlockFactory blockFactory) {
        return pages.stream().map(page -> deepCopyOf(page, blockFactory)).toList();
    }
}
