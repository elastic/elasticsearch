/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.test;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.BytesRefVector;
import org.elasticsearch.compute.data.DocBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.FloatBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.OrdinalBytesRefBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Releasables;
import org.hamcrest.Matcher;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.compute.data.BlockUtils.toJavaObject;
import static org.elasticsearch.test.ESTestCase.between;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomDouble;
import static org.elasticsearch.test.ESTestCase.randomFloat;
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
            case FLOAT -> randomFloat();
            case DOUBLE -> randomDouble();
            case BYTES_REF -> new BytesRef(randomRealisticUnicodeOfCodepointLengthBetween(0, 5));   // TODO: also test spatial WKB
            case BOOLEAN -> randomBoolean();
            case DOC -> new BlockUtils.Doc(randomInt(), randomInt(), between(0, Integer.MAX_VALUE));
            case NULL -> null;
            case COMPOSITE -> throw new IllegalArgumentException("can't make random values for composite");
            case AGGREGATE_METRIC_DOUBLE -> throw new IllegalArgumentException("can't make random values for aggregate_metric_double");
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
            return;
        }
        if (builder instanceof IntBlock.Builder b) {
            if (value instanceof Integer v) {
                b.appendInt(v);
                return;
            }
            if (value instanceof List<?> l) {
                switch (l.size()) {
                    case 0 -> b.appendNull();
                    case 1 -> b.appendInt((Integer) l.get(0));
                    default -> {
                        b.beginPositionEntry();
                        for (Object o : l) {
                            b.appendInt((Integer) o);
                        }
                        b.endPositionEntry();
                    }
                }
                return;
            }
        }
        if (builder instanceof LongBlock.Builder b) {
            if (value instanceof Long v) {
                b.appendLong(v);
                return;
            }
            if (value instanceof List<?> l) {
                switch (l.size()) {
                    case 0 -> b.appendNull();
                    case 1 -> b.appendLong((Long) l.get(0));
                    default -> {
                        b.beginPositionEntry();
                        for (Object o : l) {
                            b.appendLong((Long) o);
                        }
                        b.endPositionEntry();
                    }
                }
                return;
            }
        }
        if (builder instanceof FloatBlock.Builder b) {
            if (value instanceof Float v) {
                b.appendFloat(v);
                return;
            }
            if (value instanceof List<?> l) {
                switch (l.size()) {
                    case 0 -> b.appendNull();
                    case 1 -> b.appendFloat((Float) l.get(0));
                    default -> {
                        b.beginPositionEntry();
                        for (Object o : l) {
                            b.appendFloat((Float) o);
                        }
                        b.endPositionEntry();
                    }
                }
                return;
            }
        }
        if (builder instanceof DoubleBlock.Builder b) {
            if (value instanceof Double v) {
                b.appendDouble(v);
                return;
            }
            if (value instanceof List<?> l) {
                switch (l.size()) {
                    case 0 -> b.appendNull();
                    case 1 -> b.appendDouble((Double) l.get(0));
                    default -> {
                        b.beginPositionEntry();
                        for (Object o : l) {
                            b.appendDouble((Double) o);
                        }
                        b.endPositionEntry();
                    }
                }
                return;
            }
        }
        if (builder instanceof BytesRefBlock.Builder b) {
            if (value instanceof BytesRef v) {
                b.appendBytesRef(v);
                return;
            }
            if (value instanceof List<?> l) {
                switch (l.size()) {
                    case 0 -> b.appendNull();
                    case 1 -> b.appendBytesRef((BytesRef) l.get(0));
                    default -> {
                        b.beginPositionEntry();
                        for (Object o : l) {
                            b.appendBytesRef((BytesRef) o);
                        }
                        b.endPositionEntry();
                    }
                }
                return;
            }
        }
        if (builder instanceof BooleanBlock.Builder b) {
            if (value instanceof Boolean v) {
                b.appendBoolean(v);
                return;
            }
            if (value instanceof List<?> l) {
                switch (l.size()) {
                    case 0 -> b.appendNull();
                    case 1 -> b.appendBoolean((Boolean) l.get(0));
                    default -> {
                        b.beginPositionEntry();
                        for (Object o : l) {
                            b.appendBoolean((Boolean) o);
                        }
                        b.endPositionEntry();
                    }
                }
                return;
            }
        }
        if (builder instanceof DocBlock.Builder b && value instanceof BlockUtils.Doc v) {
            b.appendShard(v.shard()).appendSegment(v.segment()).appendDoc(v.doc());
            return;
        }
        if (value instanceof List<?> l && l.isEmpty()) {
            builder.appendNull();
            return;
        }
        throw new IllegalArgumentException("Can't append [" + value + "/" + value.getClass() + "] to [" + builder + "]");
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
        List<Object> value = valuesAtPositions(b, p, p + 1).get(0);
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

    public static List<List<Object>> valuesAtPositions(Block block, int from, int to) {
        List<List<Object>> result = new ArrayList<>(to - from);
        for (int p = from; p < to; p++) {
            if (block.isNull(p)) {
                result.add(null);
                continue;
            }
            int count = block.getValueCount(p);
            List<Object> positionValues = new ArrayList<>(count);
            int i = block.getFirstValueIndex(p);
            for (int v = 0; v < count; v++) {
                positionValues.add(switch (block.elementType()) {
                    case INT -> ((IntBlock) block).getInt(i++);
                    case LONG -> ((LongBlock) block).getLong(i++);
                    case FLOAT -> ((FloatBlock) block).getFloat(i++);
                    case DOUBLE -> ((DoubleBlock) block).getDouble(i++);
                    case BYTES_REF -> ((BytesRefBlock) block).getBytesRef(i++, new BytesRef());
                    case BOOLEAN -> ((BooleanBlock) block).getBoolean(i++);
                    default -> throw new IllegalArgumentException("unsupported element type [" + block.elementType() + "]");
                });
            }
            result.add(positionValues);
        }
        return result;
    }

    /**
     * Convert all of the {@link Block}s in a page that contain {@link BytesRef}s into
     * {@link OrdinalBytesRefBlock}s.
     */
    public static Page convertBytesRefsToOrdinals(Page page) {
        Block[] blocks = new Block[page.getBlockCount()];
        try {
            for (int b = 0; b < page.getBlockCount(); b++) {
                Block block = page.getBlock(b);
                if (block.elementType() != ElementType.BYTES_REF) {
                    blocks[b] = block;
                    continue;
                }
                Map<BytesRef, Integer> dedupe = new HashMap<>();
                BytesRefBlock bytesRefBlock = (BytesRefBlock) block;
                try (
                    IntBlock.Builder ordinals = block.blockFactory().newIntBlockBuilder(block.getPositionCount());
                    BytesRefVector.Builder bytes = block.blockFactory().newBytesRefVectorBuilder(block.getPositionCount())
                ) {
                    BytesRef scratch = new BytesRef();
                    for (int p = 0; p < block.getPositionCount(); p++) {
                        int first = block.getFirstValueIndex(p);
                        int count = block.getValueCount(p);
                        if (count == 0) {
                            ordinals.appendNull();
                            continue;
                        }
                        if (count == 1) {
                            BytesRef v = bytesRefBlock.getBytesRef(first, scratch);
                            ordinals.appendInt(dedupe(dedupe, bytes, v));
                            continue;
                        }
                        int end = first + count;
                        ordinals.beginPositionEntry();
                        for (int i = first; i < end; i++) {
                            BytesRef v = bytesRefBlock.getBytesRef(i, scratch);
                            ordinals.appendInt(dedupe(dedupe, bytes, v));
                        }
                        ordinals.endPositionEntry();
                    }
                    blocks[b] = new OrdinalBytesRefBlock(ordinals.build(), bytes.build());
                    bytesRefBlock.decRef();
                }
            }
            Page p = new Page(blocks);
            Arrays.fill(blocks, null);
            return p;
        } finally {
            Releasables.close(blocks);
        }
    }

    private static int dedupe(Map<BytesRef, Integer> dedupe, BytesRefVector.Builder bytes, BytesRef v) {
        Integer current = dedupe.get(v);
        if (current != null) {
            return current;
        }
        bytes.appendBytesRef(v);
        int o = dedupe.size();
        dedupe.put(v, o);
        return o;
    }
}
