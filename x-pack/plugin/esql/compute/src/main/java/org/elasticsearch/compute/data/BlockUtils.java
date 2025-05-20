/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.compute.data.AggregateMetricDoubleBlockBuilder.AggregateMetricDoubleLiteral;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.function.Consumer;

import static org.elasticsearch.common.lucene.BytesRefs.toBytesRef;
import static org.elasticsearch.compute.data.ElementType.fromJava;

public final class BlockUtils {

    public static final Block[] NO_BLOCKS = new Block[0];

    private BlockUtils() {}

    public record BuilderWrapper(Block.Builder builder, Consumer<Object> append) implements Releasable {
        public BuilderWrapper(Block.Builder builder, Consumer<Object> append) {
            this.builder = builder;
            this.append = o -> {
                if (o == null) {
                    builder.appendNull();
                    return;
                }
                if (o instanceof List<?> l) {
                    builder.beginPositionEntry();
                    for (Object v : l) {
                        append.accept(v);
                    }
                    builder.endPositionEntry();
                    return;
                }
                append.accept(o);
            };
        }

        public void accept(Object object) {
            append.accept(object);
        }

        @Override
        public void close() {
            builder.close();
        }
    }

    public static Block[] fromArrayRow(BlockFactory blockFactory, Object... row) {
        return fromListRow(blockFactory, Arrays.asList(row));
    }

    public static Block[] fromListRow(BlockFactory blockFactory, List<Object> row) {
        return fromListRow(blockFactory, row, 1);
    }

    public static Block[] fromListRow(BlockFactory blockFactory, List<Object> row, int blockSize) {
        if (row.isEmpty()) {
            return NO_BLOCKS;
        }

        var size = row.size();
        Block[] blocks = new Block[size];
        boolean success = false;
        try {
            for (int i = 0; i < size; i++) {
                Object object = row.get(i);
                if (object instanceof List<?> listVal) {
                    try (BuilderWrapper wrapper = wrapperFor(blockFactory, fromJava(listVal.get(0).getClass()), blockSize)) {
                        wrapper.accept(listVal);
                        Random random = Randomness.get();
                        if (isDeduplicated(listVal) && random.nextBoolean()) {
                            if (isAscending(listVal) && random.nextBoolean()) {
                                wrapper.builder.mvOrdering(Block.MvOrdering.DEDUPLICATED_AND_SORTED_ASCENDING);
                            } else {
                                wrapper.builder.mvOrdering(Block.MvOrdering.DEDUPLICATED_UNORDERD);
                            }
                        } else if (isAscending(listVal) && random.nextBoolean()) {
                            wrapper.builder.mvOrdering(Block.MvOrdering.SORTED_ASCENDING);
                        }
                        blocks[i] = wrapper.builder.build();
                    }
                } else {
                    blocks[i] = constantBlock(blockFactory, object, blockSize);
                }
            }
            success = true;
            return blocks;
        } finally {
            if (success == false) {
                Releasables.closeExpectNoException(blocks);
            }
        }
    }

    /**
     * Detect blocks with ascending fields. This is *mostly* useful for
     * exercising the specialized ascending implementations.
     */
    private static boolean isAscending(List<?> values) {
        Comparable<Object> prev = null;
        for (Object o : values) {
            @SuppressWarnings("unchecked")
            Comparable<Object> val = (Comparable<Object>) o;
            if (prev == null) {
                prev = val;
                continue;
            }
            if (prev.compareTo(val) > 0) {
                return false;
            }
            prev = val;
        }
        return true;
    }

    /**
     * Detect blocks with deduplicated fields. This is *mostly* useful for
     * exercising the specialized ascending implementations.
     */
    private static boolean isDeduplicated(List<?> values) {
        return new HashSet<>(values).size() == values.size();
    }

    public static Block[] fromList(BlockFactory blockFactory, List<List<Object>> list) {
        var size = list.size();
        if (size == 0) {
            return NO_BLOCKS;
        }
        if (size == 1) {
            return fromListRow(blockFactory, list.get(0));
        }

        var wrappers = new BuilderWrapper[list.get(0).size()];
        try {
            for (int i = 0; i < wrappers.length; i++) {
                wrappers[i] = wrapperFor(blockFactory, fromJava(type(list, i)), size);
            }
            for (List<Object> values : list) {
                for (int j = 0, vSize = values.size(); j < vSize; j++) {
                    wrappers[j].append.accept(values.get(j));
                }
            }
            final Block[] blocks = new Block[wrappers.length];
            try {
                for (int i = 0; i < blocks.length; i++) {
                    blocks[i] = wrappers[i].builder.build();
                }
                return blocks;
            } finally {
                if (blocks[blocks.length - 1] == null) {
                    Releasables.closeExpectNoException(blocks);
                }
            }
        } finally {
            Releasables.closeExpectNoException(wrappers);
        }
    }

    /** Returns a deep copy of the given block, using the blockFactory for creating the copy block. */
    public static Block deepCopyOf(Block block, BlockFactory blockFactory) {
        try (Block.Builder builder = block.elementType().newBlockBuilder(block.getPositionCount(), blockFactory)) {
            builder.copyFrom(block, 0, block.getPositionCount());
            builder.mvOrdering(block.mvOrdering());
            return builder.build();
        }
    }

    private static Class<?> type(List<List<Object>> list, int i) {
        int p = 0;
        while (p < list.size()) {
            Object v = list.get(p++).get(i);
            if (v == null) {
                continue;
            }
            if (v instanceof List<?> l) {
                if (l.isEmpty()) {
                    continue;
                }
                return l.get(0).getClass();
            }
            return v.getClass();
        }
        return null;
    }

    public static BuilderWrapper wrapperFor(BlockFactory blockFactory, ElementType type, int size) {
        var b = type.newBlockBuilder(size, blockFactory);
        return new BuilderWrapper(b, o -> appendValue(b, o, type));
    }

    public static void appendValue(Block.Builder builder, Object val, ElementType type) {
        if (val == null) {
            builder.appendNull();
            return;
        }
        switch (type) {
            case LONG -> ((LongBlock.Builder) builder).appendLong((Long) val);
            case INT -> ((IntBlock.Builder) builder).appendInt((Integer) val);
            case BYTES_REF -> ((BytesRefBlock.Builder) builder).appendBytesRef(toBytesRef(val));
            case FLOAT -> ((FloatBlock.Builder) builder).appendFloat((Float) val);
            case DOUBLE -> ((DoubleBlock.Builder) builder).appendDouble((Double) val);
            case BOOLEAN -> ((BooleanBlock.Builder) builder).appendBoolean((Boolean) val);
            default -> throw new UnsupportedOperationException("unsupported element type [" + type + "]");
        }
    }

    public static Block constantBlock(BlockFactory blockFactory, Object val, int size) {
        if (val == null) {
            return blockFactory.newConstantNullBlock(size);
        }
        return constantBlock(blockFactory, fromJava(val.getClass()), val, size);
    }

    // TODO: allow null values
    private static Block constantBlock(BlockFactory blockFactory, ElementType type, Object val, int size) {
        return switch (type) {
            case NULL -> blockFactory.newConstantNullBlock(size);
            case LONG -> blockFactory.newConstantLongBlockWith((long) val, size);
            case INT -> blockFactory.newConstantIntBlockWith((int) val, size);
            case BYTES_REF -> blockFactory.newConstantBytesRefBlockWith(toBytesRef(val), size);
            case DOUBLE -> blockFactory.newConstantDoubleBlockWith((double) val, size);
            case BOOLEAN -> blockFactory.newConstantBooleanBlockWith((boolean) val, size);
            case AGGREGATE_METRIC_DOUBLE -> blockFactory.newConstantAggregateMetricDoubleBlock((AggregateMetricDoubleLiteral) val, size);
            default -> throw new UnsupportedOperationException("unsupported element type [" + type + "]");
        };
    }

    /**
     * Returned by {@link #toJavaObject} for "doc" type blocks.
     */
    public record Doc(int shard, int segment, int doc) {}

    /**
     * Read all values from a positions into a java object. This is not fast
     * but fine to call in the "fold" path.
     */
    public static Object toJavaObject(Block block, int position) {
        if (block.isNull(position)) {
            return null;
        }
        int count = block.getValueCount(position);
        int start = block.getFirstValueIndex(position);
        if (count == 1) {
            return valueAtOffset(block, start);
        }
        int end = start + count;
        List<Object> result = new ArrayList<>(count);
        for (int i = start; i < end; i++) {
            result.add(valueAtOffset(block, i));
        }
        return result;
    }

    private static Object valueAtOffset(Block block, int offset) {
        return switch (block.elementType()) {
            case BOOLEAN -> ((BooleanBlock) block).getBoolean(offset);
            case BYTES_REF -> BytesRef.deepCopyOf(((BytesRefBlock) block).getBytesRef(offset, new BytesRef()));
            case DOUBLE -> ((DoubleBlock) block).getDouble(offset);
            case FLOAT -> ((FloatBlock) block).getFloat(offset);
            case INT -> ((IntBlock) block).getInt(offset);
            case LONG -> ((LongBlock) block).getLong(offset);
            case NULL -> null;
            case DOC -> {
                DocVector v = ((DocBlock) block).asVector();
                yield new Doc(v.shards().getInt(offset), v.segments().getInt(offset), v.docs().getInt(offset));
            }
            case COMPOSITE -> throw new IllegalArgumentException("can't read values from composite blocks");
            case AGGREGATE_METRIC_DOUBLE -> {
                AggregateMetricDoubleBlock aggBlock = (AggregateMetricDoubleBlock) block;
                yield new AggregateMetricDoubleLiteral(
                    aggBlock.minBlock().getDouble(offset),
                    aggBlock.maxBlock().getDouble(offset),
                    aggBlock.sumBlock().getDouble(offset),
                    aggBlock.countBlock().getInt(offset)
                );
            }
            case UNKNOWN -> throw new IllegalArgumentException("can't read values from [" + block + "]");
        };
    }
}
