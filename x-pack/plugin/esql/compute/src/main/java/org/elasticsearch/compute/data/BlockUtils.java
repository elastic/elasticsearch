/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

import static org.elasticsearch.common.lucene.BytesRefs.toBytesRef;
import static org.elasticsearch.compute.data.Block.constantNullBlock;

public final class BlockUtils {

    public static final Block[] NO_BLOCKS = new Block[0];

    private BlockUtils() {}

    public record BuilderWrapper(Block.Builder builder, Consumer<Object> append) {
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
    }

    public static Block[] fromArrayRow(Object... row) {
        return fromListRow(Arrays.asList(row));
    }

    public static Block[] fromListRow(List<Object> row) {
        return fromListRow(row, 1);
    }

    public static Block[] fromListRow(List<Object> row, int blockSize) {
        if (row.isEmpty()) {
            return NO_BLOCKS;
        }

        var size = row.size();
        Block[] blocks = new Block[size];
        for (int i = 0; i < size; i++) {
            Object object = row.get(i);
            if (object instanceof Integer intVal) {
                blocks[i] = IntBlock.newConstantBlockWith(intVal, blockSize);
            } else if (object instanceof Long longVal) {
                blocks[i] = LongBlock.newConstantBlockWith(longVal, blockSize);
            } else if (object instanceof Double doubleVal) {
                blocks[i] = DoubleBlock.newConstantBlockWith(doubleVal, blockSize);
            } else if (object instanceof BytesRef bytesRefVal) {
                blocks[i] = BytesRefBlock.newConstantBlockWith(bytesRefVal, blockSize);
            } else if (object instanceof Boolean booleanVal) {
                blocks[i] = BooleanBlock.newConstantBlockWith(booleanVal, blockSize);
            } else if (object instanceof List<?> listVal) {
                BuilderWrapper wrapper = wrapperFor(listVal.get(0).getClass(), 1);
                wrapper.append.accept(listVal);
                blocks[i] = wrapper.builder.build();
            } else if (object == null) {
                blocks[i] = constantNullBlock(blockSize);
            } else {
                throw new UnsupportedOperationException("can't make a block out of [" + object + "/" + object.getClass() + "]");
            }
        }
        return blocks;
    }

    public static Block[] fromList(List<List<Object>> list) {
        var size = list.size();
        if (size == 0) {
            return NO_BLOCKS;
        }
        if (size == 1) {
            return fromListRow(list.get(0));
        }

        var wrappers = new BuilderWrapper[list.get(0).size()];

        for (int i = 0; i < wrappers.length; i++) {
            wrappers[i] = wrapperFor(type(list, i), size);
        }
        for (List<Object> values : list) {
            for (int j = 0, vSize = values.size(); j < vSize; j++) {
                wrappers[j].append.accept(values.get(j));
            }
        }
        return Arrays.stream(wrappers).map(b -> b.builder.build()).toArray(Block[]::new);
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

    public static BuilderWrapper wrapperFor(Class<?> type, int size) {
        BuilderWrapper builder;
        if (type == Integer.class) {
            var b = IntBlock.newBlockBuilder(size);
            builder = new BuilderWrapper(b, o -> b.appendInt((int) o));
        } else if (type == Long.class) {
            var b = LongBlock.newBlockBuilder(size);
            builder = new BuilderWrapper(b, o -> b.appendLong((long) o));
        } else if (type == Double.class) {
            var b = DoubleBlock.newBlockBuilder(size);
            builder = new BuilderWrapper(b, o -> b.appendDouble((double) o));
        } else if (type == BytesRef.class) {
            var b = BytesRefBlock.newBlockBuilder(size);
            builder = new BuilderWrapper(b, o -> b.appendBytesRef(BytesRefs.toBytesRef(o)));
        } else if (type == Boolean.class) {
            var b = BooleanBlock.newBlockBuilder(size);
            builder = new BuilderWrapper(b, o -> b.appendBoolean((boolean) o));
        } else if (type == null) {
            var b = new Block.Builder() {
                @Override
                public Block.Builder appendNull() {
                    return this;
                }

                @Override
                public Block.Builder beginPositionEntry() {
                    return this;
                }

                @Override
                public Block.Builder endPositionEntry() {
                    return this;
                }

                @Override
                public Block.Builder copyFrom(Block block, int beginInclusive, int endExclusive) {
                    return this;
                }

                @Override
                public Block.Builder mvOrdering(Block.MvOrdering mvOrdering) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public Block build() {
                    return constantNullBlock(size);
                }
            };
            builder = new BuilderWrapper(b, o -> {});
        } else {
            throw new UnsupportedOperationException("Unrecognized type " + type);
        }
        return builder;
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
            case DOUBLE -> ((DoubleBlock.Builder) builder).appendDouble((Double) val);
            case BOOLEAN -> ((BooleanBlock.Builder) builder).appendBoolean((Boolean) val);
            default -> throw new UnsupportedOperationException("unsupported element type [" + type + "]");
        }
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
            case BYTES_REF -> ((BytesRefBlock) block).getBytesRef(offset, new BytesRef());
            case DOUBLE -> ((DoubleBlock) block).getDouble(offset);
            case INT -> ((IntBlock) block).getInt(offset);
            case LONG -> ((LongBlock) block).getLong(offset);
            case NULL -> null;
            case DOC -> {
                DocVector v = ((DocBlock) block).asVector();
                yield new Doc(v.shards().getInt(offset), v.segments().getInt(offset), v.docs().getInt(offset));
            }
            case UNKNOWN -> throw new IllegalArgumentException("can't read values from [" + block + "]");
        };
    }
}
