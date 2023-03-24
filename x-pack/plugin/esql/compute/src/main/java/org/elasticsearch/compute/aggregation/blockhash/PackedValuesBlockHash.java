/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation.blockhash;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.BytesRefArray;
import org.elasticsearch.common.util.BytesRefHash;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanArrayVector;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefArrayVector;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleArrayVector;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntArrayVector;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.IntVector;
import org.elasticsearch.compute.data.LongArrayVector;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.compute.operator.HashAggregationOperator;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;

/**
 * {@link BlockHash} implementation that can operate on any number of columns.
 * Works by concatenating the values of each column into a byte array and hashing
 * that.
 */
final class PackedValuesBlockHash extends BlockHash {
    private final Key[] keys;
    private final BytesRefHash bytesRefHash;

    PackedValuesBlockHash(List<HashAggregationOperator.GroupSpec> groups, BigArrays bigArrays) {
        this.keys = groups.stream().map(s -> switch (s.elementType()) {
            case BYTES_REF -> new BytesRefKey(s.channel());
            case BOOLEAN -> new BooleanKey(s.channel());
            case INT -> new IntKey(s.channel());
            case LONG -> new LongKey(s.channel());
            case DOUBLE -> new DoubleKey(s.channel());
            default -> throw new IllegalArgumentException("unsupported type [" + s.elementType() + "]");
        }).toArray(PackedValuesBlockHash.Key[]::new);
        this.bytesRefHash = new BytesRefHash(1, bigArrays);
    }

    @Override
    public LongBlock add(Page page) {
        KeyWork[] work = new KeyWork[page.getPositionCount()];
        for (int i = 0; i < work.length; i++) {
            work[i] = new KeyWork();
        }
        for (Key k : keys) {
            k.buildKeys(page, work);
        }

        LongBlock.Builder builder = LongBlock.newBlockBuilder(page.getPositionCount());
        for (KeyWork w : work) {
            if (w.isNull) {
                builder.appendNull();
            } else {
                builder.appendLong(hashOrdToGroup(bytesRefHash.add(w.builder.get())));
            }
        }
        return builder.build();
    }

    @Override
    public Block[] getKeys() {
        int[] positions = new int[Math.toIntExact(bytesRefHash.size())];
        BytesRefArray bytes = bytesRefHash.getBytesRefs();
        BytesRef scratch = new BytesRef();

        Block[] keyBlocks = new Block[keys.length];
        for (int i = 0; i < keyBlocks.length; i++) {
            keyBlocks[i] = keys[i].getKeys(positions, bytes, scratch);
        }
        return keyBlocks;
    }

    @Override
    public IntVector nonEmpty() {
        return IntVector.range(0, Math.toIntExact(bytesRefHash.size()));
    }

    @Override
    public void close() {
        bytesRefHash.close();
    }

    private class KeyWork {
        final BytesRefBuilder builder = new BytesRefBuilder();
        boolean isNull;

        @Override
        public String toString() {
            return "KeyWork{builder=" + builder.toBytesRef() + ", isNull=" + isNull + '}';
        }
    }

    interface Key {
        void buildKeys(Page page, KeyWork[] keyWork);

        Block getKeys(int[] positions, BytesRefArray bytes, BytesRef scratch);
    }

    private record BytesRefKey(int channel) implements Key {
        private static final VarHandle intHandle = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());

        @Override
        public void buildKeys(Page page, KeyWork[] work) {
            BytesRef scratch = new BytesRef();
            BytesRefBlock block = page.getBlock(channel);
            for (int i = 0; i < work.length; i++) {
                KeyWork w = work[i];
                if (w.isNull) {
                    continue;
                }
                if (block.isNull(i)) {
                    w.isNull = true;
                    continue;
                }
                block.getBytesRef(block.getFirstValueIndex(i), scratch);

                // Add the length of the bytes as an int and then the bytes
                int newLen = w.builder.length() + scratch.length + Integer.BYTES;
                w.builder.grow(newLen);
                intHandle.set(w.builder.bytes(), w.builder.length(), scratch.length);
                System.arraycopy(scratch.bytes, scratch.offset, w.builder.bytes(), w.builder.length() + Integer.BYTES, scratch.length);
                w.builder.setLength(newLen);
            }
        }

        @Override
        public Block getKeys(int[] positions, BytesRefArray bytes, BytesRef scratch) {
            BytesRefArray keys = new BytesRefArray(positions.length, BigArrays.NON_RECYCLING_INSTANCE);
            for (int i = 0; i < positions.length; i++) {
                bytes.get(i, scratch);
                if (scratch.length - positions[i] < Integer.BYTES) {
                    throw new IllegalStateException();
                }
                int lengthPosition = scratch.offset + positions[i];
                int len = (int) intHandle.get(scratch.bytes, lengthPosition);
                if (scratch.length + Integer.BYTES < len) {
                    throw new IllegalStateException();
                }
                scratch.length = len;
                scratch.offset = lengthPosition + Integer.BYTES;
                keys.append(scratch);
                positions[i] += scratch.length + Integer.BYTES;
            }
            return new BytesRefArrayVector(keys, positions.length).asBlock();
        }
    }

    private record LongKey(int channel) implements Key {
        private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());
        private static final int KEY_BYTES = Long.BYTES;

        @Override
        public void buildKeys(Page page, KeyWork[] work) {
            LongBlock block = page.getBlock(channel);
            for (int i = 0; i < work.length; i++) {
                KeyWork w = work[i];
                if (w.isNull) {
                    continue;
                }
                if (block.isNull(i)) {
                    w.isNull = true;
                    continue;
                }
                long value = block.getLong(block.getFirstValueIndex(i));
                int newLen = w.builder.length() + KEY_BYTES;
                w.builder.grow(newLen);
                longHandle.set(w.builder.bytes(), w.builder.length(), value);
                w.builder.setLength(newLen);
            }
        }

        @Override
        public Block getKeys(int[] positions, BytesRefArray bytes, BytesRef scratch) {
            final long[] keys = new long[positions.length];
            for (int i = 0; i < keys.length; i++) {
                bytes.get(i, scratch);
                if (scratch.length - positions[i] < KEY_BYTES) {
                    throw new IllegalStateException();
                }
                keys[i] = (long) longHandle.get(scratch.bytes, scratch.offset + positions[i]);
                positions[i] += KEY_BYTES;
            }
            return new LongArrayVector(keys, keys.length).asBlock();
        }
    }

    private record DoubleKey(int channel) implements Key {
        private static final VarHandle doubleHandle = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.nativeOrder());
        private static final int KEY_BYTES = Double.BYTES;

        @Override
        public void buildKeys(Page page, KeyWork[] work) {
            DoubleBlock block = page.getBlock(channel);
            for (int i = 0; i < work.length; i++) {
                KeyWork w = work[i];
                if (w.isNull) {
                    continue;
                }
                if (block.isNull(i)) {
                    w.isNull = true;
                    continue;
                }
                int newLen = w.builder.length() + KEY_BYTES;
                w.builder.grow(newLen);
                double value = block.getDouble(block.getFirstValueIndex(i));
                doubleHandle.set(w.builder.bytes(), w.builder.length(), value);
                w.builder.setLength(newLen);
            }
        }

        @Override
        public Block getKeys(int[] positions, BytesRefArray bytes, BytesRef scratch) {
            final double[] keys = new double[positions.length];
            for (int i = 0; i < keys.length; i++) {
                bytes.get(i, scratch);
                if (scratch.length - positions[i] < KEY_BYTES) {
                    throw new IllegalStateException();
                }
                keys[i] = (double) doubleHandle.get(scratch.bytes, scratch.offset + positions[i]);
                positions[i] += KEY_BYTES;
            }
            return new DoubleArrayVector(keys, keys.length).asBlock();
        }
    }

    private record IntKey(int channel) implements Key {
        private static final VarHandle intHandle = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());
        private static final int KEY_BYTES = Integer.BYTES;

        @Override
        public void buildKeys(Page page, KeyWork[] work) {
            IntBlock block = page.getBlock(channel);
            for (int i = 0; i < work.length; i++) {
                KeyWork w = work[i];
                if (w.isNull) {
                    continue;
                }
                if (block.isNull(i)) {
                    w.isNull = true;
                    continue;
                }
                int value = block.getInt(block.getFirstValueIndex(i));
                int newLen = w.builder.length() + KEY_BYTES;
                w.builder.grow(newLen);
                intHandle.set(w.builder.bytes(), w.builder.length(), value);
                w.builder.setLength(newLen);
            }
        }

        @Override
        public Block getKeys(int[] positions, BytesRefArray bytes, BytesRef scratch) {
            final int[] keys = new int[positions.length];
            for (int i = 0; i < keys.length; i++) {
                bytes.get(i, scratch);
                if (scratch.length - positions[i] < KEY_BYTES) {
                    throw new IllegalStateException();
                }
                keys[i] = (int) intHandle.get(scratch.bytes, scratch.offset + positions[i]);
                positions[i] += KEY_BYTES;
            }
            return new IntArrayVector(keys, keys.length).asBlock();
        }
    }

    private record BooleanKey(int channel) implements Key {
        private static final VarHandle byteHandle = MethodHandles.arrayElementVarHandle(byte[].class);
        private static final int KEY_BYTES = Byte.BYTES;

        @Override
        public void buildKeys(Page page, KeyWork[] work) {
            BooleanBlock block = page.getBlock(channel);
            for (int i = 0; i < work.length; i++) {
                KeyWork w = work[i];
                if (w.isNull) {
                    continue;
                }
                if (block.isNull(i)) {
                    w.isNull = true;
                    continue;
                }
                boolean value = block.getBoolean(block.getFirstValueIndex(i));
                int newLen = w.builder.length() + KEY_BYTES;
                w.builder.grow(newLen);
                // Serialize boolean as a byte (true: 1, false: 0)
                byteHandle.set(w.builder.bytes(), w.builder.length(), value ? (byte) 1 : 0);
                w.builder.setLength(newLen);
            }
        }

        @Override
        public Block getKeys(int[] positions, BytesRefArray bytes, BytesRef scratch) {
            final boolean[] keys = new boolean[positions.length];
            for (int i = 0; i < keys.length; i++) {
                bytes.get(i, scratch);
                if (scratch.length - positions[i] < KEY_BYTES) {
                    throw new IllegalStateException();
                }
                // Deserialize byte to boolean (true: 1, false: 0)
                keys[i] = (byte) byteHandle.get(scratch.bytes, scratch.offset + positions[i]) != 0;
                positions[i] += KEY_BYTES;
            }
            return new BooleanArrayVector(keys, keys.length).asBlock();
        }
    }

    @Override
    public String toString() {
        return "PackedValuesBlockHash{keys="
            + Arrays.toString(keys)
            + ", entries="
            + bytesRefHash.size()
            + ", size="
            + ByteSizeValue.ofBytes(bytesRefHash.ramBytesUsed())
            + '}';
    }
}
