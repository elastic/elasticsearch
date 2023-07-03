/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

public abstract class BatchEncoder implements Accountable {
    /**
     * Decodes values encoded by {@link BatchEncoder}.
     */
    public interface Decoder {
        void decode(Block.Builder builder, BytesRef[] encoded, int count);
    }

    /**
     * Get a {@link Decoder} for the provided {@link ElementType}.
     */
    public static Decoder decoder(ElementType elementType) {
        return switch (elementType) {
            case INT -> new IntsDecoder();
            case LONG -> new LongsDecoder();
            case DOUBLE -> new DoublesDecoder();
            case BYTES_REF -> new BytesRefsDecoder();
            case BOOLEAN -> new BooleansDecoder();
            default -> throw new IllegalArgumentException("can't encode " + elementType);
        };
    }

    private static long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(BatchEncoder.class);

    /**
     * Buffer into which we encode values.
     */
    protected final BytesRefBuilder bytes = new BytesRefBuilder();

    /**
     * Count of values at each position.
     */
    private int[] counts = new int[ArrayUtil.oversize(10, Integer.BYTES)];

    /**
     * Offsets into the {@link #bytes} for each value.
     */
    private int[] valueOffsets = new int[ArrayUtil.oversize(10, Integer.BYTES)];

    /**
     * The first position in the current batch.
     */
    private int firstPosition;

    /**
     * The number of positions in the current batch. It's also the maximum index into
     * {@link #counts} that has an meaning.
     */
    private int positionCount;

    /**
     * The value being encoded right now.
     */
    private int currentValue;

    /**
     * Build the encoder.
     * @param batchSize The number of bytes in a batch. We'll allocate this much memory for the
     *                  encoder and only expand the allocation if the first entry in a batch
     *                  doesn't fit into the buffer.
     */
    BatchEncoder(int batchSize) {
        bytes.grow(batchSize);
    }

    /**
     * The first position in the current batch.
     */
    public int firstPosition() {
        return firstPosition;
    }

    /**
     * The number of positions in the current batch.
     */
    public int positionCount() {
        return positionCount;
    }

    /**
     * The number of values at the position with this offset in the batch.
     * The actual position in the block we're encoding is {@code positionOffset + firstPosition()}.
     */
    public int valueCount(int positionOffset) {
        if (positionOffset >= positionCount) {
            throw new IllegalArgumentException("wanted " + positionOffset + " but only have " + positionCount);
        }
        return counts[positionOffset];
    }

    /**
     * Read the value at the specified index. Values at the first position
     * start at index {@code 0} and advance one per value. So the values
     * at position n start at {@code (0..n-1).sum(valueCount)}. There is
     * no random-access way to get the first index for a position.
     */
    public final BytesRef read(int index, BytesRef scratch) {
        scratch.bytes = bytes.bytes();
        scratch.offset = valueOffsets[index];
        scratch.length = valueOffsets[index + 1] - scratch.offset;
        return scratch;
    }

    /**
     * Encodes the next batch of entries. This will encode values until the next
     * value doesn't fit into the buffer. Callers should iterate on the values
     * that have been encoded and then call this again for the next batch.
     * <p>
     *     It's possible for this batch to be empty if there isn't room for the
     *     first entry in the buffer. If so, call again to force the buffer to
     *     expand and encode that entry.
     * </p>
     */
    public final void encodeNextBatch() {
        bytes.clear();
        firstPosition += positionCount;
        positionCount = 0;
        currentValue = 0;
        readNextBatch();
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(counts) + RamUsageEstimator.sizeOf(valueOffsets);
    }

    /**
     * Encodes the next batch of values. See {@link #encodeNextBatch()}.
     */
    protected abstract void readNextBatch();

    /**
     * Implementations of {@link #readNextBatch} should call this before any
     * values at the current position.
     */
    protected final void startPosition() {
        counts = ArrayUtil.grow(counts, positionCount + 1);
        counts[positionCount] = 0;
    }

    /**
     * Implementations of {@link #readNextBatch} should call this before adding
     * each value to the current position to mark its start.
     */
    protected final void addingValue() {
        counts[positionCount]++;
        valueOffsets = ArrayUtil.grow(valueOffsets, currentValue + 1);
        valueOffsets[currentValue++] = bytes.length();
    }

    /**
     * Implementations of {@link #readNextBatch} should call this to end
     * the current position.
     */
    protected final void endPosition() {
        valueOffsets = ArrayUtil.grow(valueOffsets, currentValue + 1);
        valueOffsets[currentValue] = bytes.length();
        positionCount++;
    }

    /**
     * Implementations of {@link #readNextBatch} should call this to encode
     * an entirely null position.
     */
    protected final void encodeNull() {
        startPosition();
        addingValue();
        endPosition();
    }

    /**
     * The number of bytes in all entries in the batch.
     */
    final int bytesLength() {
        return bytes.length();
    }

    /**
     * The maximum batch size. This starts the same as the constructor parameter
     * but will grow if a single entry doesn't fit into the batch.
     */
    final int bytesCapacity() {
        return bytes.bytes().length;
    }

    private static final VarHandle intHandle = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());

    protected abstract static class Ints extends BatchEncoder {
        protected Ints(int batchSize) {
            super(batchSize);
        }

        /**
         * Is there capacity for this many {@code int}s?
         */
        protected final boolean hasCapacity(int count) {
            return bytes.length() + count * Integer.BYTES <= bytesCapacity();
        }

        /**
         * Make sure there is capacity for this many {@code int}s, growing
         * the buffer if needed.
         */
        protected final void ensureCapacity(int count) {
            // TODO some protection against growing to gigabytes or whatever
            bytes.grow(count * Integer.BYTES);
        }

        /**
         * Encode an {@code int} into the current position and advance
         * to the next position.
         */
        protected final void encode(int v) {
            addingValue();
            intHandle.set(bytes.bytes(), bytes.length(), v);
            bytes.setLength(bytes.length() + Integer.BYTES);
        }
    }

    private static class IntsDecoder implements Decoder {
        @Override
        public void decode(Block.Builder builder, BytesRef[] encoded, int count) {
            IntBlock.Builder b = (IntBlock.Builder) builder;
            for (int i = 0; i < count; i++) {
                BytesRef e = encoded[i];
                if (e.length == 0) {
                    b.appendNull();
                } else {
                    b.appendInt((int) intHandle.get(e.bytes, e.offset));
                    e.offset += Integer.BYTES;
                    e.length -= Integer.BYTES;
                }
            }
        }
    }

    private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

    protected abstract static class Longs extends BatchEncoder {
        protected Longs(int batchSize) {
            super(batchSize);
        }

        /**
         * Is there capacity for this many {@code long}s?
         */
        protected final boolean hasCapacity(int count) {
            return bytes.length() + count * Long.BYTES <= bytesCapacity();
        }

        /**
         * Make sure there is capacity for this many {@code long}s, growing
         * the buffer if needed.
         */
        protected final void ensureCapacity(int count) {
            // TODO some protection against growing to gigabytes or whatever
            bytes.grow(count * Long.BYTES);
        }

        /**
         * Encode a {@code long} and advance to the next position.
         */
        protected final void encode(long v) {
            addingValue();
            longHandle.set(bytes.bytes(), bytes.length(), v);
            bytes.setLength(bytes.length() + Long.BYTES);
        }
    }

    private static class LongsDecoder implements Decoder {
        @Override
        public void decode(Block.Builder builder, BytesRef[] encoded, int count) {
            LongBlock.Builder b = (LongBlock.Builder) builder;
            for (int i = 0; i < count; i++) {
                BytesRef e = encoded[i];
                if (e.length == 0) {
                    b.appendNull();
                } else {
                    b.appendLong((long) longHandle.get(e.bytes, e.offset));
                    e.offset += Long.BYTES;
                    e.length -= Long.BYTES;
                }
            }
        }

    }

    private static final VarHandle doubleHandle = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.nativeOrder());

    protected abstract static class Doubles extends BatchEncoder {
        protected Doubles(int batchSize) {
            super(batchSize);
        }

        /**
         * Is there capacity for this many {@code double}s?
         */
        protected final boolean hasCapacity(int count) {
            return bytes.length() + count * Double.BYTES <= bytesCapacity();
        }

        /**
         * Make sure there is capacity for this many {@code double}s, growing
         * the buffer if needed.
         */
        protected final void ensureCapacity(int count) {
            // TODO some protection against growing to gigabytes or whatever
            bytes.grow(count * Double.BYTES);
        }

        /**
         * Encode a {@code double} and advance to the next position.
         */
        protected final void encode(double v) {
            addingValue();
            doubleHandle.set(bytes.bytes(), bytes.length(), v);
            bytes.setLength(bytes.length() + Double.BYTES);
        }
    }

    private static class DoublesDecoder implements Decoder {
        @Override
        public void decode(Block.Builder builder, BytesRef[] encoded, int count) {
            DoubleBlock.Builder b = (DoubleBlock.Builder) builder;
            for (int i = 0; i < count; i++) {
                BytesRef e = encoded[i];
                if (e.length == 0) {
                    b.appendNull();
                } else {
                    b.appendDouble((double) doubleHandle.get(e.bytes, e.offset));
                    e.offset += Double.BYTES;
                    e.length -= Double.BYTES;
                }
            }
        }
    }

    protected abstract static class Booleans extends BatchEncoder {
        protected Booleans(int batchSize) {
            super(batchSize);
        }

        /**
         * Is there capacity for this many {@code booleans}s?
         */
        protected final boolean hasCapacity(int count) {
            return bytes.length() + count <= bytesCapacity();
        }

        /*
         * There isn't an ensureCapacity here because the only user presently
         * deduplicates values and there are only two possible boolean values.
         * Which will always fit into any reasonable sized buffer.
         */

        /**
         * Encode a {@code boolean} and advance to the next position.
         */
        protected final void encode(boolean v) {
            addingValue();
            bytes.bytes()[bytes.length()] = (byte) (v ? 1 : 0);
            bytes.setLength(bytes.length() + 1);
        }
    }

    private static class BooleansDecoder implements Decoder {
        @Override
        public void decode(Block.Builder builder, BytesRef[] encoded, int count) {
            BooleanBlock.Builder b = (BooleanBlock.Builder) builder;
            for (int i = 0; i < count; i++) {
                BytesRef e = encoded[i];
                if (e.length == 0) {
                    b.appendNull();
                } else {
                    b.appendBoolean(e.bytes[e.offset] == 1);
                    e.offset++;
                    e.length--;
                }
            }
        }
    }

    protected abstract static class BytesRefs extends BatchEncoder {
        protected BytesRefs(int batchSize) {
            super(batchSize);
        }

        /**
         * Is there capacity for {@code totalBytes} and spread across
         * {@code #count} {@link BytesRef}s? You could call this with something
         * like {@code hasCapacity(Arrays.stream(bytes).mapToInt(b -> b.length).sum(), bytes.length)}.
         */
        protected final boolean hasCapacity(int totalBytes, int count) {
            return bytes.length() + totalBytes + count * Integer.BYTES <= bytesCapacity();
        }

        /**
         * Make sure there is capacity for {@code totalBytes} and spread across
         * {@code #count} {@link BytesRef}s? You could call this with something
         * like {@code ensureCapacity(Arrays.stream(bytes).mapToInt(b -> b.length).sum(), bytes.length)}.
         */
        protected final void ensureCapacity(int totalBytes, int count) {
            // TODO some protection against growing to gigabytes or whatever
            bytes.grow(totalBytes + count * Integer.BYTES);
        }

        /**
         * Encode a {@link BytesRef} and advance to the next position.
         */
        protected final void encode(BytesRef v) {
            addingValue();
            intHandle.set(bytes.bytes(), bytes.length(), v.length);
            bytes.setLength(bytes.length() + Integer.BYTES);
            bytes.append(v);
        }
    }

    private static class BytesRefsDecoder implements Decoder {
        @Override
        public void decode(Block.Builder builder, BytesRef[] encoded, int count) {
            BytesRef scratch = new BytesRef();
            BytesRefBlock.Builder b = (BytesRefBlock.Builder) builder;
            for (int i = 0; i < count; i++) {
                BytesRef e = encoded[i];
                if (e.length == 0) {
                    b.appendNull();
                } else {
                    scratch.bytes = e.bytes;
                    scratch.length = (int) intHandle.get(e.bytes, e.offset);
                    e.offset += Integer.BYTES;
                    e.length -= Integer.BYTES;
                    scratch.offset = e.offset;
                    b.appendBytesRef(scratch);
                    e.offset += scratch.length;
                    e.length -= scratch.length;
                }
            }
        }
    }
}
