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
     * Checks if an offset is {@code null}.
     */
    public interface IsNull {
        boolean isNull(int offset);
    }

    /**
     * Decodes values encoded by {@link BatchEncoder}.
     */
    public interface Decoder {
        void decode(Block.Builder builder, IsNull isNull, BytesRef[] encoded, int count);
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

    /**
     * The number of positions in the current batch.
     */
    public abstract int positionCount();

    /**
     * The number of values at the position with this offset in the batch.
     */
    public abstract int valueCount(int positionOffset);

    /**
     * Read the value at the specified index then append to the {@code dst}.
     * Values at the first position start at index {@code 0} and advance one per value.
     * So the values at position n start at {@code (0..n-1).sum(valueCount)}. There is
     * no random-access way to get the first index for a position.
     *
     * @return the number of bytes has read
     */
    public abstract int read(int index, BytesRefBuilder dst);

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
    public abstract void encodeNextBatch();

    protected abstract static class MVEncoder extends BatchEncoder {
        private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(MVEncoder.class);

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
        MVEncoder(int batchSize) {
            bytes.grow(batchSize);
        }

        /**
         * The first position in the current batch.
         */
        protected final int firstPosition() {
            return firstPosition;
        }

        /**
         * The number of positions in the current batch.
         */
        @Override
        public final int positionCount() {
            return positionCount;
        }

        /**
         * The number of values at the position with this offset in the batch.
         * The actual position in the block we're encoding is {@code positionOffset + firstPosition()}.
         */
        @Override
        public final int valueCount(int positionOffset) {
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
        @Override
        public final int read(int index, BytesRefBuilder dst) {
            int start = valueOffsets[index];
            int length = valueOffsets[index + 1] - start;
            if (length > 0) {
                dst.append(bytes.bytes(), start, length);
            }
            return length;
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
        @Override
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
    }

    protected abstract static class DirectEncoder extends BatchEncoder {
        protected static final long BASE_RAM_USAGE = RamUsageEstimator.shallowSizeOfInstance(DirectEncoder.class);
        protected final Block block;
        private int blockPosition = -1;
        private int valueCount;

        DirectEncoder(Block block) {
            this.block = block;
        }

        @Override
        public final void encodeNextBatch() {
            valueCount = block.getValueCount(++blockPosition);
        }

        @Override
        public final int positionCount() {
            return Math.max(valueCount, 1);
        }

        @Override
        public final int valueCount(int positionOffset) {
            assert positionOffset == 0 : positionOffset;
            return positionCount();
        }

        @Override
        public int read(int index, BytesRefBuilder dst) {
            if (valueCount == 0) {
                assert index == 0 : index;
                return 0;
            } else {
                assert index < valueCount : index + " > " + valueCount;
                return readValueAtBlockIndex(block.getFirstValueIndex(blockPosition) + index, dst);
            }
        }

        protected abstract int readValueAtBlockIndex(int valueIndex, BytesRefBuilder dst);

        @Override
        public final long ramBytesUsed() {
            return BASE_RAM_USAGE;
        }
    }

    private static final VarHandle intHandle = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.nativeOrder());

    protected abstract static class Ints extends MVEncoder {
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
        public void decode(Block.Builder builder, IsNull isNull, BytesRef[] encoded, int count) {
            IntBlock.Builder b = (IntBlock.Builder) builder;
            for (int i = 0; i < count; i++) {
                if (isNull.isNull(i)) {
                    b.appendNull();
                } else {
                    BytesRef e = encoded[i];
                    b.appendInt((int) intHandle.get(e.bytes, e.offset));
                    e.offset += Integer.BYTES;
                    e.length -= Integer.BYTES;
                }
            }
        }
    }

    protected static final class DirectInts extends DirectEncoder {
        DirectInts(IntBlock block) {
            super(block);
        }

        @Override
        protected int readValueAtBlockIndex(int valueIndex, BytesRefBuilder dst) {
            int before = dst.length();
            int after = before + Integer.BYTES;
            dst.grow(after);
            int v = ((IntBlock) block).getInt(valueIndex);
            intHandle.set(dst.bytes(), before, v);
            dst.setLength(after);
            return Integer.BYTES;
        }
    }

    private static final VarHandle longHandle = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.nativeOrder());

    protected abstract static class Longs extends MVEncoder {
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

    protected static final class DirectLongs extends DirectEncoder {
        DirectLongs(LongBlock block) {
            super(block);
        }

        @Override
        protected int readValueAtBlockIndex(int valueIndex, BytesRefBuilder dst) {
            int before = dst.length();
            int after = before + Long.BYTES;
            dst.grow(after);
            long v = ((LongBlock) block).getLong(valueIndex);
            longHandle.set(dst.bytes(), before, v);
            dst.setLength(after);
            return Long.BYTES;
        }
    }

    private static class LongsDecoder implements Decoder {
        @Override
        public void decode(Block.Builder builder, IsNull isNull, BytesRef[] encoded, int count) {
            LongBlock.Builder b = (LongBlock.Builder) builder;
            for (int i = 0; i < count; i++) {
                if (isNull.isNull(i)) {
                    b.appendNull();
                } else {
                    BytesRef e = encoded[i];
                    b.appendLong((long) longHandle.get(e.bytes, e.offset));
                    e.offset += Long.BYTES;
                    e.length -= Long.BYTES;
                }
            }
        }

    }

    private static final VarHandle doubleHandle = MethodHandles.byteArrayViewVarHandle(double[].class, ByteOrder.nativeOrder());

    protected abstract static class Doubles extends MVEncoder {
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

    protected static final class DirectDoubles extends DirectEncoder {
        DirectDoubles(DoubleBlock block) {
            super(block);
        }

        @Override
        protected int readValueAtBlockIndex(int valueIndex, BytesRefBuilder dst) {
            int before = dst.length();
            int after = before + Double.BYTES;
            dst.grow(after);
            double v = ((DoubleBlock) block).getDouble(valueIndex);
            doubleHandle.set(dst.bytes(), before, v);
            dst.setLength(after);
            return Double.BYTES;
        }
    }

    private static class DoublesDecoder implements Decoder {
        @Override
        public void decode(Block.Builder builder, IsNull isNull, BytesRef[] encoded, int count) {
            DoubleBlock.Builder b = (DoubleBlock.Builder) builder;
            for (int i = 0; i < count; i++) {
                if (isNull.isNull(i)) {
                    b.appendNull();
                } else {
                    BytesRef e = encoded[i];
                    b.appendDouble((double) doubleHandle.get(e.bytes, e.offset));
                    e.offset += Double.BYTES;
                    e.length -= Double.BYTES;
                }
            }
        }
    }

    protected abstract static class Booleans extends MVEncoder {
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

    protected static final class DirectBooleans extends DirectEncoder {
        DirectBooleans(BooleanBlock block) {
            super(block);
        }

        @Override
        protected int readValueAtBlockIndex(int valueIndex, BytesRefBuilder dst) {
            var v = ((BooleanBlock) block).getBoolean(valueIndex);
            dst.append((byte) (v ? 1 : 0));
            return 1;
        }
    }

    private static class BooleansDecoder implements Decoder {
        @Override
        public void decode(Block.Builder builder, IsNull isNull, BytesRef[] encoded, int count) {
            BooleanBlock.Builder b = (BooleanBlock.Builder) builder;
            for (int i = 0; i < count; i++) {
                if (isNull.isNull(i)) {
                    b.appendNull();
                } else {
                    BytesRef e = encoded[i];
                    b.appendBoolean(e.bytes[e.offset] == 1);
                    e.offset++;
                    e.length--;
                }
            }
        }
    }

    protected abstract static class BytesRefs extends MVEncoder {
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

    protected static final class DirectBytesRefs extends DirectEncoder {
        private final BytesRef scratch = new BytesRef();

        DirectBytesRefs(BytesRefBlock block) {
            super(block);
        }

        @Override
        protected int readValueAtBlockIndex(int valueIndex, BytesRefBuilder dst) {
            var v = ((BytesRefBlock) block).getBytesRef(valueIndex, scratch);
            int start = dst.length();
            dst.grow(start + Integer.BYTES + v.length);
            intHandle.set(dst.bytes(), start, v.length);
            dst.setLength(start + Integer.BYTES);
            dst.append(v);
            return Integer.BYTES + v.length;
        }
    }

    private static class BytesRefsDecoder implements Decoder {
        @Override
        public void decode(Block.Builder builder, IsNull isNull, BytesRef[] encoded, int count) {
            BytesRef scratch = new BytesRef();
            BytesRefBlock.Builder b = (BytesRefBlock.Builder) builder;
            for (int i = 0; i < count; i++) {
                if (isNull.isNull(i)) {
                    b.appendNull();
                } else {
                    BytesRef e = encoded[i];
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

    protected static final class DirectNulls extends DirectEncoder {
        DirectNulls(Block block) {
            super(block);
            assert block.areAllValuesNull() : block;
        }

        @Override
        protected int readValueAtBlockIndex(int valueIndex, BytesRefBuilder dst) {
            assert false : "all positions all nulls";
            throw new IllegalStateException("all positions all nulls");
        }
    }
}
