/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.execution.sequence;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BooleanBlock;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.data.DoubleBlock;
import org.elasticsearch.compute.data.IntBlock;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.plan.logical.SequenceStage;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeConverter;

import java.util.List;
import java.util.Objects;

/**
 * Canonical equality keys for typed stage pages, without joining fields through text delimiters or lossy conversions.
 * Types and lengths make tuple boundaries unambiguous. Equality encoding and value ordering are separate:
 * {@link #compare} applies EQL's natural tuple ordering to expanded candidates at one source ordinal.
 * <p>
 * A null component excludes the row. A multivalued component fails the query and requires an explicit stage MV_EXPAND.
 * The execution serializes calls and closes the returned temporary key after the matcher has copied it.
 */
public final class SequenceKeyCodec implements Releasable {
    private static final String BREAKER_LABEL = "esql_sequence_key";
    private static final long BASE_BYTES = RamUsageEstimator.shallowSizeOfInstance(SequenceKeyCodec.class) + RamUsageEstimator
        .shallowSizeOfInstance(BytesRef.class);
    private static final long KEY_BYTES = RamUsageEstimator.shallowSizeOfInstance(EncodedKey.class) + RamUsageEstimator
        .shallowSizeOfInstance(BytesRef.class);

    private final CircuitBreaker breaker;
    private final long bytes;
    private final BytesRef scratch;
    private int[] channels;
    private DataType[] types;
    private boolean closed;

    /** Creates a codec for channels in one resolved stage's output schema. */
    public SequenceKeyCodec(List<DataType> types, int[] channels, CircuitBreaker breaker) {
        Objects.requireNonNull(types);
        Objects.requireNonNull(channels);
        this.breaker = Objects.requireNonNull(breaker);
        if (types.size() != channels.length) {
            throw new IllegalArgumentException("sequence key types and channels must align");
        }
        for (int key = 0; key < channels.length; key++) {
            if (channels[key] < 0 || types.get(key) == null || SequenceStage.supportsKeyType(types.get(key)) == false) {
                throw new IllegalArgumentException("invalid sequence key channel or type at position [" + key + "]");
            }
        }
        bytes = BASE_BYTES + arrayBytes(channels.length, Integer.BYTES) + arrayBytes(
            channels.length,
            RamUsageEstimator.NUM_BYTES_OBJECT_REF
        );
        breaker.addEstimateBytesAndMaybeBreak(bytes, BREAKER_LABEL);
        try {
            this.channels = channels.clone();
            this.types = types.toArray(DataType[]::new);
            scratch = new BytesRef();
        } catch (RuntimeException | Error e) {
            breaker.addWithoutBreaking(-bytes);
            throw e;
        }
    }

    /** Returns a separately charged copied key, or null when any component is null. */
    @Nullable
    public EncodedKey encode(Page page, int position) {
        if (closed) {
            throw new IllegalStateException("sequence key codec is closed");
        }
        if (position < 0 || position >= page.getPositionCount()) {
            throw new IllegalArgumentException("invalid sequence key row [" + position + "]");
        }
        int length = 0;
        boolean hasNull = false;
        for (int key = 0; key < channels.length; key++) {
            Block block = page.getBlock(channels[key]);
            int count = block.getValueCount(position);
            if (block.isNull(position) || count == 0) {
                hasNull = true;
                continue;
            }
            if (count != 1) {
                throw new IllegalArgumentException(
                    "SEQUENCE BY column at position [" + key + "] is multivalued; use MV_EXPAND inside the stage"
                );
            }
            int value = block.getFirstValueIndex(position);
            int valueBytes = switch (types[key]) {
                case BOOLEAN -> 1;
                case INTEGER -> Integer.BYTES;
                case LONG, UNSIGNED_LONG, DATETIME, DATE_NANOS, DOUBLE -> Long.BYTES;
                case KEYWORD, IP, VERSION -> Math.addExact(Integer.BYTES, byteLength((BytesRefBlock) block, value));
                case NULL -> throw new IllegalStateException("non-null value in a null-typed sequence key");
                default -> throw new IllegalStateException("unexpected sequence key type [" + types[key] + "]");
            };
            length = Math.addExact(length, Math.addExact(1, valueBytes));
        }
        if (hasNull) {
            return null;
        }
        long keyBytes = KEY_BYTES + arrayBytes(length, Byte.BYTES);
        breaker.addEstimateBytesAndMaybeBreak(keyBytes, BREAKER_LABEL);
        try {
            byte[] encoded = new byte[length];
            int offset = 0;
            for (int key = 0; key < channels.length; key++) {
                Block block = page.getBlock(channels[key]);
                int value = block.getFirstValueIndex(position);
                // These tags are local to this encoding, rather than DataType ordinals that may change as types are added.
                switch (types[key]) {
                    case BOOLEAN -> {
                        encoded[offset++] = 1;
                        encoded[offset++] = (byte) (((BooleanBlock) block).getBoolean(value) ? 1 : 0);
                    }
                    case INTEGER -> {
                        encoded[offset++] = 2;
                        offset = writeNumber(encoded, offset, ((IntBlock) block).getInt(value), Integer.BYTES);
                    }
                    case LONG, UNSIGNED_LONG, DATETIME, DATE_NANOS -> {
                        encoded[offset++] = switch (types[key]) {
                            case LONG -> 3;
                            case UNSIGNED_LONG -> 4;
                            case DATETIME -> 9;
                            case DATE_NANOS -> 10;
                            default -> throw new IllegalStateException("unexpected long sequence key type [" + types[key] + "]");
                        };
                        offset = writeNumber(encoded, offset, ((LongBlock) block).getLong(value), Long.BYTES);
                    }
                    case DOUBLE -> {
                        encoded[offset++] = 5;
                        offset = writeNumber(encoded, offset, Double.doubleToLongBits(((DoubleBlock) block).getDouble(value)), Long.BYTES);
                    }
                    case KEYWORD, IP, VERSION -> {
                        encoded[offset++] = switch (types[key]) {
                            case KEYWORD -> 6;
                            case IP -> 7;
                            case VERSION -> 8;
                            default -> throw new IllegalStateException("unexpected bytes sequence key type [" + types[key] + "]");
                        };
                        BytesRef ref = ((BytesRefBlock) block).getBytesRef(value, scratch);
                        offset = writeNumber(encoded, offset, ref.length, Integer.BYTES);
                        System.arraycopy(ref.bytes, ref.offset, encoded, offset, ref.length);
                        offset += ref.length;
                    }
                    case NULL -> throw new IllegalStateException("non-null value in a null-typed sequence key");
                    default -> throw new IllegalStateException("unexpected sequence key type [" + types[key] + "]");
                }
            }
            assert offset == length;
            return new EncodedKey(new BytesRef(encoded), breaker, keyBytes);
        } catch (RuntimeException | Error e) {
            breaker.addWithoutBreaking(-keyBytes);
            throw e;
        } finally {
            // The borrowed view must not extend the lifetime of a released stage page.
            clearScratch();
        }
    }

    private int byteLength(BytesRefBlock block, int value) {
        try {
            return block.getBytesRef(value, scratch).length;
        } finally {
            clearScratch();
        }
    }

    private void clearScratch() {
        scratch.bytes = BytesRef.EMPTY_BYTES;
        scratch.offset = 0;
        scratch.length = 0;
    }

    private static int writeNumber(byte[] encoded, int offset, long value, int width) {
        for (int shift = (width - 1) * Byte.SIZE; shift >= 0; shift -= Byte.SIZE) {
            encoded[offset++] = (byte) (value >>> shift);
        }
        return offset;
    }

    /**
     * Orders canonical keys with the same resolved schema by their scalar values, rather than their length prefixes
     * or signed-number byte representation. Text uses Java String ordering, as EQL's SequenceKey does; IP/version
     * keys use the mapped display strings EQL reads. The unsigned-long block representation is already shifted into
     * signed order. Borrowed BytesRef offsets are honored and no key storage is retained.
     */
    public static int compare(BytesRef left, BytesRef right) {
        int l = left.offset;
        int r = right.offset;
        int leftEnd = Math.addExact(left.offset, left.length);
        int rightEnd = Math.addExact(right.offset, right.length);
        while (l < leftEnd && r < rightEnd) {
            int tag = left.bytes[l++];
            if (tag != right.bytes[r++]) {
                throw new IllegalArgumentException("SEQUENCE key ordering requires matching types");
            }
            int comparison;
            switch (tag) {
                case 1 -> comparison = Byte.compare(left.bytes[l++], right.bytes[r++]);
                case 2 -> {
                    comparison = Integer.compare((int) readNumber(left, l, Integer.BYTES), (int) readNumber(right, r, Integer.BYTES));
                    l += Integer.BYTES;
                    r += Integer.BYTES;
                }
                case 3, 4, 9, 10 -> {
                    comparison = Long.compare(readNumber(left, l, Long.BYTES), readNumber(right, r, Long.BYTES));
                    l += Long.BYTES;
                    r += Long.BYTES;
                }
                case 5 -> {
                    comparison = Double.compare(
                        Double.longBitsToDouble(readNumber(left, l, Long.BYTES)),
                        Double.longBitsToDouble(readNumber(right, r, Long.BYTES))
                    );
                    l += Long.BYTES;
                    r += Long.BYTES;
                }
                case 6, 7, 8 -> {
                    int leftLength = Math.toIntExact(readNumber(left, l, Integer.BYTES));
                    int rightLength = Math.toIntExact(readNumber(right, r, Integer.BYTES));
                    l += Integer.BYTES;
                    r += Integer.BYTES;
                    if (leftLength < 0 || leftLength > leftEnd - l || rightLength < 0 || rightLength > rightEnd - r) {
                        throw new IllegalArgumentException("Invalid SEQUENCE key byte length");
                    }
                    comparison = keyText(tag, new BytesRef(left.bytes, l, leftLength)).compareTo(
                        keyText(tag, new BytesRef(right.bytes, r, rightLength))
                    );
                    l += leftLength;
                    r += rightLength;
                }
                default -> throw new IllegalArgumentException("Invalid SEQUENCE key type tag [" + tag + "]");
            }
            if (comparison != 0) {
                return comparison;
            }
        }
        return Integer.compare(leftEnd - l, rightEnd - r);
    }

    private static String keyText(int tag, BytesRef value) {
        return switch (tag) {
            case 6 -> value.utf8ToString();
            case 7 -> EsqlDataTypeConverter.ipToString(value);
            case 8 -> EsqlDataTypeConverter.versionToString(value);
            default -> throw new IllegalArgumentException("Invalid SEQUENCE text key type tag [" + tag + "]");
        };
    }

    private static long readNumber(BytesRef value, int offset, int width) {
        if (offset > value.offset + value.length - width) {
            throw new IllegalArgumentException("Invalid SEQUENCE numeric key length");
        }
        long result = 0;
        for (int index = 0; index < width; index++) {
            result = result << Byte.SIZE | value.bytes[offset + index] & 0xffL;
        }
        return result;
    }

    private static long arrayBytes(int length, int width) {
        return RamUsageEstimator.alignObjectSize(RamUsageEstimator.NUM_BYTES_ARRAY_HEADER + (long) length * width);
    }

    @Override
    public void close() {
        if (closed == false) {
            closed = true;
            channels = null;
            types = null;
            clearScratch();
            breaker.addWithoutBreaking(-bytes);
        }
    }

    /** A temporary canonical key with independent reservation ownership; its borrowed view is valid until close. */
    public static final class EncodedKey implements Releasable {
        private BytesRef value;
        private final CircuitBreaker breaker;
        private final long bytes;

        private EncodedKey(BytesRef value, CircuitBreaker breaker, long bytes) {
            this.value = value;
            this.breaker = breaker;
            this.bytes = bytes;
        }

        public synchronized BytesRef bytes() {
            if (value == null) {
                throw new IllegalStateException("sequence key is closed");
            }
            return value;
        }

        @Override
        public synchronized void close() {
            if (value != null) {
                value = null;
                breaker.addWithoutBreaking(-bytes);
            }
        }
    }
}
