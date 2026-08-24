/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.bitmapterms;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** A shard or reduced result containing a portable serialized Roaring bitmap. */
public final class InternalRoaringBitmap extends InternalAggregation {

    enum BitmapFormat {
        UNMAPPED((byte) 0),
        INT((byte) 1),
        LONG((byte) 2);

        private final byte id;

        BitmapFormat(byte id) {
            this.id = id;
        }

        static BitmapFormat read(byte id) throws IOException {
            return switch (id) {
                case 0 -> UNMAPPED;
                case 1 -> INT;
                case 2 -> LONG;
                default -> throw new IOException("unknown roaring bitmap width [" + id + "]");
            };
        }
    }

    interface MutableBitmap {
        void add(long value);

        void or(MutableBitmap other);

        void optimize();

        byte[] serialize() throws IOException;

        long ramBytesUsed();

        BitmapFormat width();
    }

    private final BitmapFormat width;
    private final byte[] bitmap;

    InternalRoaringBitmap(String name, BitmapFormat width, byte[] bitmap, Map<String, Object> metadata) {
        super(name, metadata);
        this.width = Objects.requireNonNull(width);
        this.bitmap = Objects.requireNonNull(bitmap);
    }

    public InternalRoaringBitmap(StreamInput in) throws IOException {
        super(in);
        width = BitmapFormat.read(in.readByte());
        bitmap = in.readByteArray();
    }

    static InternalRoaringBitmap unmapped(String name, Map<String, Object> metadata) {
        return new InternalRoaringBitmap(name, BitmapFormat.UNMAPPED, new byte[0], metadata);
    }

    static InternalRoaringBitmap empty(String name, BitmapFormat width, Map<String, Object> metadata) {
        try {
            return new InternalRoaringBitmap(name, width, mutable(width).serialize(), metadata);
        } catch (IOException e) {
            throw new IllegalStateException("failed to serialize an empty Roaring bitmap", e);
        }
    }

    static MutableBitmap mutable(BitmapFormat width) {
        return switch (width) {
            case INT -> new IntMutableBitmap(new RoaringBitmap());
            case LONG -> new LongMutableBitmap(new Roaring64NavigableMap());
            case UNMAPPED -> throw new IllegalArgumentException("an unmapped aggregation has no bitmap width");
        };
    }

    private static MutableBitmap deserialize(BitmapFormat width, byte[] bytes) throws IOException {
        return switch (width) {
            case INT -> {
                RoaringBitmap bitmap = new RoaringBitmap();
                bitmap.deserialize(ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN));
                yield new IntMutableBitmap(bitmap);
            }
            case LONG -> {
                Roaring64NavigableMap bitmap = new Roaring64NavigableMap();
                bitmap.deserializePortable(new java.io.DataInputStream(new java.io.ByteArrayInputStream(bytes)));
                yield new LongMutableBitmap(bitmap);
            }
            case UNMAPPED -> throw new IllegalArgumentException("an unmapped aggregation has no serialized bitmap");
        };
    }

    byte[] bitmap() {
        return bitmap;
    }

    BitmapFormat width() {
        return width;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeByte(width.id);
        out.writeByteArray(bitmap);
    }

    @Override
    public String getWriteableName() {
        return RoaringBitmapAggregationBuilder.NAME;
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            private MutableBitmap reduced;
            private long breakerBytes;

            @Override
            public void accept(InternalAggregation aggregation) {
                InternalRoaringBitmap next = (InternalRoaringBitmap) aggregation;
                if (next.width == BitmapFormat.UNMAPPED) {
                    return;
                }
                try {
                    if (reduced == null) {
                        reduced = deserialize(next.width, next.bitmap);
                        adjustBreaker(reduced.ramBytesUsed());
                    } else {
                        if (reduced.width() != next.width) {
                            throw new IllegalArgumentException(
                                "[roaring_bitmap] aggregation cannot reduce [integer] and [long] field results together"
                            );
                        }
                        MutableBitmap decoded = deserialize(next.width, next.bitmap);
                        long decodedBytes = decoded.ramBytesUsed();
                        adjustBreaker(decodedBytes);
                        try {
                            long before = reduced.ramBytesUsed();
                            reduced.or(decoded);
                            adjustBreaker(reduced.ramBytesUsed() - before);
                        } finally {
                            adjustBreaker(-decodedBytes);
                        }
                    }
                } catch (IOException e) {
                    throw new IllegalArgumentException("failed to deserialize [roaring_bitmap] aggregation result", e);
                }
            }

            private void adjustBreaker(long bytes) {
                CircuitBreaker breaker = reduceContext.bigArrays().breakerService() == null
                    ? null
                    : reduceContext.bigArrays().breakerService().getBreaker(CircuitBreaker.REQUEST);
                if (breaker != null && bytes != 0) {
                    if (bytes > 0) {
                        breaker.addEstimateBytesAndMaybeBreak(bytes, "roaring_bitmap reduce");
                    } else {
                        breaker.addWithoutBreaking(bytes);
                    }
                    breakerBytes += bytes;
                }
            }

            @Override
            public InternalAggregation get() {
                if (reduced == null) {
                    return unmapped(name, getMetadata());
                }
                long before = reduced.ramBytesUsed();
                reduced.optimize();
                adjustBreaker(reduced.ramBytesUsed() - before);
                long serializationBytes = 2L * reduced.ramBytesUsed();
                adjustBreaker(serializationBytes);
                try {
                    return new InternalRoaringBitmap(name, reduced.width(), reduced.serialize(), getMetadata());
                } catch (IOException e) {
                    throw new IllegalStateException("failed to serialize reduced [roaring_bitmap] aggregation", e);
                } finally {
                    adjustBreaker(-serializationBytes);
                }
            }

            @Override
            public void close() {
                if (breakerBytes != 0) {
                    CircuitBreaker breaker = reduceContext.bigArrays().breakerService() == null
                        ? null
                        : reduceContext.bigArrays().breakerService().getBreaker(CircuitBreaker.REQUEST);
                    if (breaker != null) {
                        breaker.addWithoutBreaking(-breakerBytes);
                    }
                    breakerBytes = 0;
                }
            }
        };
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        if (width == BitmapFormat.UNMAPPED) {
            return builder.nullField(CommonFields.VALUE.getPreferredName());
        }
        return builder.field(CommonFields.VALUE.getPreferredName(), bitmap);
    }

    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return this;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        }
        if (path.size() == 1 && CommonFields.VALUE.getPreferredName().equals(path.get(0))) {
            return width == BitmapFormat.UNMAPPED ? null : bitmap;
        }
        throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass() || super.equals(object) == false) {
            return false;
        }
        InternalRoaringBitmap that = (InternalRoaringBitmap) object;
        return width == that.width && Arrays.equals(bitmap, that.bitmap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), width, Arrays.hashCode(bitmap));
    }

    private static final class IntMutableBitmap implements MutableBitmap {
        private final RoaringBitmap bitmap;

        private IntMutableBitmap(RoaringBitmap bitmap) {
            this.bitmap = bitmap;
        }

        @Override
        public void add(long value) {
            if (value > Integer.MAX_VALUE) {
                throw new IllegalArgumentException("[roaring_bitmap] integer field produced out-of-range value [" + value + "]");
            }
            bitmap.add((int) value);
        }

        @Override
        public void or(MutableBitmap other) {
            bitmap.or(((IntMutableBitmap) other).bitmap);
        }

        @Override
        public void optimize() {
            bitmap.runOptimize();
        }

        @Override
        public byte[] serialize() throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream(bitmap.serializedSizeInBytes());
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                bitmap.serialize(out);
            }
            return bytes.toByteArray();
        }

        @Override
        public long ramBytesUsed() {
            return bitmap.getLongSizeInBytes();
        }

        @Override
        public BitmapFormat width() {
            return BitmapFormat.INT;
        }
    }

    private static final class LongMutableBitmap implements MutableBitmap {
        private final Roaring64NavigableMap bitmap;

        private LongMutableBitmap(Roaring64NavigableMap bitmap) {
            this.bitmap = bitmap;
        }

        @Override
        public void add(long value) {
            bitmap.addLong(value);
        }

        @Override
        public void or(MutableBitmap other) {
            bitmap.or(((LongMutableBitmap) other).bitmap);
        }

        @Override
        public void optimize() {
            bitmap.runOptimize();
        }

        @Override
        public byte[] serialize() throws IOException {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            try (DataOutputStream out = new DataOutputStream(bytes)) {
                bitmap.serializePortable(out);
            }
            return bytes.toByteArray();
        }

        @Override
        public long ramBytesUsed() {
            return bitmap.getLongSizeInBytes();
        }

        @Override
        public BitmapFormat width() {
            return BitmapFormat.LONG;
        }
    }
}
