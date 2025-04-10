/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class StoreStats implements Writeable, ToXContentFragment {

    /**
     * Sentinel value for cases where the shard does not yet know its reserved size so we must fall back to an estimate, for instance
     * prior to receiving the list of files in a peer recovery.
     */
    public static final long UNKNOWN_RESERVED_BYTES = -1L;

    private long sizeInBytes;
    private long totalDataSetSizeInBytes;
    private long reservedSizeInBytes;

    public StoreStats() {

    }

    public StoreStats(StreamInput in) throws IOException {
        sizeInBytes = in.readVLong();
        totalDataSetSizeInBytes = in.readVLong();
        reservedSizeInBytes = in.readZLong();
    }

    /**
     * @param sizeInBytes the size of the store in bytes
     * @param totalDataSetSizeInBytes the size of the total data set in bytes, can differ from sizeInBytes for shards using shared cache
     *                                storage
     * @param reservedSize a prediction of how much larger the store is expected to grow, or {@link StoreStats#UNKNOWN_RESERVED_BYTES}.
     */
    public StoreStats(long sizeInBytes, long totalDataSetSizeInBytes, long reservedSize) {
        assert reservedSize == UNKNOWN_RESERVED_BYTES || reservedSize >= 0 : reservedSize;
        this.sizeInBytes = sizeInBytes;
        this.totalDataSetSizeInBytes = totalDataSetSizeInBytes;
        this.reservedSizeInBytes = reservedSize;
    }

    public void add(StoreStats stats) {
        if (stats == null) {
            return;
        }
        sizeInBytes += stats.sizeInBytes;
        totalDataSetSizeInBytes += stats.totalDataSetSizeInBytes;
        reservedSizeInBytes = ignoreIfUnknown(reservedSizeInBytes) + ignoreIfUnknown(stats.reservedSizeInBytes);
    }

    private static long ignoreIfUnknown(long reservedSize) {
        return reservedSize == UNKNOWN_RESERVED_BYTES ? 0L : reservedSize;
    }

    public long sizeInBytes() {
        return sizeInBytes;
    }

    public ByteSizeValue size() {
        return ByteSizeValue.ofBytes(sizeInBytes);
    }

    public long totalDataSetSizeInBytes() {
        return totalDataSetSizeInBytes;
    }

    public ByteSizeValue totalDataSetSize() {
        return ByteSizeValue.ofBytes(totalDataSetSizeInBytes);
    }

    public long reservedSizeInBytes() {
        return reservedSizeInBytes;
    }

    /**
     * A prediction of how much larger this store will eventually grow. For instance, if we are currently doing a peer recovery or restoring
     * a snapshot into this store then we can account for the rest of the recovery using this field. A value of {@code -1B} indicates that
     * the reserved size is unknown.
     */
    public ByteSizeValue getReservedSize() {
        return ByteSizeValue.ofBytes(reservedSizeInBytes);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(sizeInBytes);
        out.writeVLong(totalDataSetSizeInBytes);
        out.writeZLong(reservedSizeInBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.STORE);
        builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, size());
        builder.humanReadableField(Fields.TOTAL_DATA_SET_SIZE_IN_BYTES, Fields.TOTAL_DATA_SET_SIZE, totalDataSetSize());
        builder.humanReadableField(Fields.RESERVED_IN_BYTES, Fields.RESERVED, getReservedSize());
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoreStats that = (StoreStats) o;
        return sizeInBytes == that.sizeInBytes
            && totalDataSetSizeInBytes == that.totalDataSetSizeInBytes
            && reservedSizeInBytes == that.reservedSizeInBytes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(sizeInBytes, totalDataSetSizeInBytes, reservedSizeInBytes);
    }

    static final class Fields {
        static final String STORE = "store";
        static final String SIZE = "size";
        static final String SIZE_IN_BYTES = "size_in_bytes";
        static final String TOTAL_DATA_SET_SIZE = "total_data_set_size";
        static final String TOTAL_DATA_SET_SIZE_IN_BYTES = "total_data_set_size_in_bytes";
        static final String RESERVED = "reserved";
        static final String RESERVED_IN_BYTES = "reserved_in_bytes";
    }
}
