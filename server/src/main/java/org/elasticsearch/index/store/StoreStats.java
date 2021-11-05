/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.store;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class StoreStats implements Writeable, ToXContentFragment {

    /**
     * Sentinel value for cases where the shard does not yet know its reserved size so we must fall back to an estimate, for instance
     * prior to receiving the list of files in a peer recovery.
     */
    public static final long UNKNOWN_RESERVED_BYTES = -1L;

    public static final Version RESERVED_BYTES_VERSION = Version.V_7_9_0;
    public static final Version TOTAL_DATA_SET_SIZE_SIZE_VERSION = Version.V_7_13_0;

    private long sizeInBytes;
    private long totalDataSetSizeInBytes;
    private long reservedSize;

    public StoreStats() {

    }

    public StoreStats(StreamInput in) throws IOException {
        sizeInBytes = in.readVLong();
        if (in.getVersion().onOrAfter(TOTAL_DATA_SET_SIZE_SIZE_VERSION)) {
            totalDataSetSizeInBytes = in.readVLong();
        } else {
            totalDataSetSizeInBytes = sizeInBytes;
        }
        if (in.getVersion().onOrAfter(RESERVED_BYTES_VERSION)) {
            reservedSize = in.readZLong();
        } else {
            reservedSize = UNKNOWN_RESERVED_BYTES;
        }
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
        this.reservedSize = reservedSize;
    }

    public void add(StoreStats stats) {
        if (stats == null) {
            return;
        }
        sizeInBytes += stats.sizeInBytes;
        totalDataSetSizeInBytes += stats.totalDataSetSizeInBytes;
        reservedSize = ignoreIfUnknown(reservedSize) + ignoreIfUnknown(stats.reservedSize);
    }

    private static long ignoreIfUnknown(long reservedSize) {
        return reservedSize == UNKNOWN_RESERVED_BYTES ? 0L : reservedSize;
    }

    public long sizeInBytes() {
        return sizeInBytes;
    }

    public long getSizeInBytes() {
        return sizeInBytes;
    }

    public ByteSizeValue size() {
        return new ByteSizeValue(sizeInBytes);
    }

    public ByteSizeValue getSize() {
        return size();
    }

    public ByteSizeValue totalDataSetSize() {
        return new ByteSizeValue(totalDataSetSizeInBytes);
    }

    public ByteSizeValue getTotalDataSetSize() {
        return totalDataSetSize();
    }

    public long totalDataSetSizeInBytes() {
        return totalDataSetSizeInBytes;
    }

    /**
     * A prediction of how much larger this store will eventually grow. For instance, if we are currently doing a peer recovery or restoring
     * a snapshot into this store then we can account for the rest of the recovery using this field. A value of {@code -1B} indicates that
     * the reserved size is unknown.
     */
    public ByteSizeValue getReservedSize() {
        return new ByteSizeValue(reservedSize);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(sizeInBytes);
        if (out.getVersion().onOrAfter(TOTAL_DATA_SET_SIZE_SIZE_VERSION)) {
            out.writeVLong(totalDataSetSizeInBytes);
        }
        if (out.getVersion().onOrAfter(RESERVED_BYTES_VERSION)) {
            out.writeZLong(reservedSize);
        }
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
