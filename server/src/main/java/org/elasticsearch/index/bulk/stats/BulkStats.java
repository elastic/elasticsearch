/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.bulk.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Bulk related statistics, including the time and size of shard bulk requests,
 * starting at the shard level and allowing aggregation to indices and node level
 */
public class BulkStats implements Writeable, ToXContentFragment {

    private long totalOperations = 0;
    private long totalTimeInMillis = 0;
    private long totalSizeInBytes = 0;
    private long avgTimeInMillis = 0;
    private long avgSizeInBytes = 0;

    /**
     * Constructs a new BulkStats instance with all statistics initialized to zero.
     */
    public BulkStats() {

    }

    /**
     * Deserializes a BulkStats instance from a stream input.
     *
     * @param in the stream to read from
     * @throws IOException if an I/O error occurs while reading from the stream
     */
    public BulkStats(StreamInput in) throws IOException {
        totalOperations = in.readVLong();
        totalTimeInMillis = in.readVLong();
        totalSizeInBytes = in.readVLong();
        avgTimeInMillis = in.readVLong();
        avgSizeInBytes = in.readVLong();
    }

    /**
     * Constructs a BulkStats instance with specified values.
     *
     * @param totalOperations the total number of bulk operations
     * @param totalTimeInMillis the total time spent on bulk operations in milliseconds
     * @param totalSizeInBytes the total size of bulk operations in bytes
     * @param avgTimeInMillis the average time per bulk operation in milliseconds
     * @param avgSizeInBytes the average size per bulk operation in bytes
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BulkStats stats = new BulkStats(100, 5000, 1024000, 50, 10240);
     * }</pre>
     */
    public BulkStats(long totalOperations, long totalTimeInMillis, long totalSizeInBytes, long avgTimeInMillis, long avgSizeInBytes) {
        this.totalOperations = totalOperations;
        this.totalTimeInMillis = totalTimeInMillis;
        this.totalSizeInBytes = totalSizeInBytes;
        this.avgTimeInMillis = avgTimeInMillis;
        this.avgSizeInBytes = avgSizeInBytes;
    }

    /**
     * Adds the statistics from another BulkStats instance to this one.
     *
     * @param bulkStats the BulkStats to add
     *
     * <p><b>Usage Examples:</b></p>
     * <pre>{@code
     * BulkStats stats1 = new BulkStats(100, 5000, 1024000, 50, 10240);
     * BulkStats stats2 = new BulkStats(50, 2500, 512000, 50, 10240);
     * stats1.add(stats2); // Combines the statistics
     * }</pre>
     */
    public void add(BulkStats bulkStats) {
        addTotals(bulkStats);
    }

    /**
     * Adds the total statistics from another BulkStats instance to this one.
     * Recalculates averages based on the combined totals.
     *
     * @param bulkStats the BulkStats to add, or null (which is ignored)
     */
    public void addTotals(BulkStats bulkStats) {
        if (bulkStats == null) {
            return;
        }
        if (this.totalOperations > 0 || bulkStats.totalOperations > 0) {
            this.avgTimeInMillis = (avgTimeInMillis * totalOperations + bulkStats.avgTimeInMillis * bulkStats.totalOperations)
                / (totalOperations + bulkStats.totalOperations);
            this.avgSizeInBytes = (avgSizeInBytes * totalOperations + bulkStats.avgSizeInBytes * bulkStats.totalOperations)
                / (totalOperations + bulkStats.totalOperations);
        }
        this.totalOperations += bulkStats.totalOperations;
        this.totalTimeInMillis += bulkStats.totalTimeInMillis;
        this.totalSizeInBytes += bulkStats.totalSizeInBytes;
    }

    /**
     * Retrieves the total size of all bulk operations in bytes.
     *
     * @return the total size in bytes
     */
    public long getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    /**
     * Retrieves the total number of bulk operations.
     *
     * @return the total operation count
     */
    public long getTotalOperations() {
        return totalOperations;
    }

    /**
     * Retrieves the total time spent on bulk operations as a TimeValue.
     *
     * @return the total time as a TimeValue
     */
    public TimeValue getTotalTime() {
        return new TimeValue(totalTimeInMillis);
    }

    /**
     * Retrieves the average time per bulk operation as a TimeValue.
     *
     * @return the average time as a TimeValue
     */
    public TimeValue getAvgTime() {
        return new TimeValue(avgTimeInMillis);
    }

    /**
     * Retrieves the total time spent on bulk operations in milliseconds.
     *
     * @return the total time in milliseconds
     */
    public long getTotalTimeInMillis() {
        return totalTimeInMillis;
    }

    /**
     * Retrieves the average time per bulk operation in milliseconds.
     *
     * @return the average time in milliseconds
     */
    public long getAvgTimeInMillis() {
        return avgTimeInMillis;
    }

    /**
     * Retrieves the average size per bulk operation in bytes.
     *
     * @return the average size in bytes
     */
    public long getAvgSizeInBytes() {
        return avgSizeInBytes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalOperations);
        out.writeVLong(totalTimeInMillis);
        out.writeVLong(totalSizeInBytes);
        out.writeVLong(avgTimeInMillis);
        out.writeVLong(avgSizeInBytes);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.BULK);
        builder.field(Fields.TOTAL_OPERATIONS, totalOperations);
        builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, getTotalTime());
        builder.field(Fields.TOTAL_SIZE_IN_BYTES, totalSizeInBytes);
        builder.humanReadableField(Fields.AVG_TIME_IN_MILLIS, Fields.AVG_TIME, getAvgTime());
        builder.field(Fields.AVG_SIZE_IN_BYTES, avgSizeInBytes);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final BulkStats that = (BulkStats) o;
        return Objects.equals(this.totalOperations, that.totalOperations)
            && Objects.equals(this.totalTimeInMillis, that.totalTimeInMillis)
            && Objects.equals(this.totalSizeInBytes, that.totalSizeInBytes)
            && Objects.equals(this.avgTimeInMillis, that.avgTimeInMillis)
            && Objects.equals(this.avgSizeInBytes, that.avgSizeInBytes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalOperations, totalTimeInMillis, totalSizeInBytes, avgTimeInMillis, avgSizeInBytes);
    }

    static final class Fields {
        static final String BULK = "bulk";
        static final String TOTAL_OPERATIONS = "total_operations";
        static final String TOTAL_TIME = "total_time";
        static final String AVG_TIME = "avg_time";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
        static final String TOTAL_SIZE_IN_BYTES = "total_size_in_bytes";
        static final String AVG_TIME_IN_MILLIS = "avg_time_in_millis";
        static final String AVG_SIZE_IN_BYTES = "avg_size_in_bytes";
    }
}
