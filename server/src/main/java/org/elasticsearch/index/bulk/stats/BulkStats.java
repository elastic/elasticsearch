/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.bulk.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

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

    public BulkStats() {

    }

    public BulkStats(StreamInput in) throws IOException {
        totalOperations = in.readVLong();
        totalTimeInMillis = in.readVLong();
        totalSizeInBytes = in.readVLong();
        avgTimeInMillis = in.readVLong();
        avgSizeInBytes = in.readVLong();
    }

    public BulkStats(long totalOperations, long totalTimeInMillis, long totalSizeInBytes, long avgTimeInMillis, long avgSizeInBytes) {
        this.totalOperations = totalOperations;
        this.totalTimeInMillis = totalTimeInMillis;
        this.totalSizeInBytes = totalSizeInBytes;
        this.avgTimeInMillis = avgTimeInMillis;
        this.avgSizeInBytes = avgSizeInBytes;
    }

    public void add(BulkStats bulkStats) {
        addTotals(bulkStats);
    }

    public void addTotals(BulkStats bulkStats) {
        if (bulkStats == null) {
            return;
        }
        if (this.totalOperations > 0 || bulkStats.totalOperations > 0) {
            this.avgTimeInMillis =
                (avgTimeInMillis * totalOperations + bulkStats.avgTimeInMillis * bulkStats.totalOperations) / (totalOperations
                    + bulkStats.totalOperations);
            this.avgSizeInBytes =
                (avgSizeInBytes * totalOperations + bulkStats.avgSizeInBytes * bulkStats.totalOperations) / (totalOperations
                    + bulkStats.totalOperations);
        }
        this.totalOperations += bulkStats.totalOperations;
        this.totalTimeInMillis += bulkStats.totalTimeInMillis;
        this.totalSizeInBytes += bulkStats.totalSizeInBytes;
    }

    public long getTotalSizeInBytes() {
        return totalSizeInBytes;
    }

    public long getTotalOperations() {
        return totalOperations;
    }

    public TimeValue getTotalTime() {
        return new TimeValue(totalTimeInMillis);
    }

    public TimeValue getAvgTime() {
        return new TimeValue(avgTimeInMillis);
    }

    public long getTotalTimeInMillis() {
        return totalTimeInMillis;
    }

    public long getAvgTimeInMillis() {
        return avgTimeInMillis;
    }

    public long getAvgSizeInBytes() {
        return avgSizeInBytes;
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalOperations);
        out.writeVLong(totalTimeInMillis);
        out.writeVLong(totalSizeInBytes);
        out.writeVLong(avgTimeInMillis);
        out.writeVLong(avgSizeInBytes);
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
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
        return Objects.equals(this.totalOperations, that.totalOperations) && Objects.equals(this.totalTimeInMillis, that.totalTimeInMillis)
            && Objects.equals(this.totalSizeInBytes, that.totalSizeInBytes) && Objects.equals(this.avgTimeInMillis, that.avgTimeInMillis)
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

