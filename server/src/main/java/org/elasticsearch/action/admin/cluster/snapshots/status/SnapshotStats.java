/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public class SnapshotStats implements Writeable, ToXContentObject {

    private long startTime;
    private long time;
    private int incrementalFileCount;
    private int totalFileCount;
    private int processedFileCount;
    private long incrementalSize;
    private long totalSize;
    private long processedSize;

    SnapshotStats() {}

    SnapshotStats(StreamInput in) throws IOException {
        startTime = in.readVLong();
        time = in.readVLong();

        incrementalFileCount = in.readVInt();
        processedFileCount = in.readVInt();

        incrementalSize = in.readVLong();
        processedSize = in.readVLong();

        totalFileCount = in.readVInt();
        totalSize = in.readVLong();
    }

    SnapshotStats(
        long startTime,
        long time,
        int incrementalFileCount,
        int totalFileCount,
        int processedFileCount,
        long incrementalSize,
        long totalSize,
        long processedSize
    ) {
        this.startTime = startTime;
        this.time = time;
        assert time >= 0 : "Tried to initialize snapshot stats with negative total time [" + time + "]";
        this.incrementalFileCount = incrementalFileCount;
        this.totalFileCount = totalFileCount;
        this.processedFileCount = processedFileCount;
        this.incrementalSize = incrementalSize;
        this.totalSize = totalSize;
        this.processedSize = processedSize;
    }

    /**
     * Returns time when snapshot started
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Returns snapshot running time
     */
    public long getTime() {
        return time;
    }

    /**
     * Returns incremental file count of the snapshot
     */
    public int getIncrementalFileCount() {
        return incrementalFileCount;
    }

    /**
     * Returns total number of files in the snapshot
     */
    public int getTotalFileCount() {
        return totalFileCount;
    }

    /**
     * Returns number of files in the snapshot that were processed so far
     */
    public int getProcessedFileCount() {
        return processedFileCount;
    }

    /**
     * Return incremental files size of the snapshot
     */
    public long getIncrementalSize() {
        return incrementalSize;
    }

    /**
     * Returns total size of files in the snapshot
     */
    public long getTotalSize() {
        return totalSize;
    }

    /**
     * Returns total size of files in the snapshot that were processed so far
     */
    public long getProcessedSize() {
        return processedSize;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(startTime);
        out.writeVLong(time);

        out.writeVInt(incrementalFileCount);
        out.writeVInt(processedFileCount);

        out.writeVLong(incrementalSize);
        out.writeVLong(processedSize);

        out.writeVInt(totalFileCount);
        out.writeVLong(totalSize);
    }

    static final class Fields {
        static final String STATS = "stats";

        static final String INCREMENTAL = "incremental";
        static final String PROCESSED = "processed";
        static final String TOTAL = "total";

        static final String FILE_COUNT = "file_count";
        static final String SIZE = "size";
        static final String SIZE_IN_BYTES = "size_in_bytes";

        static final String START_TIME_IN_MILLIS = "start_time_in_millis";
        static final String TIME_IN_MILLIS = "time_in_millis";
        static final String TIME = "time";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        {
            builder.startObject(Fields.INCREMENTAL);
            {
                builder.field(Fields.FILE_COUNT, getIncrementalFileCount());
                builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, ByteSizeValue.ofBytes(getIncrementalSize()));
            }
            builder.endObject();

            if (getProcessedFileCount() != getIncrementalFileCount()) {
                builder.startObject(Fields.PROCESSED);
                {
                    builder.field(Fields.FILE_COUNT, getProcessedFileCount());
                    builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, ByteSizeValue.ofBytes(getProcessedSize()));
                }
                builder.endObject();
            }

            builder.startObject(Fields.TOTAL);
            {
                builder.field(Fields.FILE_COUNT, getTotalFileCount());
                builder.humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, ByteSizeValue.ofBytes(getTotalSize()));
            }
            builder.endObject();

            // timings stats
            builder.field(Fields.START_TIME_IN_MILLIS, getStartTime());
            builder.humanReadableField(Fields.TIME_IN_MILLIS, Fields.TIME, new TimeValue(getTime()));
        }
        return builder.endObject();
    }

    /**
     * Add stats instance to the total
     * @param stats Stats instance to add
     * @param updateTimestamps Whether or not start time and duration should be updated
     */
    void add(SnapshotStats stats, boolean updateTimestamps) {
        incrementalFileCount += stats.incrementalFileCount;
        totalFileCount += stats.totalFileCount;
        processedFileCount += stats.processedFileCount;

        incrementalSize += stats.incrementalSize;
        totalSize += stats.totalSize;
        processedSize += stats.processedSize;

        if (startTime == 0) {
            // First time here
            startTime = stats.startTime;
            time = stats.time;
        } else if (updateTimestamps) {
            // The time the last snapshot ends
            long endTime = Math.max(startTime + time, stats.startTime + stats.time);

            // The time the first snapshot starts
            startTime = Math.min(startTime, stats.startTime);

            // Update duration
            time = endTime - startTime;
        }
        assert time >= 0
            : "Update with [" + Strings.toString(stats) + "][" + updateTimestamps + "] resulted in negative total time [" + time + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SnapshotStats that = (SnapshotStats) o;

        if (startTime != that.startTime) return false;
        if (time != that.time) return false;
        if (incrementalFileCount != that.incrementalFileCount) return false;
        if (totalFileCount != that.totalFileCount) return false;
        if (processedFileCount != that.processedFileCount) return false;
        if (incrementalSize != that.incrementalSize) return false;
        if (totalSize != that.totalSize) return false;
        return processedSize == that.processedSize;
    }

    @Override
    public int hashCode() {
        int result = (int) (startTime ^ (startTime >>> 32));
        result = 31 * result + (int) (time ^ (time >>> 32));
        result = 31 * result + incrementalFileCount;
        result = 31 * result + totalFileCount;
        result = 31 * result + processedFileCount;
        result = 31 * result + (int) (incrementalSize ^ (incrementalSize >>> 32));
        result = 31 * result + (int) (totalSize ^ (totalSize >>> 32));
        result = 31 * result + (int) (processedSize ^ (processedSize >>> 32));
        return result;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }
}
