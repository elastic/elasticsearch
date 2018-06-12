/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class SnapshotStats implements Streamable, ToXContentFragment {

    private long startTime;
    private long time;
    private int incrementalFileCount;
    private int totalFileCount;
    private int processedFileCount;
    private long incrementalSize;
    private long totalSize;
    private long processedSize;

    SnapshotStats() {
    }

    SnapshotStats(long startTime, long time,
                  int incrementalFileCount, int totalFileCount, int processedFileCount,
                  long incrementalSize, long totalSize, long processedSize) {
        this.startTime = startTime;
        this.time = time;
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


    public static SnapshotStats readSnapshotStats(StreamInput in) throws IOException {
        SnapshotStats stats = new SnapshotStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(startTime);
        out.writeVLong(time);

        out.writeVInt(incrementalFileCount);
        out.writeVInt(processedFileCount);

        out.writeVLong(incrementalSize);
        out.writeVLong(processedSize);

        if (out.getVersion().onOrAfter(Version.V_6_4_0)) {
            out.writeVInt(totalFileCount);
            out.writeVLong(totalSize);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        startTime = in.readVLong();
        time = in.readVLong();

        incrementalFileCount = in.readVInt();
        processedFileCount = in.readVInt();

        incrementalSize = in.readVLong();
        processedSize = in.readVLong();

        if (in.getVersion().onOrAfter(Version.V_6_4_0)) {
            totalFileCount = in.readVInt();
            totalSize = in.readVLong();
        } else {
            totalFileCount = incrementalFileCount;
            totalSize = incrementalSize;
        }
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
        builder.startObject(Fields.STATS)
            //  incremental starts
            .startObject(Fields.INCREMENTAL)
            .field(Fields.FILE_COUNT, getIncrementalFileCount())
            .humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, new ByteSizeValue(getIncrementalSize()))
            //  incremental ends
            .endObject();

        if (getProcessedFileCount() != getIncrementalFileCount()) {
            //  processed starts
            builder.startObject(Fields.PROCESSED)
                .field(Fields.FILE_COUNT, getProcessedFileCount())
                .humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, new ByteSizeValue(getProcessedSize()))
                //  processed ends
                .endObject();
        }
        //  total starts
        builder.startObject(Fields.TOTAL)
            .field(Fields.FILE_COUNT, getTotalFileCount())
            .humanReadableField(Fields.SIZE_IN_BYTES, Fields.SIZE, new ByteSizeValue(getTotalSize()))
            //  total ends
            .endObject();
       // timings stats
       builder.field(Fields.START_TIME_IN_MILLIS, getStartTime())
            .humanReadableField(Fields.TIME_IN_MILLIS, Fields.TIME, new TimeValue(getTime()));

        return builder.endObject();
    }

    void add(SnapshotStats stats) {
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
        } else {
            // The time the last snapshot ends
            long endTime = Math.max(startTime + time, stats.startTime + stats.time);

            // The time the first snapshot starts
            startTime = Math.min(startTime, stats.startTime);

            // Update duration
            time = endTime - startTime;
        }
    }
}
