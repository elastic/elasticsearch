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
    private int differenceOfNumberOfFiles;
    private int totalNumberOfFiles;
    private int processedFiles;
    private long differenceOfSize;
    private long totalSize;
    private long processedSize;

    SnapshotStats() {
    }

    SnapshotStats(long startTime, long time,
                  int differenceOfNumberOfFiles, int totalNumberOfFiles, int processedFiles,
                  long differenceOfSize, long totalSize, long processedSize) {
        this.startTime = startTime;
        this.time = time;
        this.differenceOfNumberOfFiles = differenceOfNumberOfFiles;
        this.totalNumberOfFiles = totalNumberOfFiles;
        this.processedFiles = processedFiles;
        this.differenceOfSize = differenceOfSize;
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
     * Returns difference of number of files between previous and this snapshot
     */
    public int getDifferenceOfNumberOfFiles() {
        return differenceOfNumberOfFiles;
    }

    /**
     * Returns total number of files in the snapshot
     */
    public int getTotalNumberOfFiles() {
        return totalNumberOfFiles;
    }

    /**
     * Returns number of files in the snapshot that were processed so far
     */
    public int getProcessedFiles() {
        return processedFiles;
    }

    /**
     * Return difference size of files between previous and this snapshot
     */
    public long getDifferenceOfSize() {
        return differenceOfSize;
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

        out.writeVInt(totalNumberOfFiles);
        out.writeVInt(processedFiles);

        out.writeVLong(totalSize);
        out.writeVLong(processedSize);

        if (out.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            out.writeVInt(differenceOfNumberOfFiles);
            out.writeVLong(differenceOfSize);
        }
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        startTime = in.readVLong();
        time = in.readVLong();

        totalNumberOfFiles = in.readVInt();
        processedFiles = in.readVInt();

        totalSize = in.readVLong();
        processedSize = in.readVLong();

        if (in.getVersion().onOrAfter(Version.V_7_0_0_alpha1)) {
            differenceOfNumberOfFiles = in.readVInt();
            differenceOfSize = in.readVLong();
        } else {
            differenceOfNumberOfFiles = totalNumberOfFiles;
            differenceOfSize = totalSize;
        }
    }

    static final class Fields {
        static final String STATS = "stats";
        static final String DIFFERENCE_OF_NUMBER_OF_FILES = "difference_of_number_of_files";
        static final String TOTAL_NUMBER_OF_FILES = "total_number_of_files";
        static final String PROCESSED_FILES = "processed_files";
        static final String DIFFERENCE_OF_SIZE_IN_BYTES = "difference_of_size_in_bytes";
        static final String DIFFERENCE_OF_SIZE = "difference_of_size";
        static final String TOTAL_SIZE_IN_BYTES = "total_size_in_bytes";
        static final String TOTAL_SIZE = "total_size";
        static final String PROCESSED_SIZE_IN_BYTES = "processed_size_in_bytes";
        static final String PROCESSED_SIZE = "processed_size";
        static final String START_TIME_IN_MILLIS = "start_time_in_millis";
        static final String TIME_IN_MILLIS = "time_in_millis";
        static final String TIME = "time";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.STATS);
        builder.field(Fields.DIFFERENCE_OF_NUMBER_OF_FILES, getDifferenceOfNumberOfFiles());
        builder.field(Fields.TOTAL_NUMBER_OF_FILES, getTotalNumberOfFiles());
        builder.field(Fields.PROCESSED_FILES, getProcessedFiles());
        builder.humanReadableField(Fields.DIFFERENCE_OF_SIZE_IN_BYTES, Fields.DIFFERENCE_OF_SIZE, new ByteSizeValue(getDifferenceOfSize()));
        builder.humanReadableField(Fields.TOTAL_SIZE_IN_BYTES, Fields.TOTAL_SIZE, new ByteSizeValue(getTotalSize()));
        builder.humanReadableField(Fields.PROCESSED_SIZE_IN_BYTES, Fields.PROCESSED_SIZE, new ByteSizeValue(getProcessedSize()));
        builder.field(Fields.START_TIME_IN_MILLIS, getStartTime());
        builder.humanReadableField(Fields.TIME_IN_MILLIS, Fields.TIME, new TimeValue(getTime()));
        builder.endObject();
        return builder;
    }

    void add(SnapshotStats stats) {
        differenceOfNumberOfFiles += stats.differenceOfNumberOfFiles;
        totalNumberOfFiles += stats.totalNumberOfFiles;
        processedFiles += stats.processedFiles;

        differenceOfSize += stats.differenceOfSize;
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
