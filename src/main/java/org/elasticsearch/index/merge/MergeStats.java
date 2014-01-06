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

package org.elasticsearch.index.merge;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 *
 */
public class MergeStats implements Streamable, ToXContent {

    private long total;
    private long totalTimeInMillis;
    private long totalNumDocs;
    private long totalSizeInBytes;
    private long current;
    private long currentNumDocs;
    private long currentSizeInBytes;

    public MergeStats() {

    }

    public void add(long totalMerges, long totalMergeTime, long totalNumDocs, long totalSizeInBytes, long currentMerges, long currentNumDocs, long currentSizeInBytes) {
        this.total += totalMerges;
        this.totalTimeInMillis += totalMergeTime;
        this.totalNumDocs += totalNumDocs;
        this.totalSizeInBytes += totalSizeInBytes;
        this.current += currentMerges;
        this.currentNumDocs += currentNumDocs;
        this.currentSizeInBytes += currentSizeInBytes;
    }

    public void add(MergeStats mergeStats) {
        if (mergeStats == null) {
            return;
        }
        this.total += mergeStats.total;
        this.totalTimeInMillis += mergeStats.totalTimeInMillis;
        this.totalNumDocs += mergeStats.totalNumDocs;
        this.totalSizeInBytes += mergeStats.totalSizeInBytes;
        this.current += mergeStats.current;
        this.currentNumDocs += mergeStats.currentNumDocs;
        this.currentSizeInBytes += mergeStats.currentSizeInBytes;
    }

    /**
     * The total number of merges executed.
     */
    public long getTotal() {
        return this.total;
    }

    /**
     * The total time merges have been executed (in milliseconds).
     */
    public long getTotalTimeInMillis() {
        return this.totalTimeInMillis;
    }

    /**
     * The total time merges have been executed.
     */
    public TimeValue getTotalTime() {
        return new TimeValue(totalTimeInMillis);
    }

    public long getTotalNumDocs() {
        return this.totalNumDocs;
    }

    public long getTotalSizeInBytes() {
        return this.totalSizeInBytes;
    }

    public ByteSizeValue getTotalSize() {
        return new ByteSizeValue(totalSizeInBytes);
    }

    /**
     * The current number of merges executing.
     */
    public long getCurrent() {
        return this.current;
    }

    public long getCurrentNumDocs() {
        return this.currentNumDocs;
    }

    public long getCurrentSizeInBytes() {
        return this.currentSizeInBytes;
    }

    public ByteSizeValue getCurrentSize() {
        return new ByteSizeValue(currentSizeInBytes);
    }

    public static MergeStats readMergeStats(StreamInput in) throws IOException {
        MergeStats stats = new MergeStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.MERGES);
        builder.field(Fields.CURRENT, current);
        builder.field(Fields.CURRENT_DOCS, currentNumDocs);
        builder.byteSizeField(Fields.CURRENT_SIZE_IN_BYTES, Fields.CURRENT_SIZE, currentSizeInBytes);
        builder.field(Fields.TOTAL, total);
        builder.timeValueField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, totalTimeInMillis);
        builder.field(Fields.TOTAL_DOCS, totalNumDocs);
        builder.byteSizeField(Fields.TOTAL_SIZE_IN_BYTES, Fields.TOTAL_SIZE, totalSizeInBytes);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString MERGES = new XContentBuilderString("merges");
        static final XContentBuilderString CURRENT = new XContentBuilderString("current");
        static final XContentBuilderString CURRENT_DOCS = new XContentBuilderString("current_docs");
        static final XContentBuilderString CURRENT_SIZE = new XContentBuilderString("current_size");
        static final XContentBuilderString CURRENT_SIZE_IN_BYTES = new XContentBuilderString("current_size_in_bytes");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString TOTAL_TIME = new XContentBuilderString("total_time");
        static final XContentBuilderString TOTAL_TIME_IN_MILLIS = new XContentBuilderString("total_time_in_millis");
        static final XContentBuilderString TOTAL_DOCS = new XContentBuilderString("total_docs");
        static final XContentBuilderString TOTAL_SIZE = new XContentBuilderString("total_size");
        static final XContentBuilderString TOTAL_SIZE_IN_BYTES = new XContentBuilderString("total_size_in_bytes");
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        total = in.readVLong();
        totalTimeInMillis = in.readVLong();
        totalNumDocs = in.readVLong();
        totalSizeInBytes = in.readVLong();
        current = in.readVLong();
        currentNumDocs = in.readVLong();
        currentSizeInBytes = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(total);
        out.writeVLong(totalTimeInMillis);
        out.writeVLong(totalNumDocs);
        out.writeVLong(totalSizeInBytes);
        out.writeVLong(current);
        out.writeVLong(currentNumDocs);
        out.writeVLong(currentSizeInBytes);
    }
}