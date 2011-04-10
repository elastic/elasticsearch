/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

    private long current;

    private long totalTimeInMillis;

    public MergeStats() {

    }

    public MergeStats(long total, long current, long totalTimeInMillis) {
        this.total = total;
        this.current = current;
        this.totalTimeInMillis = totalTimeInMillis;
    }

    public void add(long totalMerges, long currentMerges, long totalMergeTime) {
        this.total += totalMerges;
        this.current += currentMerges;
        this.totalTimeInMillis += totalMergeTime;
    }

    public void add(MergeStats mergeStats) {
        if (mergeStats == null) {
            return;
        }
        this.total += mergeStats.total;
        this.current += mergeStats.current;
        this.totalTimeInMillis += mergeStats.totalTimeInMillis;
    }

    /**
     * The total number of merges executed.
     */
    public long total() {
        return this.total;
    }

    /**
     * The current number of merges executing.
     */
    public long current() {
        return this.current;
    }

    /**
     * The total time merges have been executed (in milliseconds).
     */
    public long totalTimeInMillis() {
        return this.totalTimeInMillis;
    }

    /**
     * The total time merges have been executed.
     */
    public TimeValue totalTime() {
        return new TimeValue(totalTimeInMillis);
    }

    public static MergeStats readMergeStats(StreamInput in) throws IOException {
        MergeStats stats = new MergeStats();
        stats.readFrom(in);
        return stats;
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.MERGES);
        builder.field(Fields.CURRENT, current);
        builder.field(Fields.TOTAL, total);
        builder.field(Fields.TOTAL_TIME, totalTime().toString());
        builder.field(Fields.TOTAL_TIME_IN_MILLIS, totalTimeInMillis);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString MERGES = new XContentBuilderString("merges");
        static final XContentBuilderString CURRENT = new XContentBuilderString("current");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString TOTAL_TIME = new XContentBuilderString("total_time");
        static final XContentBuilderString TOTAL_TIME_IN_MILLIS = new XContentBuilderString("total_time_in_millis");
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        total = in.readVLong();
        current = in.readVLong();
        totalTimeInMillis = in.readVLong();
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(total);
        out.writeVLong(current);
        out.writeVLong(totalTimeInMillis);
    }
}