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

package org.elasticsearch.index.refresh;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

public class RefreshStats implements Streamable, ToXContent {

    private long total;

    private long totalTimeInMillis;

    public RefreshStats() {

    }

    public RefreshStats(long total, long totalTimeInMillis) {
        this.total = total;
        this.totalTimeInMillis = totalTimeInMillis;
    }

    public void add(long total, long totalTimeInMillis) {
        this.total += total;
        this.totalTimeInMillis += totalTimeInMillis;
    }

    public void add(RefreshStats refreshStats) {
        if (refreshStats == null) {
            return;
        }
        this.total += refreshStats.total;
        this.totalTimeInMillis += refreshStats.totalTimeInMillis;
    }

    /**
     * The total number of refresh executed.
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

    public static RefreshStats readRefreshStats(StreamInput in) throws IOException {
        RefreshStats refreshStats = new RefreshStats();
        refreshStats.readFrom(in);
        return refreshStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.REFRESH);
        builder.field(Fields.TOTAL, total);
        builder.timeValueField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, totalTimeInMillis);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString REFRESH = new XContentBuilderString("refresh");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString TOTAL_TIME = new XContentBuilderString("total_time");
        static final XContentBuilderString TOTAL_TIME_IN_MILLIS = new XContentBuilderString("total_time_in_millis");
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        total = in.readVLong();
        totalTimeInMillis = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(total);
        out.writeVLong(totalTimeInMillis);
    }
}