/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.index.get;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 */
public class GetStats implements Streamable, ToXContent {

    private long existsCount;
    private long existsTimeInMillis;
    private long missingCount;
    private long missingTimeInMillis;
    private long current;

    public GetStats() {
    }

    public GetStats(long existsCount, long existsTimeInMillis, long missingCount, long missingTimeInMillis, long current) {
        this.existsCount = existsCount;
        this.existsTimeInMillis = existsTimeInMillis;
        this.missingCount = missingCount;
        this.missingTimeInMillis = missingTimeInMillis;
        this.current = current;
    }

    public void add(GetStats stats) {
        if (stats == null) {
            return;
        }
        existsCount += stats.existsCount;
        existsTimeInMillis += stats.existsTimeInMillis;
        missingCount += stats.missingCount;
        missingTimeInMillis += stats.missingTimeInMillis;
        current += stats.current;
    }

    public long count() {
        return existsCount + missingCount;
    }

    public long getCount() {
        return count();
    }

    public long timeInMillis() {
        return existsTimeInMillis + missingTimeInMillis;
    }

    public long getTimeInMillis() {
        return timeInMillis();
    }

    public TimeValue time() {
        return new TimeValue(timeInMillis());
    }

    public TimeValue getTime() {
        return time();
    }

    public long existsCount() {
        return this.existsCount;
    }

    public long getExistsCount() {
        return this.existsCount;
    }

    public long existsTimeInMillis() {
        return this.existsTimeInMillis;
    }

    public long getExistsTimeInMillis() {
        return this.existsTimeInMillis;
    }

    public TimeValue existsTime() {
        return new TimeValue(existsTimeInMillis);
    }

    public TimeValue getExistsTime() {
        return existsTime();
    }

    public long missingCount() {
        return this.missingCount;
    }

    public long getMissingCount() {
        return this.missingCount;
    }

    public long missingTimeInMillis() {
        return this.missingTimeInMillis;
    }

    public long getMissingTimeInMillis() {
        return this.missingTimeInMillis;
    }

    public TimeValue missingTime() {
        return new TimeValue(missingTimeInMillis);
    }

    public TimeValue getMissingTime() {
        return missingTime();
    }

    public long current() {
        return this.current;
    }

    public long getCurrent() {
        return this.current;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.GET);
        builder.field(Fields.TOTAL, count());
        builder.field(Fields.TIME, time().toString());
        builder.field(Fields.TIME_IN_MILLIS, timeInMillis());
        builder.field(Fields.EXISTS_TOTAL, existsCount);
        builder.field(Fields.EXISTS_TIME, existsTime().toString());
        builder.field(Fields.EXISTS_TIME_IN_MILLIS, existsTimeInMillis);
        builder.field(Fields.MISSING_TOTAL, missingCount);
        builder.field(Fields.MISSING_TIME, missingTime().toString());
        builder.field(Fields.MISSING_TIME_IN_MILLIS, missingTimeInMillis);
        builder.field(Fields.CURRENT, current);
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString GET = new XContentBuilderString("get");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString TIME = new XContentBuilderString("time");
        static final XContentBuilderString TIME_IN_MILLIS = new XContentBuilderString("time_in_millis");
        static final XContentBuilderString EXISTS_TOTAL = new XContentBuilderString("exists_total");
        static final XContentBuilderString EXISTS_TIME = new XContentBuilderString("exists_time");
        static final XContentBuilderString EXISTS_TIME_IN_MILLIS = new XContentBuilderString("exists_time_in_millis");
        static final XContentBuilderString MISSING_TOTAL = new XContentBuilderString("missing_total");
        static final XContentBuilderString MISSING_TIME = new XContentBuilderString("missing_time");
        static final XContentBuilderString MISSING_TIME_IN_MILLIS = new XContentBuilderString("missing_time_in_millis");
        static final XContentBuilderString CURRENT = new XContentBuilderString("current");
    }

    public static GetStats readGetStats(StreamInput in) throws IOException {
        GetStats stats = new GetStats();
        stats.readFrom(in);
        return stats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        existsCount = in.readVLong();
        existsTimeInMillis = in.readVLong();
        missingCount = in.readVLong();
        missingTimeInMillis = in.readVLong();
        current = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(existsCount);
        out.writeVLong(existsTimeInMillis);
        out.writeVLong(missingCount);
        out.writeVLong(missingTimeInMillis);
        out.writeVLong(current);
    }
}
