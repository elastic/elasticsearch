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

package org.elasticsearch.index.suggest.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 * Exposes suggest related statistics.
 */
public class SuggestStats implements Streamable, ToXContent {

    private long suggestCount;
    private long suggestTimeInMillis;
    private long current;

    public SuggestStats() {
    }

    SuggestStats(long suggestCount, long suggestTimeInMillis, long current) {
        this.suggestCount = suggestCount;
        this.suggestTimeInMillis = suggestTimeInMillis;
        this.current = current;
    }

    /**
     * @return The number of times the suggest api has been invoked.
     */
    public long getCount() {
        return suggestCount;
    }

    /**
     * @return The total amount of time spend in the suggest api
     */
    public long getTimeInMillis() {
        return suggestTimeInMillis;
    }

    /**
     * @return The total amount of time spend in the suggest api
     */
    public TimeValue getTime() {
        return new TimeValue(getTimeInMillis());
    }

    /**
     * @return The total amount of active suggest api invocations.
     */
    public long getCurrent() {
        return current;
    }

    public void add(SuggestStats suggestStats) {
        if (suggestStats != null) {
            suggestCount += suggestStats.getCount();
            suggestTimeInMillis += suggestStats.getTimeInMillis();
            current += suggestStats.getCurrent();
        }
    }

    public static SuggestStats readSuggestStats(StreamInput in) throws IOException {
        SuggestStats stats = new SuggestStats();
        stats.readFrom(in);
        return stats;
    }

    static final class Fields {
        static final XContentBuilderString SUGGEST = new XContentBuilderString("suggest");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString TIME = new XContentBuilderString("time");
        static final XContentBuilderString TIME_IN_MILLIS = new XContentBuilderString("time_in_millis");
        static final XContentBuilderString CURRENT = new XContentBuilderString("current");
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        suggestCount = in.readVLong();
        suggestTimeInMillis = in.readVLong();
        current = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(suggestCount);
        out.writeVLong(suggestTimeInMillis);
        out.writeVLong(current);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SUGGEST);
        builder.field(Fields.TOTAL, suggestCount);
        builder.timeValueField(Fields.TIME_IN_MILLIS, Fields.TIME, suggestTimeInMillis);
        builder.field(Fields.CURRENT, current);
        builder.endObject();
        return builder;
    }
}
