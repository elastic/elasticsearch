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

package org.elasticsearch.index.search.stats;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class SearchStats implements Streamable, ToXContent {

    public static class Stats implements Streamable, ToXContent {

        private long queryCount;
        private long queryTimeInMillis;
        private long queryCurrent;

        private long fetchCount;
        private long fetchTimeInMillis;
        private long fetchCurrent;

        Stats() {

        }

        public Stats(long queryCount, long queryTimeInMillis, long queryCurrent, long fetchCount, long fetchTimeInMillis, long fetchCurrent) {
            this.queryCount = queryCount;
            this.queryTimeInMillis = queryTimeInMillis;
            this.queryCurrent = queryCurrent;
            this.fetchCount = fetchCount;
            this.fetchTimeInMillis = fetchTimeInMillis;
            this.fetchCurrent = fetchCurrent;
        }

        public Stats(Stats stats) {
            this(stats.queryCount, stats.queryTimeInMillis, stats.queryCurrent, stats.fetchCount, stats.fetchTimeInMillis, stats.fetchCurrent);
        }

        public void add(Stats stats) {
            queryCount += stats.queryCount;
            queryTimeInMillis += stats.queryTimeInMillis;
            queryCurrent += stats.queryCurrent;

            fetchCount += stats.fetchCount;
            fetchTimeInMillis += stats.fetchTimeInMillis;
            fetchCurrent += stats.fetchCurrent;
        }

        public long getQueryCount() {
            return queryCount;
        }

        public TimeValue getQueryTime() {
            return new TimeValue(queryTimeInMillis);
        }

        public long getQueryTimeInMillis() {
            return queryTimeInMillis;
        }

        public long getQueryCurrent() {
            return queryCurrent;
        }

        public long getFetchCount() {
            return fetchCount;
        }

        public TimeValue getFetchTime() {
            return new TimeValue(fetchTimeInMillis);
        }

        public long getFetchTimeInMillis() {
            return fetchTimeInMillis;
        }

        public long getFetchCurrent() {
            return fetchCurrent;
        }


        public static Stats readStats(StreamInput in) throws IOException {
            Stats stats = new Stats();
            stats.readFrom(in);
            return stats;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            queryCount = in.readVLong();
            queryTimeInMillis = in.readVLong();
            queryCurrent = in.readVLong();

            fetchCount = in.readVLong();
            fetchTimeInMillis = in.readVLong();
            fetchCurrent = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(queryCount);
            out.writeVLong(queryTimeInMillis);
            out.writeVLong(queryCurrent);

            out.writeVLong(fetchCount);
            out.writeVLong(fetchTimeInMillis);
            out.writeVLong(fetchCurrent);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.QUERY_TOTAL, queryCount);
            builder.timeValueField(Fields.QUERY_TIME_IN_MILLIS, Fields.QUERY_TIME, queryTimeInMillis);
            builder.field(Fields.QUERY_CURRENT, queryCurrent);

            builder.field(Fields.FETCH_TOTAL, fetchCount);
            builder.timeValueField(Fields.FETCH_TIME_IN_MILLIS, Fields.FETCH_TIME, fetchTimeInMillis);
            builder.field(Fields.FETCH_CURRENT, fetchCurrent);

            return builder;
        }
    }

    Stats totalStats;
    long openContexts;

    @Nullable
    Map<String, Stats> groupStats;

    public SearchStats() {
        totalStats = new Stats();
    }

    public SearchStats(Stats totalStats, long openContexts, @Nullable Map<String, Stats> groupStats) {
        this.totalStats = totalStats;
        this.openContexts = openContexts;
        this.groupStats = groupStats;
    }

    public void add(SearchStats searchStats) {
        add(searchStats, true);
    }

    public void add(SearchStats searchStats, boolean includeTypes) {
        if (searchStats == null) {
            return;
        }
        totalStats.add(searchStats.totalStats);
        openContexts += searchStats.openContexts;
        if (includeTypes && searchStats.groupStats != null && !searchStats.groupStats.isEmpty()) {
            if (groupStats == null) {
                groupStats = new HashMap<>(searchStats.groupStats.size());
            }
            for (Map.Entry<String, Stats> entry : searchStats.groupStats.entrySet()) {
                Stats stats = groupStats.get(entry.getKey());
                if (stats == null) {
                    groupStats.put(entry.getKey(), new Stats(entry.getValue()));
                } else {
                    stats.add(entry.getValue());
                }
            }
        }
    }

    public Stats getTotal() {
        return this.totalStats;
    }

    public long getOpenContexts() {
        return this.openContexts;
    }

    @Nullable
    public Map<String, Stats> getGroupStats() {
        return this.groupStats;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.SEARCH);
        builder.field(Fields.OPEN_CONTEXTS, openContexts);
        totalStats.toXContent(builder, params);
        if (groupStats != null && !groupStats.isEmpty()) {
            builder.startObject(Fields.GROUPS);
            for (Map.Entry<String, Stats> entry : groupStats.entrySet()) {
                builder.startObject(entry.getKey(), XContentBuilder.FieldCaseConversion.NONE);
                entry.getValue().toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString SEARCH = new XContentBuilderString("search");
        static final XContentBuilderString OPEN_CONTEXTS = new XContentBuilderString("open_contexts");
        static final XContentBuilderString GROUPS = new XContentBuilderString("groups");
        static final XContentBuilderString QUERY_TOTAL = new XContentBuilderString("query_total");
        static final XContentBuilderString QUERY_TIME = new XContentBuilderString("query_time");
        static final XContentBuilderString QUERY_TIME_IN_MILLIS = new XContentBuilderString("query_time_in_millis");
        static final XContentBuilderString QUERY_CURRENT = new XContentBuilderString("query_current");
        static final XContentBuilderString FETCH_TOTAL = new XContentBuilderString("fetch_total");
        static final XContentBuilderString FETCH_TIME = new XContentBuilderString("fetch_time");
        static final XContentBuilderString FETCH_TIME_IN_MILLIS = new XContentBuilderString("fetch_time_in_millis");
        static final XContentBuilderString FETCH_CURRENT = new XContentBuilderString("fetch_current");
    }

    public static SearchStats readSearchStats(StreamInput in) throws IOException {
        SearchStats searchStats = new SearchStats();
        searchStats.readFrom(in);
        return searchStats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        totalStats = Stats.readStats(in);
        openContexts = in.readVLong();
        if (in.readBoolean()) {
            int size = in.readVInt();
            groupStats = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                groupStats.put(in.readString(), Stats.readStats(in));
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalStats.writeTo(out);
        out.writeVLong(openContexts);
        if (groupStats == null || groupStats.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(groupStats.size());
            for (Map.Entry<String, Stats> entry : groupStats.entrySet()) {
                out.writeString(entry.getKey());
                entry.getValue().writeTo(out);
            }
        }
    }

    @Override
    public String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder().prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (IOException e) {
            return "{ \"error\" : \"" + e.getMessage() + "\"}";
        }
    }
}
