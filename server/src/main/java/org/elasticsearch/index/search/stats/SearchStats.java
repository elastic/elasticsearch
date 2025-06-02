/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.search.stats;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class SearchStats implements Writeable, ToXContentFragment {

    public static class Stats implements Writeable, ToXContentFragment {

        private long queryCount;
        private long queryTimeInMillis;
        private long queryCurrent;

        private long fetchCount;
        private long fetchTimeInMillis;
        private long fetchCurrent;

        private long scrollCount;
        private long scrollTimeInMillis;
        private long scrollCurrent;

        private long suggestCount;
        private long suggestTimeInMillis;
        private long suggestCurrent;

        private long queryFailure;
        private long fetchFailure;

        private double recentSearchLoad;

        private Stats() {
            // for internal use, initializes all counts to 0
        }

        public Stats(
            long queryCount,
            long queryTimeInMillis,
            long queryCurrent,
            long queryFailure,
            long fetchCount,
            long fetchTimeInMillis,
            long fetchCurrent,
            long fetchFailure,
            long scrollCount,
            long scrollTimeInMillis,
            long scrollCurrent,
            long suggestCount,
            long suggestTimeInMillis,
            long suggestCurrent,
            double recentSearchLoad
        ) {
            this.queryCount = queryCount;
            this.queryTimeInMillis = queryTimeInMillis;
            this.queryCurrent = queryCurrent;
            this.queryFailure = queryFailure;

            this.fetchCount = fetchCount;
            this.fetchTimeInMillis = fetchTimeInMillis;
            this.fetchCurrent = fetchCurrent;
            this.fetchFailure = fetchFailure;

            this.scrollCount = scrollCount;
            this.scrollTimeInMillis = scrollTimeInMillis;
            this.scrollCurrent = scrollCurrent;

            this.suggestCount = suggestCount;
            this.suggestTimeInMillis = suggestTimeInMillis;
            this.suggestCurrent = suggestCurrent;

            this.recentSearchLoad = recentSearchLoad;

        }

        private Stats(StreamInput in) throws IOException {
            queryCount = in.readVLong();
            queryTimeInMillis = in.readVLong();
            queryCurrent = in.readVLong();

            fetchCount = in.readVLong();
            fetchTimeInMillis = in.readVLong();
            fetchCurrent = in.readVLong();

            scrollCount = in.readVLong();
            scrollTimeInMillis = in.readVLong();
            scrollCurrent = in.readVLong();

            suggestCount = in.readVLong();
            suggestTimeInMillis = in.readVLong();
            suggestCurrent = in.readVLong();

            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                queryFailure = in.readVLong();
                fetchFailure = in.readVLong();
            }

            if (in.getTransportVersion().onOrAfter(TransportVersions.SEARCH_LOAD_PER_INDEX_STATS)) {
                recentSearchLoad = in.readDouble();
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(queryCount);
            out.writeVLong(queryTimeInMillis);
            out.writeVLong(queryCurrent);

            out.writeVLong(fetchCount);
            out.writeVLong(fetchTimeInMillis);
            out.writeVLong(fetchCurrent);

            out.writeVLong(scrollCount);
            out.writeVLong(scrollTimeInMillis);
            out.writeVLong(scrollCurrent);

            out.writeVLong(suggestCount);
            out.writeVLong(suggestTimeInMillis);
            out.writeVLong(suggestCurrent);

            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                out.writeVLong(queryFailure);
                out.writeVLong(fetchFailure);
            }

            if (out.getTransportVersion().onOrAfter(TransportVersions.SEARCH_LOAD_PER_INDEX_STATS)) {
                out.writeDouble(recentSearchLoad);
            }
        }

        public void add(Stats stats) {
            queryCount += stats.queryCount;
            queryTimeInMillis += stats.queryTimeInMillis;
            queryCurrent += stats.queryCurrent;
            queryFailure += stats.queryFailure;

            fetchCount += stats.fetchCount;
            fetchTimeInMillis += stats.fetchTimeInMillis;
            fetchCurrent += stats.fetchCurrent;
            fetchFailure += stats.fetchFailure;

            scrollCount += stats.scrollCount;
            scrollTimeInMillis += stats.scrollTimeInMillis;
            scrollCurrent += stats.scrollCurrent;

            suggestCount += stats.suggestCount;
            suggestTimeInMillis += stats.suggestTimeInMillis;
            suggestCurrent += stats.suggestCurrent;

            recentSearchLoad += stats.recentSearchLoad;
        }

        public void addForClosingShard(Stats stats) {
            queryCount += stats.queryCount;
            queryTimeInMillis += stats.queryTimeInMillis;
            queryFailure += stats.queryFailure;

            fetchCount += stats.fetchCount;
            fetchTimeInMillis += stats.fetchTimeInMillis;
            fetchFailure += stats.fetchFailure;

            scrollCount += stats.scrollCount;
            scrollTimeInMillis += stats.scrollTimeInMillis;
            // need consider the count of the shard's current scroll
            scrollCount += stats.scrollCurrent;

            suggestCount += stats.suggestCount;
            suggestTimeInMillis += stats.suggestTimeInMillis;

            recentSearchLoad += stats.recentSearchLoad;
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

        public long getQueryFailure() {
            return queryFailure;
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

        public long getFetchFailure() {
            return fetchFailure;
        }

        public long getScrollCount() {
            return scrollCount;
        }

        public TimeValue getScrollTime() {
            return new TimeValue(scrollTimeInMillis);
        }

        public long getScrollTimeInMillis() {
            return scrollTimeInMillis;
        }

        public long getScrollCurrent() {
            return scrollCurrent;
        }

        public long getSuggestCount() {
            return suggestCount;
        }

        public long getSuggestTimeInMillis() {
            return suggestTimeInMillis;
        }

        public TimeValue getSuggestTime() {
            return new TimeValue(suggestTimeInMillis);
        }

        public long getSuggestCurrent() {
            return suggestCurrent;
        }

        public double getSearchLoadRate() {
            return recentSearchLoad;
        }

        public static Stats readStats(StreamInput in) throws IOException {
            return new Stats(in);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.QUERY_TOTAL, queryCount);
            builder.humanReadableField(Fields.QUERY_TIME_IN_MILLIS, Fields.QUERY_TIME, getQueryTime());
            builder.field(Fields.QUERY_CURRENT, queryCurrent);
            builder.field(Fields.QUERY_FAILURE, queryFailure);

            builder.field(Fields.FETCH_TOTAL, fetchCount);
            builder.humanReadableField(Fields.FETCH_TIME_IN_MILLIS, Fields.FETCH_TIME, getFetchTime());
            builder.field(Fields.FETCH_CURRENT, fetchCurrent);
            builder.field(Fields.FETCH_FAILURE, fetchFailure);

            builder.field(Fields.SCROLL_TOTAL, scrollCount);
            builder.humanReadableField(Fields.SCROLL_TIME_IN_MILLIS, Fields.SCROLL_TIME, getScrollTime());
            builder.field(Fields.SCROLL_CURRENT, scrollCurrent);

            builder.field(Fields.SUGGEST_TOTAL, suggestCount);
            builder.humanReadableField(Fields.SUGGEST_TIME_IN_MILLIS, Fields.SUGGEST_TIME, getSuggestTime());
            builder.field(Fields.SUGGEST_CURRENT, suggestCurrent);

            builder.field(Fields.EMW_RATE, recentSearchLoad);

            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Stats that = (Stats) o;
            return queryCount == that.queryCount
                && queryTimeInMillis == that.queryTimeInMillis
                && queryCurrent == that.queryCurrent
                && queryFailure == that.queryFailure
                && fetchCount == that.fetchCount
                && fetchTimeInMillis == that.fetchTimeInMillis
                && fetchCurrent == that.fetchCurrent
                && fetchFailure == that.fetchFailure
                && scrollCount == that.scrollCount
                && scrollTimeInMillis == that.scrollTimeInMillis
                && scrollCurrent == that.scrollCurrent
                && suggestCount == that.suggestCount
                && suggestTimeInMillis == that.suggestTimeInMillis
                && suggestCurrent == that.suggestCurrent
                && recentSearchLoad == that.recentSearchLoad;
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                queryCount,
                queryTimeInMillis,
                queryCurrent,
                queryFailure,
                fetchCount,
                fetchTimeInMillis,
                fetchCurrent,
                fetchCount,
                scrollCount,
                scrollTimeInMillis,
                scrollCurrent,
                suggestCount,
                suggestTimeInMillis,
                suggestCurrent,
                recentSearchLoad
            );
        }
    }

    private final Stats totalStats;
    private long openContexts;

    @Nullable
    private Map<String, Stats> groupStats;

    public SearchStats() {
        totalStats = new Stats();
    }

    public SearchStats(Stats totalStats, long openContexts, @Nullable Map<String, Stats> groupStats) {
        this.totalStats = totalStats;
        this.openContexts = openContexts;
        this.groupStats = groupStats;
    }

    public SearchStats(StreamInput in) throws IOException {
        totalStats = Stats.readStats(in);
        openContexts = in.readVLong();
        if (in.readBoolean()) {
            groupStats = in.readMap(Stats::readStats);
        }
    }

    public void add(SearchStats searchStats) {
        if (searchStats == null) {
            return;
        }
        addTotals(searchStats);
        openContexts += searchStats.openContexts;
        if (searchStats.groupStats != null && searchStats.groupStats.isEmpty() == false) {
            if (groupStats == null) {
                groupStats = Maps.newMapWithExpectedSize(searchStats.groupStats.size());
            }
            for (Map.Entry<String, Stats> entry : searchStats.groupStats.entrySet()) {
                groupStats.putIfAbsent(entry.getKey(), new Stats());
                groupStats.get(entry.getKey()).add(entry.getValue());
            }
        }
    }

    public void addTotals(SearchStats searchStats) {
        if (searchStats == null) {
            return;
        }
        totalStats.add(searchStats.totalStats);
    }

    public void addTotalsForClosingShard(SearchStats searchStats) {
        if (searchStats == null) {
            return;
        }
        totalStats.addForClosingShard(searchStats.totalStats);
    }

    public Stats getTotal() {
        return this.totalStats;
    }

    public long getOpenContexts() {
        return this.openContexts;
    }

    @Nullable
    public Map<String, Stats> getGroupStats() {
        return this.groupStats != null ? Collections.unmodifiableMap(this.groupStats) : null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(Fields.SEARCH);
        builder.field(Fields.OPEN_CONTEXTS, openContexts);
        totalStats.toXContent(builder, params);
        if (groupStats != null && groupStats.isEmpty() == false) {
            builder.startObject(Fields.GROUPS);
            for (Map.Entry<String, Stats> entry : groupStats.entrySet()) {
                builder.startObject(entry.getKey());
                entry.getValue().toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

    static final class Fields {
        static final String SEARCH = "search";
        static final String OPEN_CONTEXTS = "open_contexts";
        static final String GROUPS = "groups";
        static final String QUERY_TOTAL = "query_total";
        static final String QUERY_TIME = "query_time";
        static final String QUERY_TIME_IN_MILLIS = "query_time_in_millis";
        static final String QUERY_CURRENT = "query_current";
        static final String QUERY_FAILURE = "query_failure";
        static final String FETCH_TOTAL = "fetch_total";
        static final String FETCH_TIME = "fetch_time";
        static final String FETCH_TIME_IN_MILLIS = "fetch_time_in_millis";
        static final String FETCH_CURRENT = "fetch_current";
        static final String FETCH_FAILURE = "fetch_failure";
        static final String SCROLL_TOTAL = "scroll_total";
        static final String SCROLL_TIME = "scroll_time";
        static final String SCROLL_TIME_IN_MILLIS = "scroll_time_in_millis";
        static final String SCROLL_CURRENT = "scroll_current";
        static final String SUGGEST_TOTAL = "suggest_total";
        static final String SUGGEST_TIME = "suggest_time";
        static final String SUGGEST_TIME_IN_MILLIS = "suggest_time_in_millis";
        static final String SUGGEST_CURRENT = "suggest_current";
        static final String EMW_RATE = "ewm_rate";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        totalStats.writeTo(out);
        out.writeVLong(openContexts);
        if (groupStats == null || groupStats.isEmpty()) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeMap(groupStats, StreamOutput::writeWriteable);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchStats that = (SearchStats) o;
        return Objects.equals(totalStats, that.totalStats)
            && openContexts == that.openContexts
            && Objects.equals(groupStats, that.groupStats);
    }

    @Override
    public int hashCode() {
        return Objects.hash(totalStats, openContexts, groupStats);
    }
}
