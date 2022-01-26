/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class ScriptContextStats implements Writeable, ToXContentFragment, Comparable<ScriptContextStats> {
    private final String context;
    private final long compilations;
    private final TimeSeries compilationsHistory;
    private final long cacheEvictions;
    private final TimeSeries cacheEvictionsHistory;
    private final long compilationLimitTriggered;

    public ScriptContextStats(
        String context,
        long compilations,
        long cacheEvictions,
        long compilationLimitTriggered,
        TimeSeries compilationsHistory,
        TimeSeries cacheEvictionsHistory
    ) {
        this.context = Objects.requireNonNull(context);
        this.compilations = compilations;
        this.cacheEvictions = cacheEvictions;
        this.compilationLimitTriggered = compilationLimitTriggered;
        this.compilationsHistory = compilationsHistory;
        this.cacheEvictionsHistory = cacheEvictionsHistory;
    }

    public ScriptContextStats(StreamInput in) throws IOException {
        context = in.readString();
        compilations = in.readVLong();
        cacheEvictions = in.readVLong();
        compilationLimitTriggered = in.readVLong();
        compilationsHistory = null;
        cacheEvictionsHistory = null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(context);
        out.writeVLong(compilations);
        out.writeVLong(cacheEvictions);
        out.writeVLong(compilationLimitTriggered);
    }

    public static class TimeSeries implements Writeable, ToXContentFragment {
        public final long fiveMinutes;
        public final long fifteenMinutes;
        public final long twentyFourHours;

        public TimeSeries() {
            this.fiveMinutes = 0;
            this.fifteenMinutes = 0;
            this.twentyFourHours = 0;
        }

        public TimeSeries(long fiveMinutes, long fifteenMinutes, long twentyFourHours) {
            assert fiveMinutes >= 0;
            this.fiveMinutes = fiveMinutes;
            assert fifteenMinutes >= fiveMinutes;
            this.fifteenMinutes = fifteenMinutes;
            assert twentyFourHours >= fifteenMinutes;
            this.twentyFourHours = twentyFourHours;
        }

        public TimeSeries(StreamInput in) throws IOException {
            fiveMinutes = in.readVLong();
            fifteenMinutes = in.readVLong();
            twentyFourHours = in.readVLong();
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.FIVE_MINUTES, fiveMinutes);
            builder.field(Fields.FIFTEEN_MINUTES, fifteenMinutes);
            builder.field(Fields.TWENTY_FOUR_HOURS, twentyFourHours);
            return builder;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(fiveMinutes);
            out.writeVLong(fifteenMinutes);
            out.writeVLong(twentyFourHours);
        }

        public boolean isEmpty() {
            return twentyFourHours == 0;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TimeSeries that = (TimeSeries) o;
            return fiveMinutes == that.fiveMinutes && fifteenMinutes == that.fifteenMinutes && twentyFourHours == that.twentyFourHours;
        }

        @Override
        public int hashCode() {
            return Objects.hash(fiveMinutes, fifteenMinutes, twentyFourHours);
        }
    }

    public String getContext() {
        return context;
    }

    public long getCompilations() {
        return compilations;
    }

    public TimeSeries getCompilationsHistory() {
        return compilationsHistory;
    }

    public long getCacheEvictions() {
        return cacheEvictions;
    }

    public TimeSeries getCacheEvictionsHistory() {
        return cacheEvictionsHistory;
    }

    public long getCompilationLimitTriggered() {
        return compilationLimitTriggered;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Fields.CONTEXT, getContext());
        builder.field(Fields.COMPILATIONS, getCompilations());

        TimeSeries series = getCompilationsHistory();
        if (series != null && series.isEmpty() == false) {
            builder.startObject(Fields.COMPILATIONS_HISTORY);
            series.toXContent(builder, params);
            builder.endObject();
        }

        builder.field(Fields.CACHE_EVICTIONS, getCacheEvictions());
        series = getCacheEvictionsHistory();
        if (series != null && series.isEmpty() == false) {
            builder.startObject(Fields.CACHE_EVICTIONS_HISTORY);
            series.toXContent(builder, params);
            builder.endObject();
        }

        builder.field(Fields.COMPILATION_LIMIT_TRIGGERED, getCompilationLimitTriggered());
        builder.endObject();
        return builder;
    }

    @Override
    public int compareTo(ScriptContextStats o) {
        return this.context.compareTo(o.context);
    }

    static final class Fields {
        static final String CONTEXT = "context";
        static final String COMPILATIONS = "compilations";
        static final String COMPILATIONS_HISTORY = "compilations_history";
        static final String CACHE_EVICTIONS = "cache_evictions";
        static final String CACHE_EVICTIONS_HISTORY = "cache_evictions_history";
        static final String COMPILATION_LIMIT_TRIGGERED = "compilation_limit_triggered";
        static final String FIVE_MINUTES = "5m";
        static final String FIFTEEN_MINUTES = "15m";
        static final String TWENTY_FOUR_HOURS = "24h";
    }
}
