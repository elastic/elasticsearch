/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Record object that holds stats information for the different script contexts in a node.
 *
 * @param context                    Context name.
 * @param compilations               Total number of compilations.
 * @param compilationsHistory        Historical information of the compilations of scripts in timeseries format.
 * @param cacheEvictions             Total of evictions.
 * @param cacheEvictionsHistory      Historical information of the evictions of scripts in timeseries format.
 * @param compilationLimitTriggered  Total times that a limit of compilations that have reached the limit.
 */
public record ScriptContextStats(
    String context,
    long compilations,
    TimeSeries compilationsHistory,
    long cacheEvictions,
    TimeSeries cacheEvictionsHistory,
    long compilationLimitTriggered
) implements Writeable, ToXContentFragment, Comparable<ScriptContextStats> {

    public ScriptContextStats(
        String context,
        long compilationLimitTriggered,
        TimeSeries compilationsHistory,
        TimeSeries cacheEvictionsHistory
    ) {
        this(
            Objects.requireNonNull(context),
            compilationsHistory.total,
            compilationsHistory,
            cacheEvictionsHistory.total,
            cacheEvictionsHistory,
            compilationLimitTriggered
        );
    }

    public static ScriptContextStats read(StreamInput in) throws IOException {
        var context = in.readString();
        var compilations = in.readVLong();
        var cacheEvictions = in.readVLong();
        var compilationLimitTriggered = in.readVLong();
        TimeSeries compilationsHistory;
        TimeSeries cacheEvictionsHistory;
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_1_0)) {
            compilationsHistory = new TimeSeries(in);
            cacheEvictionsHistory = new TimeSeries(in);
        } else if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_0_0)) {
            compilationsHistory = new TimeSeries(in).withTotal(compilations);
            cacheEvictionsHistory = new TimeSeries(in).withTotal(cacheEvictions);
        } else {
            compilationsHistory = new TimeSeries(compilations);
            cacheEvictionsHistory = new TimeSeries(cacheEvictions);
        }
        return new ScriptContextStats(
            context,
            compilations,
            compilationsHistory,
            cacheEvictions,
            cacheEvictionsHistory,
            compilationLimitTriggered
        );
    }

    public static ScriptContextStats merge(ScriptContextStats first, ScriptContextStats second) {
        assert first.context.equals(second.context) : "To merge 2 ScriptContextStats both of them must have the same context.";
        return new ScriptContextStats(
            first.context,
            first.compilations + second.compilations,
            TimeSeries.merge(first.compilationsHistory, second.compilationsHistory),
            first.cacheEvictions + second.cacheEvictions,
            TimeSeries.merge(first.cacheEvictionsHistory, second.cacheEvictionsHistory),
            first.compilationLimitTriggered + second.compilationLimitTriggered
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(context);
        out.writeVLong(compilations);
        out.writeVLong(cacheEvictions);
        out.writeVLong(compilationLimitTriggered);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_0_0)) {
            compilationsHistory.writeTo(out);
            cacheEvictionsHistory.writeTo(out);
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
        if (series != null && series.areTimingsEmpty() == false) {
            builder.startObject(Fields.COMPILATIONS_HISTORY);
            series.toXContent(builder, params);
            builder.endObject();
        }

        builder.field(Fields.CACHE_EVICTIONS, getCacheEvictions());
        series = getCacheEvictionsHistory();
        if (series != null && series.areTimingsEmpty() == false) {
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
