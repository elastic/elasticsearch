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
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record ScriptStats(

    List<ScriptContextStats> contextStats,
    long compilations,
    long cacheEvictions,
    long compilationLimitTriggered,
    TimeSeries compilationsHistory,
    TimeSeries cacheEvictionsHistory
) implements Writeable, ToXContentFragment {

    public ScriptStats(
        long compilations,
        long cacheEvictions,
        long compilationLimitTriggered,
        TimeSeries compilationsHistory,
        TimeSeries cacheEvictionsHistory
    ) {
        this(
            List.of(),
            compilations,
            cacheEvictions,
            compilationLimitTriggered,
            Objects.requireNonNullElseGet(compilationsHistory, () -> new TimeSeries(compilations)),
            Objects.requireNonNullElseGet(cacheEvictionsHistory, () -> new TimeSeries(cacheEvictions))
        );
    }

    public static ScriptStats of(List<ScriptContextStats> contextStats) {
        long compilations = 0;
        long cacheEvictions = 0;
        long compilationLimitTriggered = 0;
        for (var stats : contextStats) {
            compilations += stats.getCompilations();
            cacheEvictions += stats.getCacheEvictions();
            compilationLimitTriggered += stats.getCompilationLimitTriggered();
        }
        return new ScriptStats(
            contextStats.stream().sorted(ScriptContextStats::compareTo).toList(),
            compilations,
            cacheEvictions,
            compilationLimitTriggered,
            new TimeSeries(compilations),
            new TimeSeries(cacheEvictions)
        );
    }

    public static ScriptStats of(ScriptContextStats context) {
        return new ScriptStats(
            context.getCompilations(),
            context.getCacheEvictions(),
            context.getCompilationLimitTriggered(),
            context.getCompilationsHistory(),
            context.getCacheEvictionsHistory()
        );
    }

    public static ScriptStats of(StreamInput in) throws IOException {
        TimeSeries compilationsHistory;
        TimeSeries cacheEvictionsHistory;
        long compilations;
        long cacheEvictions;
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_1_0)) {
            compilationsHistory = new TimeSeries(in);
            cacheEvictionsHistory = new TimeSeries(in);
            compilations = compilationsHistory.total;
            cacheEvictions = cacheEvictionsHistory.total;
        } else {
            compilations = in.readVLong();
            cacheEvictions = in.readVLong();
            compilationsHistory = new TimeSeries(compilations);
            cacheEvictionsHistory = new TimeSeries(cacheEvictions);
        }
        var compilationLimitTriggered = in.readVLong();
        var contextStats = in.readList(ScriptContextStats::new);
        return new ScriptStats(
            contextStats,
            compilations,
            cacheEvictions,
            compilationLimitTriggered,
            compilationsHistory,
            cacheEvictionsHistory
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_1_0)) {
            compilationsHistory.writeTo(out);
            cacheEvictionsHistory.writeTo(out);
        } else {
            out.writeVLong(compilations);
            out.writeVLong(cacheEvictions);
        }
        out.writeVLong(compilationLimitTriggered);
        out.writeList(contextStats);
    }

    public List<ScriptContextStats> getContextStats() {
        return contextStats;
    }

    public long getCompilations() {
        return compilations;
    }

    public long getCacheEvictions() {
        return cacheEvictions;
    }

    public long getCompilationLimitTriggered() {
        return compilationLimitTriggered;
    }

    public ScriptCacheStats toScriptCacheStats() {
        if (contextStats.isEmpty()) {
            return new ScriptCacheStats(this);
        }
        Map<String, ScriptStats> contexts = Maps.newMapWithExpectedSize(contextStats.size());
        for (ScriptContextStats contextStats : contextStats) {
            contexts.put(contextStats.getContext(), ScriptStats.of(contextStats));
        }
        return new ScriptCacheStats(contexts);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SCRIPT_STATS);
        builder.field(Fields.COMPILATIONS, compilations);
        builder.field(Fields.CACHE_EVICTIONS, cacheEvictions);
        builder.field(Fields.COMPILATION_LIMIT_TRIGGERED, compilationLimitTriggered);
        if (compilationsHistory != null && compilationsHistory.areTimingsEmpty() == false) {
            builder.startObject(ScriptContextStats.Fields.COMPILATIONS_HISTORY);
            compilationsHistory.toXContent(builder, params);
            builder.endObject();
        }
        if (cacheEvictionsHistory != null && cacheEvictionsHistory.areTimingsEmpty() == false) {
            builder.startObject(ScriptContextStats.Fields.COMPILATIONS_HISTORY);
            cacheEvictionsHistory.toXContent(builder, params);
            builder.endObject();
        }
        builder.startArray(Fields.CONTEXTS);
        for (ScriptContextStats contextStats : contextStats) {
            contextStats.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String SCRIPT_STATS = "script";
        static final String CONTEXTS = "contexts";
        static final String COMPILATIONS = "compilations";
        static final String CACHE_EVICTIONS = "cache_evictions";
        static final String COMPILATION_LIMIT_TRIGGERED = "compilation_limit_triggered";
    }
}
