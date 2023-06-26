/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentHelper;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.common.collect.Iterators.single;
import static org.elasticsearch.script.ScriptContextStats.Fields.COMPILATIONS_HISTORY;
import static org.elasticsearch.script.ScriptStats.Fields.CACHE_EVICTIONS;
import static org.elasticsearch.script.ScriptStats.Fields.COMPILATIONS;
import static org.elasticsearch.script.ScriptStats.Fields.COMPILATION_LIMIT_TRIGGERED;
import static org.elasticsearch.script.ScriptStats.Fields.CONTEXTS;
import static org.elasticsearch.script.ScriptStats.Fields.SCRIPT_STATS;

/**
 * Record object that holds global statistics of the scripts in a node.
 *
 * @param contextStats               A list of different {@link ScriptContextStats}
 * @param compilations               Total number of compilations.
 * @param cacheEvictions             Total number of evictions.
 * @param compilationLimitTriggered  Total number of times that the compilation time has been reached.
 * @param compilationsHistory        Historical information of the compilations in timeseries format.
 * @param cacheEvictionsHistory      Historical information of the evictions in timeseries format.
 */
public record ScriptStats(
    List<ScriptContextStats> contextStats,
    long compilations,
    long cacheEvictions,
    long compilationLimitTriggered,
    TimeSeries compilationsHistory,
    TimeSeries cacheEvictionsHistory
) implements Writeable, ChunkedToXContent {

    public static final ScriptStats IDENTITY = new ScriptStats(0, 0, 0, new TimeSeries(0), new TimeSeries(0));

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

    public static ScriptStats merge(ScriptStats first, ScriptStats second) {
        var mergedScriptContextStats = List.<ScriptContextStats>of();

        if (first.contextStats.isEmpty() == false || second.contextStats.isEmpty() == false) {
            var mapToCollectMergedStats = new HashMap<String, ScriptContextStats>();

            first.contextStats.forEach(cs -> mapToCollectMergedStats.merge(cs.context(), cs, ScriptContextStats::merge));
            second.contextStats.forEach(cs -> mapToCollectMergedStats.merge(cs.context(), cs, ScriptContextStats::merge));

            mergedScriptContextStats = new ArrayList<>(mapToCollectMergedStats.values());
        }

        return new ScriptStats(
            mergedScriptContextStats,
            first.compilations + second.compilations,
            first.cacheEvictions + second.cacheEvictions,
            first.compilationLimitTriggered + second.compilationLimitTriggered,
            TimeSeries.merge(first.compilationsHistory, second.compilationsHistory),
            TimeSeries.merge(first.cacheEvictionsHistory, second.cacheEvictionsHistory)
        );
    }

    public static ScriptStats read(List<ScriptContextStats> contextStats) {
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

    public static ScriptStats read(ScriptContextStats context) {
        return new ScriptStats(
            context.getCompilations(),
            context.getCacheEvictions(),
            context.getCompilationLimitTriggered(),
            context.getCompilationsHistory(),
            context.getCacheEvictionsHistory()
        );
    }

    public static ScriptStats read(StreamInput in) throws IOException {
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
        var contextStats = in.readList(ScriptContextStats::read);
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
            contexts.put(contextStats.getContext(), ScriptStats.read(contextStats));
        }
        return new ScriptCacheStats(contexts);
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
        return Iterators.concat(
            ChunkedToXContentHelper.startObject(SCRIPT_STATS),
            ChunkedToXContentHelper.field(COMPILATIONS, compilations),
            ChunkedToXContentHelper.field(CACHE_EVICTIONS, cacheEvictions),
            ChunkedToXContentHelper.field(COMPILATION_LIMIT_TRIGGERED, compilationLimitTriggered),
            single((builder, params) -> {
                if (compilationsHistory != null && compilationsHistory.areTimingsEmpty() == false) {
                    builder.startObject(COMPILATIONS_HISTORY);
                    compilationsHistory.toXContent(builder, params);
                    builder.endObject();
                }
                if (cacheEvictionsHistory != null && cacheEvictionsHistory.areTimingsEmpty() == false) {
                    builder.startObject(COMPILATIONS_HISTORY);
                    cacheEvictionsHistory.toXContent(builder, params);
                    builder.endObject();
                }
                return builder;
            }),
            ChunkedToXContentHelper.array(CONTEXTS, contextStats.iterator()),
            ChunkedToXContentHelper.endObject()
        );
    }

    static final class Fields {
        static final String SCRIPT_STATS = "script";
        static final String CONTEXTS = "contexts";
        static final String COMPILATIONS = "compilations";
        static final String CACHE_EVICTIONS = "cache_evictions";
        static final String COMPILATION_LIMIT_TRIGGERED = "compilation_limit_triggered";
    }
}
