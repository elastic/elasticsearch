/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

// This class is deprecated in favor of ScriptStats and ScriptContextStats
public record ScriptCacheStats(Map<String, ScriptStats> context, ScriptStats general) implements Writeable, ToXContentFragment {

    public ScriptCacheStats(Map<String, ScriptStats> context) {
        this(Collections.unmodifiableMap(context), null);
    }

    public ScriptCacheStats(ScriptStats general) {
        this(null, Objects.requireNonNull(general));
    }

    public static ScriptCacheStats read(StreamInput in) throws IOException {
        boolean isContext = in.readBoolean();
        if (isContext == false) {
            return new ScriptCacheStats(ScriptStats.read(in));
        }

        int size = in.readInt();
        Map<String, ScriptStats> context = Maps.newMapWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            String name = in.readString();
            context.put(name, ScriptStats.read(in));
        }
        return new ScriptCacheStats(context);
    }

    private Map.Entry<String, ScriptStats>[] sortedContextStats() {
        @SuppressWarnings("unchecked")
        Map.Entry<String, ScriptStats>[] stats = context.entrySet().toArray(Map.Entry[]::new);
        Arrays.sort(stats, Map.Entry.comparingByKey());
        return stats;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (general != null) {
            out.writeBoolean(false);
            general.writeTo(out);
            return;
        }

        out.writeBoolean(true);
        out.writeInt(context.size());
        for (Map.Entry<String, ScriptStats> stats : sortedContextStats()) {
            out.writeString(stats.getKey());
            stats.getValue().writeTo(out);
        }
    }

    private static void scriptStatsToXContent(ScriptStats s, XContentBuilder builder) throws IOException {
        builder.field(ScriptStats.Fields.COMPILATIONS, s.getCompilations());
        builder.field(ScriptStats.Fields.CACHE_EVICTIONS, s.getCacheEvictions());
        builder.field(ScriptStats.Fields.COMPILATION_LIMIT_TRIGGERED, s.getCompilationLimitTriggered());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SCRIPT_CACHE_STATS);
        builder.startObject(Fields.SUM);
        if (general != null) {
            scriptStatsToXContent(general, builder);
            builder.endObject().endObject();
            return builder;
        }

        scriptStatsToXContent(sum(), builder);
        builder.endObject();

        builder.startArray(Fields.CONTEXTS);
        for (Map.Entry<String, ScriptStats> stats : sortedContextStats()) {
            builder.startObject();
            builder.field(Fields.CONTEXT, stats.getKey());
            scriptStatsToXContent(stats.getValue(), builder);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();

        return builder;
    }

    /**
     * Get the context specific stats, null if using general cache
     */
    public Map<String, ScriptStats> getContextStats() {
        return context;
    }

    /**
     * Get the general stats, null if using context cache
     */
    public ScriptStats getGeneralStats() {
        return general;
    }

    /**
     * The sum of all script stats, either the general stats or the sum of all stats of the context stats.
     */
    public ScriptStats sum() {
        if (general != null) {
            return general;
        }
        long compilations = 0;
        long cacheEvictions = 0;
        long compilationLimitTriggered = 0;
        for (ScriptStats stat : context.values()) {
            compilations += stat.getCompilations();
            cacheEvictions += stat.getCacheEvictions();
            compilationLimitTriggered += stat.getCompilationLimitTriggered();
        }
        return new ScriptStats(compilations, cacheEvictions, compilationLimitTriggered, null, null);
    }

    static final class Fields {
        static final String SCRIPT_CACHE_STATS = "script_cache";
        static final String CONTEXT = "context";
        static final String SUM = "sum";
        static final String CONTEXTS = "contexts";
    }
}
