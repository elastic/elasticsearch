/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ScriptStats implements Writeable, ToXContentFragment {
    private final List<ScriptContextStats> contextStats;
    private final long compilations;
    private final long cacheEvictions;
    private final long compilationLimitTriggered;

    public ScriptStats(List<ScriptContextStats> contextStats) {
        ArrayList<ScriptContextStats> ctxStats = new ArrayList<>(contextStats.size());
        ctxStats.addAll(contextStats);
        ctxStats.sort(ScriptContextStats::compareTo);
        this.contextStats = Collections.unmodifiableList(ctxStats);
        long compilations = 0;
        long cacheEvictions = 0;
        long compilationLimitTriggered = 0;
        for (ScriptContextStats stats : contextStats) {
            compilations += stats.getCompilations();
            cacheEvictions += stats.getCacheEvictions();
            compilationLimitTriggered += stats.getCompilationLimitTriggered();
        }
        this.compilations = compilations;
        this.cacheEvictions = cacheEvictions;
        this.compilationLimitTriggered = compilationLimitTriggered;
    }

    public ScriptStats(long compilations, long cacheEvictions, long compilationLimitTriggered) {
        this.contextStats = Collections.emptyList();
        this.compilations = compilations;
        this.cacheEvictions = cacheEvictions;
        this.compilationLimitTriggered = compilationLimitTriggered;
    }

    public ScriptStats(ScriptContextStats context) {
        this(context.getCompilations(), context.getCacheEvictions(), context.getCompilationLimitTriggered());
    }

    public ScriptStats(StreamInput in) throws IOException {
        compilations = in.readVLong();
        cacheEvictions = in.readVLong();
        compilationLimitTriggered = in.getVersion().onOrAfter(Version.V_7_0_0) ? in.readVLong() : 0;
        contextStats = in.getVersion().onOrAfter(Version.V_7_9_0) ? in.readList(ScriptContextStats::new) : Collections.emptyList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(compilations);
        out.writeVLong(cacheEvictions);
        if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
            out.writeVLong(compilationLimitTriggered);
        }
        if (out.getVersion().onOrAfter(Version.V_7_9_0)) {
            out.writeList(contextStats);
        }
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
        Map<String, ScriptStats> contexts = new HashMap<>(contextStats.size());
        for (ScriptContextStats contextStats : contextStats) {
            contexts.put(contextStats.getContext(), new ScriptStats(contextStats));
        }
        return new ScriptCacheStats(contexts);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SCRIPT_STATS);
        builder.field(Fields.COMPILATIONS, compilations);
        builder.field(Fields.CACHE_EVICTIONS, cacheEvictions);
        builder.field(Fields.COMPILATION_LIMIT_TRIGGERED, compilationLimitTriggered);
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
