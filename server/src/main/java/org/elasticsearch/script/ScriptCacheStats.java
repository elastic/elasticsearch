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

package org.elasticsearch.script;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class ScriptCacheStats implements Writeable, ToXContentFragment {
    private final Map<String,ScriptStats> context;
    private final ScriptStats general;

    public ScriptCacheStats(Map<String, ScriptStats> context) {
        this.context = Collections.unmodifiableMap(context);
        this.general = null;
    }

    public ScriptCacheStats(ScriptStats general) {
        this.general = Objects.requireNonNull(general);
        this.context = null;
    }

    public ScriptCacheStats(StreamInput in) throws IOException {
        boolean isContext = in.readBoolean();
        if (isContext == false) {
            general = new ScriptStats(in);
            context = null;
            return;
        }

        general = null;
        int size = in.readInt();
        Map<String,ScriptStats> context = new HashMap<>(size);
        for (int i=0; i < size; i++) {
            String name = in.readString();
            context.put(name, new ScriptStats(in));
        }
        this.context = Collections.unmodifiableMap(context);
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
        for (String name: context.keySet().stream().sorted().collect(Collectors.toList())) {
            out.writeString(name);
            context.get(name).writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SCRIPT_CACHE_STATS);
        builder.startObject(Fields.SUM);
        if (general != null) {
            builder.field(ScriptStats.Fields.COMPILATIONS, general.getCompilations());
            builder.field(ScriptStats.Fields.CACHE_EVICTIONS, general.getCacheEvictions());
            builder.field(ScriptStats.Fields.COMPILATION_LIMIT_TRIGGERED, general.getCompilationLimitTriggered());
            builder.endObject().endObject();
            return builder;
        }

        ScriptStats sum = sum();
        builder.field(ScriptStats.Fields.COMPILATIONS, sum.getCompilations());
        builder.field(ScriptStats.Fields.CACHE_EVICTIONS, sum.getCacheEvictions());
        builder.field(ScriptStats.Fields.COMPILATION_LIMIT_TRIGGERED, sum.getCompilationLimitTriggered());
        builder.endObject();

        builder.startArray(Fields.CONTEXTS);
        for (String name: context.keySet().stream().sorted().collect(Collectors.toList())) {
            ScriptStats stats = context.get(name);
            builder.startObject();
            builder.field(Fields.CONTEXT, name);
            builder.field(ScriptStats.Fields.COMPILATIONS, stats.getCompilations());
            builder.field(ScriptStats.Fields.CACHE_EVICTIONS, stats.getCacheEvictions());
            builder.field(ScriptStats.Fields.COMPILATION_LIMIT_TRIGGERED, stats.getCompilationLimitTriggered());
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
        return ScriptStats.sum(context.values());
    }

    static final class Fields {
        static final String SCRIPT_CACHE_STATS = "script_cache";
        static final String CONTEXT = "context";
        static final String SUM = "sum";
        static final String CONTEXTS = "contexts";
    }
}
