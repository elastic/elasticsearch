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
import java.util.List;
import java.util.stream.Collectors;

public class ScriptStats implements Writeable, ToXContentFragment {
    private final long compilations;
    private final long cacheEvictions;
    private final long compilationLimitTriggered;
    private final List<ScriptContextStats> contexts;

    public ScriptStats(long compilations, long cacheEvictions, long compilationLimitTriggered) {
        this.compilations = compilations;
        this.cacheEvictions = cacheEvictions;
        this.compilationLimitTriggered = compilationLimitTriggered;
        this.contexts = Collections.emptyList();
    }

    public ScriptStats(List<ScriptContextStats> context) {
        this.contexts = Collections.unmodifiableList(context.stream().sorted().collect(Collectors.toList()));

        long compilations = 0;
        long cacheEvictions = 0;
        long compilationLimitTriggered = 0;
        for (ScriptContextStats stat: context) {
            compilations += stat.getCompilations();
            cacheEvictions += stat.getCacheEvictions();
            compilationLimitTriggered += stat.getCompilationLimitTriggered();
        }

        this.compilations = compilations;
        this.cacheEvictions = cacheEvictions;
        this.compilationLimitTriggered = compilationLimitTriggered;
    }

    public ScriptStats(StreamInput in) throws IOException {
        this.compilations = in.readLong();
        this.cacheEvictions = in.readLong();
        this.compilationLimitTriggered = in.readLong();
        this.contexts = in.readList(ScriptContextStats::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(compilations);
        out.writeLong(cacheEvictions);
        out.writeLong(compilationLimitTriggered);
        out.writeList(contexts);
    }

    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SCRIPT_STATS);
        builder.field(ScriptContextStats.Fields.COMPILATIONS, compilations);
        builder.field(ScriptContextStats.Fields.CACHE_EVICTIONS, cacheEvictions);
        builder.field(ScriptContextStats.Fields.COMPILATION_LIMIT_TRIGGERED, compilationLimitTriggered);

        builder.startArray(Fields.CONTEXTS);
        for (ScriptContextStats stats: contexts) {
            builder.startObject();
            builder.field(ScriptContextStats.Fields.CONTEXT, stats.getContextName());
            builder.field(ScriptContextStats.Fields.COMPILATIONS, stats.getCompilations());
            builder.field(ScriptContextStats.Fields.CACHE_EVICTIONS, stats.getCacheEvictions());
            builder.field(ScriptContextStats.Fields.COMPILATION_LIMIT_TRIGGERED, stats.getCompilationLimitTriggered());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();

        return builder;
    }

    public ScriptContextStats getByContext(String context) {
        for (ScriptContextStats stats: contexts) {
            if (stats.getContextName().equals(context)) {
                return stats;
            }
        }
        return null;
    }

    public List<ScriptContextStats> getContextStats() {
        return contexts;
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

    static final class Fields {
        static final String SCRIPT_STATS = "script";
        static final String CONTEXTS = "contexts";
    }
}
