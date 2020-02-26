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

public class ScriptStats implements Writeable, ToXContentFragment {
    private final long compilations;
    private final long cacheEvictions;
    private final long compilationLimitTriggered;

    public ScriptStats(long compilations, long cacheEvictions, long compilationLimitTriggered) {
        this.compilations = compilations;
        this.cacheEvictions = cacheEvictions;
        this.compilationLimitTriggered = compilationLimitTriggered;
    }

    public ScriptStats(StreamInput in) throws IOException {
        compilations = in.readVLong();
        cacheEvictions = in.readVLong();
        compilationLimitTriggered = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(compilations);
        out.writeVLong(cacheEvictions);
        out.writeVLong(compilationLimitTriggered);
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SCRIPT_STATS);
        builder.field(Fields.COMPILATIONS, getCompilations());
        builder.field(Fields.CACHE_EVICTIONS, getCacheEvictions());
        builder.field(Fields.COMPILATION_LIMIT_TRIGGERED, getCompilationLimitTriggered());
        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String SCRIPT_STATS = "script";
        static final String COMPILATIONS = "compilations";
        static final String CACHE_EVICTIONS = "cache_evictions";
        static final String COMPILATION_LIMIT_TRIGGERED = "compilation_limit_triggered";
    }

    public static ScriptStats sum(Iterable<ScriptStats> stats) {
        long compilations = 0;
        long cacheEvictions = 0;
        long compilationLimitTriggered = 0;
        for (ScriptStats stat: stats) {
            compilations += stat.compilations;
            cacheEvictions += stat.cacheEvictions;
            compilationLimitTriggered += stat.compilationLimitTriggered;
        }
        return new ScriptStats(
            compilations,
            cacheEvictions,
            compilationLimitTriggered
        );
    }
}
