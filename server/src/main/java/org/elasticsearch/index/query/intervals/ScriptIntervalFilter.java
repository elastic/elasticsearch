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

package org.elasticsearch.index.query.intervals;

import org.apache.lucene.queries.intervals.IntervalsSource;
import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.IntervalFilter;
import org.elasticsearch.index.query.IntervalFilterScript;
import org.elasticsearch.index.query.IntervalsSourceProvider;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.script.Script;

import java.io.IOException;
import java.util.Objects;

public class ScriptIntervalFilter extends IntervalFilter {
    public static final String NAME = "script";

    private final Script script;

    public ScriptIntervalFilter(Script script) {
        this.script = script;
    }

    public ScriptIntervalFilter(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.CURRENT)) {
            this.script = new Script(in);
        } else {
            // back-compat
            // name already read due to being named writable
            in.readOptionalNamedWriteable(IntervalsSourceProvider.class); // no-op, just here to read the null filter
            in.readBoolean();
            this.script = new Script(in);
        }
    }

    @Override
    public IntervalsSource filter(final IntervalsSource input, final QueryShardContext context, final MappedFieldType fieldType)
        throws IOException {
        IntervalFilterScript ifs = context.getScriptService().compile(script, IntervalFilterScript.CONTEXT).newInstance();
        return new IntervalsSourceProvider.ScriptFilterSource(input, script.getIdOrCode(), ifs);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(getWriteableName(), script, params);
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.CURRENT)) {
            script.writeTo(out);
        } else {
            // back-compat
            // name already written due to being a named writable
            out.writeOptionalNamedWriteable(null); // filter is was always null for script filter
            out.writeBoolean(true);
            script.writeTo(out);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public int hashCode() {
        return Objects.hash(script, NAME);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScriptIntervalFilter other = (ScriptIntervalFilter) o;
        return Objects.equals(script, other.script);
    }

    public static ScriptIntervalFilter fromXContent(XContentParser parser) throws IOException {
        Script script = Script.parse(parser);
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "Expected [END_OBJECT] but got [" + parser.currentToken() + "]");
        }
        return new ScriptIntervalFilter(script);
    }
}
