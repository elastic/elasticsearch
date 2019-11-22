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
import org.elasticsearch.index.query.IntervalsSourceProvider;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Function;

public abstract class SourceProviderIntervalFilter extends IntervalFilter {
    private final IntervalsSourceProvider filter;
    private final String name;

    public SourceProviderIntervalFilter(final String name, final IntervalsSourceProvider filter) {
        this.name = name;
        this.filter = filter;
    }

    public SourceProviderIntervalFilter(final String name, final StreamInput in) throws IOException {
        this.name = name;
        if (in.getVersion().onOrAfter(Version.CURRENT)) {
            this.filter = in.readNamedWriteable(IntervalsSourceProvider.class);
        } else {
            // back-compat
            // name already read due to being a named writable now
            this.filter = in.readOptionalNamedWriteable(IntervalsSourceProvider.class); // optional previously
            in.readBoolean(); // no-op to read that we never had a script
        }
    }

    public abstract IntervalsSource getIntervalsSource(IntervalsSource input, IntervalsSource filterSource) throws IOException;

    @Override
    public IntervalsSource filter(final IntervalsSource input, final QueryShardContext context, final MappedFieldType fieldType)
        throws IOException {
        IntervalsSource filterSource = filter.getSource(context, fieldType);
        return getIntervalsSource(input, filterSource);
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject();
        builder.field(name);
        builder.startObject();
        filter.toXContent(builder, params);
        builder.endObject();
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.CURRENT)) {
            out.writeNamedWriteable(filter);
        } else {
            // back-compat
            // name already written due to being a named writable now
            out.writeOptionalNamedWriteable(filter);
            out.writeBoolean(false); // always no scripts
        }
    }

    @Override
    public String getWriteableName() {
        return name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SourceProviderIntervalFilter fltr = (SourceProviderIntervalFilter) o;
        return Objects.equals(filter, fltr.filter) &&
            Objects.equals(name, fltr.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filter, name);
    }

    protected static <T extends SourceProviderIntervalFilter> T fromXContent(XContentParser parser,
                                                                             Function<IntervalsSourceProvider, T> filter)
        throws IOException {

        if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(),
                "Expected [START_OBJECT] but got [" + parser.currentToken() + "]");
        }
        if (parser.nextToken() != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "Expected [FIELD_NAME] but got [" + parser.currentToken() + "]");
        }
        IntervalsSourceProvider intervals = IntervalsSourceProvider.fromXContent(parser);
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "Expected [END_OBJECT] but got [" + parser.currentToken() + "]");
        }
        if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "Expected [END_OBJECT] but got [" + parser.currentToken() + "]");
        }

        return filter.apply(intervals);
    }
}
