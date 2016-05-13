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

package org.elasticsearch.search.rescore;

import org.elasticsearch.action.support.ToXContentToBytes;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.rescore.QueryRescorer.QueryRescoreContext;

import java.io.IOException;
import java.util.Objects;

/**
 * The abstract base builder for instances of {@link RescoreBuilder}.
 */
public abstract class RescoreBuilder<RB extends RescoreBuilder<RB>> extends ToXContentToBytes implements NamedWriteable {

    protected Integer windowSize;

    private static ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");

    /**
     * Construct an empty RescoreBuilder.
     */
    public RescoreBuilder() {
    }

    /**
     * Read from a stream.
     */
    protected RescoreBuilder(StreamInput in) throws IOException {
        windowSize = in.readOptionalVInt();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalVInt(this.windowSize);
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    @SuppressWarnings("unchecked")
    public RB windowSize(int windowSize) {
        this.windowSize = windowSize;
        return (RB) this;
    }

    public Integer windowSize() {
        return windowSize;
    }

    public static RescoreBuilder<?> parseFromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        String fieldName = null;
        RescoreBuilder<?> rescorer = null;
        Integer windowSize = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (parseContext.getParseFieldMatcher().match(fieldName, WINDOW_SIZE_FIELD)) {
                    windowSize = parser.intValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "rescore doesn't support [" + fieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                // we only have QueryRescorer at this point
                if (QueryRescorerBuilder.NAME.equals(fieldName)) {
                    rescorer = QueryRescorerBuilder.fromXContent(parseContext);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "rescore doesn't support rescorer with name [" + fieldName + "]");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "unexpected token [" + token + "] after [" + fieldName + "]");
            }
        }
        if (rescorer == null) {
            throw new ParsingException(parser.getTokenLocation(), "missing rescore type");
        }
        if (windowSize != null) {
            rescorer.windowSize(windowSize.intValue());
        }
        return rescorer;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (windowSize != null) {
            builder.field("window_size", windowSize);
        }
        doXContent(builder, params);
        builder.endObject();
        return builder;
    }

    protected abstract void doXContent(XContentBuilder builder, Params params) throws IOException;

    public abstract QueryRescoreContext build(QueryShardContext context) throws IOException;

    public static QueryRescorerBuilder queryRescorer(QueryBuilder queryBuilder) {
        return new QueryRescorerBuilder(queryBuilder);
    }

    @Override
    public int hashCode() {
        return Objects.hash(windowSize);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        @SuppressWarnings("rawtypes")
        RescoreBuilder other = (RescoreBuilder) obj;
        return Objects.equals(windowSize, other.windowSize);
    }
}
