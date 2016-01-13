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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.index.query.QueryShardContext;

import java.io.IOException;
import java.util.Objects;

/**
 * The base builder for rescorers. Wraps a conrete instance of {@link RescoreBuilder} and
 * adds the ability to specify the optional `window_size` parameter
 */
public class RescoreBaseBuilder implements ToXContent, Writeable<RescoreBaseBuilder> {

    private RescoreBuilder rescorer;
    private Integer windowSize;
    public static final RescoreBaseBuilder PROTOTYPE = new RescoreBaseBuilder(new QueryRescorerBuilder(new MatchAllQueryBuilder()));

    private static ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");

    public RescoreBaseBuilder(RescoreBuilder rescorer) {
        if (rescorer == null) {
            throw new IllegalArgumentException("rescorer cannot be null");
        }
        this.rescorer = rescorer;
    }

    public RescoreBuilder rescorer() {
        return this.rescorer;
    }

    public RescoreBaseBuilder windowSize(int windowSize) {
        this.windowSize = windowSize;
        return this;
    }

    public Integer windowSize() {
        return windowSize;
    }

    public RescoreBaseBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();
        String fieldName = null;
        RescoreBuilder rescorer = null;
        Integer windowSize = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
            } else if (token.isValue()) {
                if (parseContext.parseFieldMatcher().match(fieldName, WINDOW_SIZE_FIELD)) {
                    windowSize = parser.intValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "rescore doesn't support [" + fieldName + "]");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                // we only have QueryRescorer at this point
                if (QueryRescorerBuilder.NAME.equals(fieldName)) {
                    rescorer = QueryRescorerBuilder.PROTOTYPE.fromXContent(parseContext);
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
        RescoreBaseBuilder rescoreBuilder = new RescoreBaseBuilder(rescorer);
        if (windowSize != null) {
            rescoreBuilder.windowSize(windowSize.intValue());
        }
        return rescoreBuilder;
    }

    public RescoreSearchContext build(QueryShardContext context) throws IOException {
        RescoreSearchContext rescoreContext = this.rescorer.build(context);
        if (windowSize != null) {
            rescoreContext.setWindowSize(this.windowSize);
        }
        return rescoreContext;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (windowSize != null) {
            builder.field("window_size", windowSize);
        }
        rescorer.toXContent(builder, params);
        return builder;
    }

    public static QueryRescorerBuilder queryRescorer(QueryBuilder<?> queryBuilder) {
        return new QueryRescorerBuilder(queryBuilder);
    }

    @Override
    public final int hashCode() {
        return Objects.hash(windowSize, rescorer);
    }

    @Override
    public final boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        RescoreBaseBuilder other = (RescoreBaseBuilder) obj;
        return Objects.equals(windowSize, other.windowSize) &&
               Objects.equals(rescorer, other.rescorer);
    }

    @Override
    public RescoreBaseBuilder readFrom(StreamInput in) throws IOException {
        RescoreBaseBuilder builder = new RescoreBaseBuilder(in.readRescorer());
        builder.windowSize = in.readOptionalVInt();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeRescorer(rescorer);
        out.writeOptionalVInt(this.windowSize);
    }

    @Override
    public final String toString() {
        try {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.prettyPrint();
            builder.startObject();
            toXContent(builder, EMPTY_PARAMS);
            builder.endObject();
            return builder.string();
        } catch (Exception e) {
            return "{ \"error\" : \"" + ExceptionsHelper.detailedMessage(e) + "\"}";
        }
    }
}
