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

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanFirstQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

public class SpanFirstQueryBuilder extends AbstractQueryBuilder<SpanFirstQueryBuilder> implements SpanQueryBuilder {

    public static final String NAME = "span_first";
    public static final ParseField QUERY_NAME_FIELD = new ParseField(NAME);

    private static final ParseField MATCH_FIELD = new ParseField("match");
    private static final ParseField END_FIELD = new ParseField("end");

    private final SpanQueryBuilder matchBuilder;

    private final int end;

    /**
     * Query that matches spans queries defined in <code>matchBuilder</code>
     * whose end position is less than or equal to <code>end</code>.
     * @param matchBuilder inner {@link SpanQueryBuilder}
     * @param end maximum end position of the match, needs to be positive
     * @throws IllegalArgumentException for negative <code>end</code> positions
     */
    public SpanFirstQueryBuilder(SpanQueryBuilder matchBuilder, int end) {
        if (matchBuilder == null) {
            throw new IllegalArgumentException("inner span query cannot be null");
        }
        if (end < 0) {
            throw new IllegalArgumentException("parameter [end] needs to be positive.");
        }
        this.matchBuilder = matchBuilder;
        this.end = end;
    }

    /**
     * Read from a stream.
     */
    public SpanFirstQueryBuilder(StreamInput in) throws IOException {
        super(in);
        matchBuilder = (SpanQueryBuilder) in.readNamedWriteable(QueryBuilder.class);
        end = in.readInt();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(matchBuilder);
        out.writeInt(end);
    }

    /**
     * @return the inner {@link SpanQueryBuilder} defined in this query
     */
    public SpanQueryBuilder innerQuery() {
        return this.matchBuilder;
    }

    /**
     * @return maximum end position of the matching inner span query
     */
    public int end() {
        return this.end;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(MATCH_FIELD.getPreferredName());
        matchBuilder.toXContent(builder, params);
        builder.field(END_FIELD.getPreferredName(), end);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static Optional<SpanFirstQueryBuilder> fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;

        SpanQueryBuilder match = null;
        Integer end = null;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, MATCH_FIELD)) {
                    Optional<QueryBuilder> query = parseContext.parseInnerQueryBuilder();
                    if (query.isPresent() == false || query.get() instanceof SpanQueryBuilder == false) {
                        throw new ParsingException(parser.getTokenLocation(), "spanFirst [match] must be of type span query");
                    }
                    match = (SpanQueryBuilder) query.get();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_first] query does not support [" + currentFieldName + "]");
                }
            } else {
                if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                    boost = parser.floatValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, END_FIELD)) {
                    end = parser.intValue();
                } else if (parseContext.getParseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_first] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (match == null) {
            throw new ParsingException(parser.getTokenLocation(), "spanFirst must have [match] span query clause");
        }
        if (end == null) {
            throw new ParsingException(parser.getTokenLocation(), "spanFirst must have [end] set for it");
        }
        SpanFirstQueryBuilder queryBuilder = new SpanFirstQueryBuilder(match, end);
        queryBuilder.boost(boost).queryName(queryName);
        return Optional.of(queryBuilder);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query innerSpanQuery = matchBuilder.toQuery(context);
        assert innerSpanQuery instanceof SpanQuery;
        return new SpanFirstQuery((SpanQuery) innerSpanQuery, end);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(matchBuilder, end);
    }

    @Override
    protected boolean doEquals(SpanFirstQueryBuilder other) {
        return Objects.equals(matchBuilder, other.matchBuilder) &&
               Objects.equals(end, other.end);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
