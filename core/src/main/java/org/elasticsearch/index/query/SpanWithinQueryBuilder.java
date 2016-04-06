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
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanWithinQuery;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

/**
 * Builder for {@link org.apache.lucene.search.spans.SpanWithinQuery}.
 */
public class SpanWithinQueryBuilder extends AbstractQueryBuilder<SpanWithinQueryBuilder>
        implements SpanQueryBuilder<SpanWithinQueryBuilder> {

    public static final String NAME = "span_within";
    public static final ParseField QUERY_NAME_FIELD = new ParseField(NAME);
    public static final SpanWithinQueryBuilder PROTOTYPE =
            new SpanWithinQueryBuilder(SpanTermQueryBuilder.PROTOTYPE, SpanTermQueryBuilder.PROTOTYPE);

    private static final ParseField BIG_FIELD = new ParseField("big");
    private static final ParseField LITTLE_FIELD = new ParseField("little");

    private final SpanQueryBuilder big;
    private final SpanQueryBuilder little;

    /**
     * Query that returns spans from <code>little</code> that are contained in a spans from <code>big</code>.
     * @param big clause that must enclose {@code little} for a match.
     * @param little the little clause, it must be contained within {@code big} for a match.
     */
    public SpanWithinQueryBuilder(SpanQueryBuilder big, SpanQueryBuilder little) {
        if (big == null) {
            throw new IllegalArgumentException("inner clause [big] cannot be null.");
        }
        if (little == null) {
            throw new IllegalArgumentException("inner clause [little] cannot be null.");
        }
        this.little = little;
        this.big = big;
    }

    /**
     * @return the little clause, contained within {@code big} for a match.
     */
    public SpanQueryBuilder littleQuery() {
        return this.little;
    }

    /**
     * @return the big clause that must enclose {@code little} for a match.
     */
    public SpanQueryBuilder bigQuery() {
        return this.big;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);

        builder.field(BIG_FIELD.getPreferredName());
        big.toXContent(builder, params);

        builder.field(LITTLE_FIELD.getPreferredName());
        little.toXContent(builder, params);

        printBoostAndQueryName(builder);

        builder.endObject();
    }

    public static SpanWithinQueryBuilder fromXContent(QueryParseContext parseContext) throws IOException {
        XContentParser parser = parseContext.parser();

        float boost = AbstractQueryBuilder.DEFAULT_BOOST;
        String queryName = null;
        SpanQueryBuilder big = null;
        SpanQueryBuilder little = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (parseContext.parseFieldMatcher().match(currentFieldName, BIG_FIELD)) {
                    QueryBuilder query = parseContext.parseInnerQueryBuilder();
                    if (query instanceof SpanQueryBuilder == false) {
                        throw new ParsingException(parser.getTokenLocation(), "span_within [big] must be of type span query");
                    }
                    big = (SpanQueryBuilder) query;
                } else if (parseContext.parseFieldMatcher().match(currentFieldName, LITTLE_FIELD)) {
                    QueryBuilder query = parseContext.parseInnerQueryBuilder();
                    if (query instanceof SpanQueryBuilder == false) {
                        throw new ParsingException(parser.getTokenLocation(), "span_within [little] must be of type span query");
                    }
                    little = (SpanQueryBuilder) query;
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "[span_within] query does not support [" + currentFieldName + "]");
                }
            } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.BOOST_FIELD)) {
                boost = parser.floatValue();
            } else if (parseContext.parseFieldMatcher().match(currentFieldName, AbstractQueryBuilder.NAME_FIELD)) {
                queryName = parser.text();
            } else {
                throw new ParsingException(parser.getTokenLocation(), "[span_within] query does not support [" + currentFieldName + "]");
            }
        }

        if (big == null) {
            throw new ParsingException(parser.getTokenLocation(), "span_within must include [big]");
        }
        if (little == null) {
            throw new ParsingException(parser.getTokenLocation(), "span_within must include [little]");
        }

        SpanWithinQueryBuilder query = new SpanWithinQueryBuilder(big, little);
        query.boost(boost).queryName(queryName);
        return query;
    }

    @Override
    protected Query doToQuery(QueryShardContext context) throws IOException {
        Query innerBig = big.toQuery(context);
        assert innerBig instanceof SpanQuery;
        Query innerLittle = little.toQuery(context);
        assert innerLittle instanceof SpanQuery;
        return new SpanWithinQuery((SpanQuery) innerBig, (SpanQuery) innerLittle);
    }

    @Override
    protected SpanWithinQueryBuilder doReadFrom(StreamInput in) throws IOException {
        SpanQueryBuilder big = (SpanQueryBuilder)in.readQuery();
        SpanQueryBuilder little = (SpanQueryBuilder)in.readQuery();
        return new SpanWithinQueryBuilder(big, little);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeQuery(big);
        out.writeQuery(little);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(big, little);
    }

    @Override
    protected boolean doEquals(SpanWithinQueryBuilder other) {
        return Objects.equals(big, other.big) &&
               Objects.equals(little, other.little);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
