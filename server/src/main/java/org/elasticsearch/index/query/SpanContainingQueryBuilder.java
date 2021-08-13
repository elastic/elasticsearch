/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.spans.SpanContainingQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.query.SpanQueryBuilder.SpanQueryBuilderUtil.checkNoBoost;

/**
 * Builder for {@link org.apache.lucene.search.spans.SpanContainingQuery}.
 */
public class SpanContainingQueryBuilder extends AbstractQueryBuilder<SpanContainingQueryBuilder> implements SpanQueryBuilder {
    public static final String NAME = "span_containing";

    private static final ParseField BIG_FIELD = new ParseField("big");
    private static final ParseField LITTLE_FIELD = new ParseField("little");

    private final SpanQueryBuilder big;
    private final SpanQueryBuilder little;

    /**
     * @param big the big clause, it must enclose {@code little} for a match.
     * @param little the little clause, it must be contained within {@code big} for a match.
     */
    public SpanContainingQueryBuilder(SpanQueryBuilder big, SpanQueryBuilder little) {
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
     * Read from a stream.
     */
    public SpanContainingQueryBuilder(StreamInput in) throws IOException {
        super(in);
        big = (SpanQueryBuilder) in.readNamedWriteable(QueryBuilder.class);
        little = (SpanQueryBuilder) in.readNamedWriteable(QueryBuilder.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(big);
        out.writeNamedWriteable(little);
    }

    /**
     * @return the big clause, it must enclose {@code little} for a match.
     */
    public SpanQueryBuilder bigQuery() {
        return this.big;
    }

    /**
     * @return the little clause, it must be contained within {@code big} for a match.
     */
    public SpanQueryBuilder littleQuery() {
        return this.little;
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

    public static SpanContainingQueryBuilder fromXContent(XContentParser parser) throws IOException {
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
                if (BIG_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    QueryBuilder query = parseInnerQueryBuilder(parser);
                    if (query instanceof SpanQueryBuilder == false) {
                        throw new ParsingException(parser.getTokenLocation(), "span_containing [big] must be of type span query");
                    }
                    big = (SpanQueryBuilder) query;
                    checkNoBoost(NAME, currentFieldName, parser, big);
                } else if (LITTLE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    QueryBuilder query = parseInnerQueryBuilder(parser);
                    if (query instanceof SpanQueryBuilder == false) {
                        throw new ParsingException(parser.getTokenLocation(), "span_containing [little] must be of type span query");
                    }
                    little = (SpanQueryBuilder) query;
                    checkNoBoost(NAME, currentFieldName, parser, little);
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "[span_containing] query does not support [" + currentFieldName + "]");
                }
            } else if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                boost = parser.floatValue();
            } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                queryName = parser.text();
            } else {
                throw new ParsingException(parser.getTokenLocation(),
                        "[span_containing] query does not support [" + currentFieldName + "]");
            }
        }

        SpanContainingQueryBuilder query = new SpanContainingQueryBuilder(big, little);
        query.boost(boost).queryName(queryName);
        return query;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        Query innerBig = big.toQuery(context);
        assert innerBig instanceof SpanQuery;
        Query innerLittle = little.toQuery(context);
        assert innerLittle instanceof SpanQuery;
        return new SpanContainingQuery((SpanQuery) innerBig, (SpanQuery) innerLittle);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(big, little);
    }

    @Override
    protected boolean doEquals(SpanContainingQueryBuilder other) {
        return Objects.equals(big, other.big) &&
               Objects.equals(little, other.little);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
