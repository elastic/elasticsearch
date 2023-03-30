/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.queries.spans.SpanFirstQuery;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.index.query.SpanQueryBuilder.SpanQueryBuilderUtil.checkNoBoost;

public class SpanFirstQueryBuilder extends AbstractQueryBuilder<SpanFirstQueryBuilder> implements SpanQueryBuilder {
    public static final String NAME = "span_first";

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
        boostAndQueryNameToXContent(builder);
        builder.endObject();
    }

    public static SpanFirstQueryBuilder fromXContent(XContentParser parser) throws IOException {
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
                if (MATCH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    QueryBuilder query = parseInnerQueryBuilder(parser);
                    if (query instanceof SpanQueryBuilder == false) {
                        throw new ParsingException(parser.getTokenLocation(), "span_first [match] must be of type span query");
                    }
                    match = (SpanQueryBuilder) query;
                    checkNoBoost(NAME, currentFieldName, parser, match);
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_first] query does not support [" + currentFieldName + "]");
                }
            } else {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else if (END_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    end = parser.intValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[span_first] query does not support [" + currentFieldName + "]");
                }
            }
        }
        if (match == null) {
            throw new ParsingException(parser.getTokenLocation(), "span_first must have [match] span query clause");
        }
        if (end == null) {
            throw new ParsingException(parser.getTokenLocation(), "span_first must have [end] set for it");
        }
        SpanFirstQueryBuilder queryBuilder = new SpanFirstQueryBuilder(match, end);
        queryBuilder.boost(boost).queryName(queryName);
        return queryBuilder;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
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
        return Objects.equals(matchBuilder, other.matchBuilder) && Objects.equals(end, other.end);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.ZERO;
    }
}
