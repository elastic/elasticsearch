/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.queries.function.FunctionScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * The BoostingQuery class can be used to effectively demote results that match a given query.
 * Unlike the "NOT" clause, this still selects documents that contain undesirable terms,
 * but reduces their overall score:
 * <p>
 * Query balancedQuery = new BoostingQuery(positiveQuery, negativeQuery, 0.01f);
 * In this scenario the positiveQuery contains the mandatory, desirable criteria which is used to
 * select all matching documents, and the negativeQuery contains the undesirable elements which
 * are simply used to lessen the scores. Documents that match the negativeQuery have their score
 * multiplied by the supplied "boost" parameter, so this should be less than 1 to achieve a
 * demoting effect
 */
public class BoostingQueryBuilder extends AbstractQueryBuilder<BoostingQueryBuilder> {
    public static final String NAME = "boosting";

    private static final ParseField POSITIVE_FIELD = new ParseField("positive");
    private static final ParseField NEGATIVE_FIELD = new ParseField("negative");
    private static final ParseField NEGATIVE_BOOST_FIELD = new ParseField("negative_boost");

    private final QueryBuilder positiveQuery;

    private final QueryBuilder negativeQuery;

    private float negativeBoost = -1;

    /**
     * Create a new {@link BoostingQueryBuilder}
     *
     * @param positiveQuery the positive query for this boosting query.
     * @param negativeQuery the negative query for this boosting query.
     */
    public BoostingQueryBuilder(QueryBuilder positiveQuery, QueryBuilder negativeQuery) {
        if (positiveQuery == null) {
            throw new IllegalArgumentException("inner clause [positive] cannot be null.");
        }
        if (negativeQuery == null) {
            throw new IllegalArgumentException("inner clause [negative] cannot be null.");
        }
        this.positiveQuery = positiveQuery;
        this.negativeQuery = negativeQuery;
    }

    /**
     * Read from a stream.
     */
    public BoostingQueryBuilder(StreamInput in) throws IOException {
        super(in);
        positiveQuery = in.readNamedWriteable(QueryBuilder.class);
        negativeQuery = in.readNamedWriteable(QueryBuilder.class);
        negativeBoost = in.readFloat();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(positiveQuery);
        out.writeNamedWriteable(negativeQuery);
        out.writeFloat(negativeBoost);
    }

    /**
     * Get the positive query for this boosting query.
     */
    public QueryBuilder positiveQuery() {
        return this.positiveQuery;
    }

    /**
     * Get the negative query for this boosting query.
     */
    public QueryBuilder negativeQuery() {
        return this.negativeQuery;
    }

    /**
     * Set the negative boost factor.
     */
    public BoostingQueryBuilder negativeBoost(float negativeBoost) {
        if (negativeBoost < 0) {
            throw new IllegalArgumentException("query requires negativeBoost to be set to positive value");
        }
        this.negativeBoost = negativeBoost;
        return this;
    }

    /**
     * Get the negative boost factor.
     */
    public float negativeBoost() {
        return this.negativeBoost;
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(NAME);
        builder.field(POSITIVE_FIELD.getPreferredName());
        positiveQuery.toXContent(builder, params);
        builder.field(NEGATIVE_FIELD.getPreferredName());
        negativeQuery.toXContent(builder, params);
        builder.field(NEGATIVE_BOOST_FIELD.getPreferredName(), negativeBoost);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static BoostingQueryBuilder fromXContent(XContentParser parser) throws IOException {
        QueryBuilder positiveQuery = null;
        boolean positiveQueryFound = false;
        QueryBuilder negativeQuery = null;
        boolean negativeQueryFound = false;
        float boost = DEFAULT_BOOST;
        float negativeBoost = -1;
        String queryName = null;

        String currentFieldName = null;
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (POSITIVE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    positiveQuery = parseInnerQueryBuilder(parser);
                    positiveQueryFound = true;
                } else if (NEGATIVE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    negativeQuery = parseInnerQueryBuilder(parser);
                    negativeQueryFound = true;
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[boosting] query does not support [" + currentFieldName + "]");
                }
            } else if (token.isValue()) {
                if (NEGATIVE_BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    negativeBoost = parser.floatValue();
                } else if (NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    queryName = parser.text();
                } else if (BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    boost = parser.floatValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "[boosting] query does not support [" + currentFieldName + "]");
                }
            }
        }

        if (positiveQueryFound == false) {
            throw new ParsingException(parser.getTokenLocation(), "[boosting] query requires 'positive' query to be set'");
        }
        if (negativeQueryFound == false) {
            throw new ParsingException(parser.getTokenLocation(), "[boosting] query requires 'negative' query to be set'");
        }
        if (negativeBoost < 0) {
            throw new ParsingException(
                parser.getTokenLocation(),
                "[boosting] query requires 'negative_boost' to be set to be a positive value'"
            );
        }

        BoostingQueryBuilder boostingQuery = new BoostingQueryBuilder(positiveQuery, negativeQuery);
        boostingQuery.negativeBoost(negativeBoost);
        boostingQuery.boost(boost);
        boostingQuery.queryName(queryName);
        return boostingQuery;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        Query positive = positiveQuery.toQuery(context);
        Query negative = negativeQuery.toQuery(context);
        return FunctionScoreQuery.boostByQuery(positive, negative, negativeBoost);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(negativeBoost, positiveQuery, negativeQuery);
    }

    @Override
    protected boolean doEquals(BoostingQueryBuilder other) {
        return Objects.equals(negativeBoost, other.negativeBoost)
            && Objects.equals(positiveQuery, other.positiveQuery)
            && Objects.equals(negativeQuery, other.negativeQuery);
    }

    @Override
    protected QueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        QueryBuilder positiveQuery = this.positiveQuery.rewrite(queryRewriteContext);
        if (positiveQuery instanceof MatchNoneQueryBuilder) {
            return positiveQuery;
        }

        QueryBuilder negativeQuery = this.negativeQuery.rewrite(queryRewriteContext);
        if (positiveQuery != this.positiveQuery || negativeQuery != this.negativeQuery) {
            BoostingQueryBuilder newQueryBuilder = new BoostingQueryBuilder(positiveQuery, negativeQuery);
            newQueryBuilder.negativeBoost = negativeBoost;
            return newQueryBuilder;
        }
        return this;
    }

    @Override
    protected void extractInnerHitBuilders(Map<String, InnerHitContextBuilder> innerHits) {
        InnerHitContextBuilder.extractInnerHits(positiveQuery, innerHits);
        InnerHitContextBuilder.extractInnerHits(negativeQuery, innerHits);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.ZERO;
    }

    @Override
    public Query toHighlightQuery(String fieldName) {
        return Stream.of(positiveQuery, negativeQuery)
            .map(qb -> qb.toHighlightQuery(fieldName))
            .filter(Objects::nonNull)
            .toList()
            .stream()
            .findFirst()
            .orElse(null);
    }
}
