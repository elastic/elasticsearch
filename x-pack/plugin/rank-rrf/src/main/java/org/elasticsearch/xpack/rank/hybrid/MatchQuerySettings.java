/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.hybrid;

import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.Operator;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class MatchQuerySettings implements QuerySettings {
    public static final QueryType QUERY_TYPE = QueryType.MATCH;

    public static final ParseField OPERATOR_FIELD = new ParseField("operator");

    static final ConstructingObjectParser<MatchQuerySettings, Void> PARSER = new ConstructingObjectParser<>(
        "match_query_settings",
        false,
        args -> {
            String typeString = (String) args[0];
            String operatorString = (String) args[1];

            QueryType queryType = QueryType.fromString(typeString);
            if (queryType != QueryType.MATCH) {
                throw new IllegalStateException("Query type must be " + QueryType.MATCH);
            }

            return new MatchQuerySettings(
                operatorString != null ? Operator.fromString(operatorString) : MatchQueryBuilder.DEFAULT_OPERATOR
            );
        }
    );

    static {
        PARSER.declareString(constructorArg(), TYPE_FIELD);
        PARSER.declareString(optionalConstructorArg(), OPERATOR_FIELD);
    }

    private final Operator operator;

    public MatchQuerySettings(Operator operator) {
        this.operator = operator;
    }

    public Operator getOperator() {
        return operator;
    }

    @Override
    public QueryType getQueryType() {
        return QUERY_TYPE;
    }

    @Override
    public QueryBuilder constructQueryBuilder(String field, String query) {
        return new MatchQueryBuilder(field, query).operator(getOperator());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE_FIELD.getPreferredName(), getName());
        builder.field(OPERATOR_FIELD.getPreferredName(), operator);
        builder.endObject();

        return builder;
    }

    public static MatchQuerySettings fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
