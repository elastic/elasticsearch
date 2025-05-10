/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.hybrid;

import org.elasticsearch.index.query.MatchPhraseQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class MatchPhraseQuerySettings implements QuerySettings {
    public static final QueryType QUERY_TYPE = QueryType.MATCH_PHRASE;

    public static final ParseField SLOP_FIELD = new ParseField("slop");

    static final ConstructingObjectParser<MatchPhraseQuerySettings, Void> PARSER = new ConstructingObjectParser<>(
        "match_phrase_query_settings",
        false,
        args -> {
            String typeString = (String) args[0];
            Integer slop = (Integer) args[1];

            QueryType queryType = QueryType.fromString(typeString);
            if (queryType != QUERY_TYPE) {
                throw new IllegalStateException("Query type must be " + QUERY_TYPE);
            }

            return new MatchPhraseQuerySettings(slop);
        }
    );

    static {
        PARSER.declareString(constructorArg(), TYPE_FIELD);
        PARSER.declareInt(optionalConstructorArg(), SLOP_FIELD);
    }

    private final Integer slop;

    public MatchPhraseQuerySettings(Integer slop) {
        this.slop = slop;
    }

    public Integer getSlop() {
        return slop;
    }

    @Override
    public QueryType getQueryType() {
        return QUERY_TYPE;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(TYPE_FIELD.getPreferredName(), getName());
        if (slop != null) {
            builder.field(SLOP_FIELD.getPreferredName(), slop);
        }
        builder.endObject();

        return builder;
    }

    @Override
    public QueryBuilder constructQueryBuilder(String field, String query) {
        MatchPhraseQueryBuilder queryBuilder = new MatchPhraseQueryBuilder(field, query);
        if (slop != null) {
            queryBuilder.slop(getSlop());
        }

        return queryBuilder;
    }

    public static MatchPhraseQuerySettings fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }
}
