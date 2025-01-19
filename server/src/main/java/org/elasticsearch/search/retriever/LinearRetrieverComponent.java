/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.search.retriever.LinearRetrieverBuilder.RETRIEVERS_FIELD;

public class LinearRetrieverComponent implements ToXContentObject {

    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");
    public static final ParseField WEIGHT_FIELD = new ParseField("weight");
    public static final ParseField NORMALIZER_FIELD = new ParseField("normalizer");

    static final float DEFAULT_WEIGHT = 1f;
    static final ScoreNormalizer DEFAULT_NORMALIZER = IdentityScoreNormalizer.INSTANCE;

    RetrieverBuilder retriever;
    float weight;
    ScoreNormalizer normalizer;

    public LinearRetrieverComponent(RetrieverBuilder retrieverBuilder, Float weight, ScoreNormalizer normalizer) {
        assert retrieverBuilder != null;
        this.retriever = retrieverBuilder;
        this.weight = weight == null ? DEFAULT_WEIGHT : weight;
        this.normalizer = normalizer == null ? DEFAULT_NORMALIZER : normalizer;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(RETRIEVERS_FIELD.getPreferredName(), retriever);
        return builder;
    }

    public static LinearRetrieverComponent fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        RetrieverBuilder retrieverBuilder = null;
        Float weight = null;
        ScoreNormalizer normalizer = null;
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
        while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
            if (parser.currentToken() == XContentParser.Token.FIELD_NAME) {
                if (RETRIEVER_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                    parser.nextToken();
                    ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.currentToken(), parser);
                    parser.nextToken();
                    ensureExpectedToken(XContentParser.Token.FIELD_NAME, parser.currentToken(), parser);
                    final String retrieverName = parser.currentName();
                    parser.nextToken();
                    retrieverBuilder = parser.namedObject(RetrieverBuilder.class, retrieverName, context);
                    parser.nextToken();
                } else if (WEIGHT_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                    parser.nextToken();
                    weight = parser.floatValue();
                } else if (NORMALIZER_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                    parser.nextToken();
                    if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
                        normalizer = ScoreNormalizer.valueOf(parser.text());
                    } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
                        parser.nextToken();
                        normalizer = ScoreNormalizer.parse(parser.currentName(), parser);
                        parser.nextToken();
                    } else {
                        throw new ParsingException(parser.getTokenLocation(), "Unsupported token [" + parser.currentToken() + "]");
                    }
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "Unexpected token [" + parser.currentToken() + "] for linear retriever."
                    );
                }
            } else {
                throw new ParsingException(
                    parser.getTokenLocation(),
                    "Expected [" + XContentParser.Token.FIELD_NAME + "] but got [" + parser.currentToken() + "] instead."
                );
            }
        }
        if (retrieverBuilder == null) {
            throw new IllegalArgumentException("Missing required field [" + RETRIEVER_FIELD.getPreferredName() + "]");
        }
        return new LinearRetrieverComponent(retrieverBuilder, weight, normalizer);
    }
}
