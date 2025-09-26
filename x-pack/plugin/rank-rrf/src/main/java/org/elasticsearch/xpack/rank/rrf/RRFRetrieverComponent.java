/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.rrf;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

public class RRFRetrieverComponent implements ToXContentObject {

    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");
    public static final ParseField WEIGHT_FIELD = new ParseField("weight");
    static final float DEFAULT_WEIGHT = 1f;

    final RetrieverBuilder retriever;
    final float weight;

    public RRFRetrieverComponent(RetrieverBuilder retrieverBuilder, Float weight) {
        this.retriever = Objects.requireNonNull(retrieverBuilder, "retrieverBuilder must not be null");
        this.weight = weight == null ? DEFAULT_WEIGHT : weight;
        if (this.weight < 0) {
            throw new IllegalArgumentException("[weight] must be non-negative, found [" + this.weight + "]");
        }
    }

    public RetrieverBuilder retriever() {
        return retriever;
    }

    public float weight() {
        return weight;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContentObject.Params params) throws IOException {
        builder.startObject();
        builder.field(RETRIEVER_FIELD.getPreferredName(), retriever);
        builder.field(WEIGHT_FIELD.getPreferredName(), weight);
        builder.endObject();
        return builder;
    }

    public static RRFRetrieverComponent fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "expected object but found [{}]", parser.currentToken());
        }

        // Peek at the first field to determine the format
        XContentParser.Token token = parser.nextToken();
        if (token == XContentParser.Token.END_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "retriever component must contain a retriever");
        }
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new ParsingException(parser.getTokenLocation(), "expected field name but found [{}]", token);
        }

        String firstFieldName = parser.currentName();

        // Check if this is a structured component (starts with "retriever" or "weight")
        if (RETRIEVER_FIELD.match(firstFieldName, parser.getDeprecationHandler())
            || WEIGHT_FIELD.match(firstFieldName, parser.getDeprecationHandler())) {
            // This is a structured component - parse manually
            RetrieverBuilder retriever = null;
            Float weight = null;

            do {
                String fieldName = parser.currentName();
                if (RETRIEVER_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    if (retriever != null) {
                        throw new ParsingException(parser.getTokenLocation(), "only one retriever can be specified");
                    }
                    parser.nextToken();
                    if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                        throw new ParsingException(parser.getTokenLocation(), "retriever must be an object");
                    }
                    parser.nextToken();
                    String retrieverType = parser.currentName();
                    retriever = parser.namedObject(RetrieverBuilder.class, retrieverType, context);
                    context.trackRetrieverUsage(retriever);
                    parser.nextToken();
                } else if (WEIGHT_FIELD.match(fieldName, parser.getDeprecationHandler())) {
                    if (weight != null) {
                        throw new ParsingException(parser.getTokenLocation(), "[weight] field can only be specified once");
                    }
                    parser.nextToken();
                    weight = parser.floatValue();
                } else {
                    throw new ParsingException(
                        parser.getTokenLocation(),
                        "unknown field [{}], expected [{}] or [{}]",
                        fieldName,
                        RETRIEVER_FIELD.getPreferredName(),
                        WEIGHT_FIELD.getPreferredName()
                    );
                }
            } while (parser.nextToken() == XContentParser.Token.FIELD_NAME);

            if (retriever == null) {
                throw new ParsingException(parser.getTokenLocation(), "retriever component must contain a retriever");
            }

            return new RRFRetrieverComponent(retriever, weight);
        } else {
            RetrieverBuilder retriever = parser.namedObject(RetrieverBuilder.class, firstFieldName, context);
            context.trackRetrieverUsage(retriever);
            if (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                throw new ParsingException(parser.getTokenLocation(), "unknown field [{}] after retriever", parser.currentName());
            }
            return new RRFRetrieverComponent(retriever, DEFAULT_WEIGHT);
        }
    }
}
