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
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

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

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<RRFRetrieverComponent, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        "rrf_component",
        false,
        (args, context) -> {
            RetrieverBuilder retrieverBuilder = (RetrieverBuilder) args[0];
            Float weight = (Float) args[1];
            return new RRFRetrieverComponent(retrieverBuilder, weight);
        }
    );

    static {
        PARSER.declareNamedObject(constructorArg(), (p, c, n) -> {
            RetrieverBuilder innerRetriever = p.namedObject(RetrieverBuilder.class, n, c);
            c.trackRetrieverUsage(innerRetriever.getName());
            return innerRetriever;
        }, RETRIEVER_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), WEIGHT_FIELD);
    }

    public static RRFRetrieverComponent fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        RetrieverBuilder innerRetriever = null;
        float weight = DEFAULT_WEIGHT;

        if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
            throw new ParsingException(parser.getTokenLocation(), "[{}] expected object", parser.currentToken());
        }

        while ((parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            var name = parser.currentName();

            if (name.equals(RETRIEVER_FIELD.getPreferredName())) {
                if (parser.nextToken() != XContentParser.Token.START_OBJECT) {
                    throw new ParsingException(parser.getTokenLocation(), "[{}] expected object", parser.currentToken());
                }
                parser.nextToken();

                name = parser.currentName();
                innerRetriever = parser.namedObject(RetrieverBuilder.class, name, context);
                parser.nextToken();
            } else if (name.equals(WEIGHT_FIELD.getPreferredName())) {
                if (parser.nextToken() != XContentParser.Token.VALUE_NUMBER) {
                    throw new ParsingException(parser.getTokenLocation(), "[{}] expected number", parser.currentToken());
                }

                weight = parser.floatValue();
            } else {
                innerRetriever = parser.namedObject(RetrieverBuilder.class, name, context);
                context.trackRetrieverUsage(innerRetriever.getName());
                parser.nextToken();
                break;
            }
        }
        return new RRFRetrieverComponent(innerRetriever, weight);
    }
}
