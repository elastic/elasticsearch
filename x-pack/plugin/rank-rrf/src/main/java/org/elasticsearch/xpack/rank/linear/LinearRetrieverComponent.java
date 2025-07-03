/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class LinearRetrieverComponent implements ToXContentObject {

    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");
    public static final ParseField WEIGHT_FIELD = new ParseField("weight");
    public static final ScoreNormalizer DEFAULT_NORMALIZER = IdentityScoreNormalizer.INSTANCE;

    static final float DEFAULT_WEIGHT = 1f;

    RetrieverBuilder retriever;
    float weight;

    public LinearRetrieverComponent(RetrieverBuilder retrieverBuilder, Float weight) {
        assert retrieverBuilder != null;
        this.retriever = retrieverBuilder;
        this.weight = weight == null ? DEFAULT_WEIGHT : weight;
        if (this.weight < 0) {
            throw new IllegalArgumentException("[weight] must be non-negative");
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(RETRIEVER_FIELD.getPreferredName(), retriever);
        builder.field(WEIGHT_FIELD.getPreferredName(), weight);
        return builder;
    }

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<LinearRetrieverComponent, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        "retriever-component",
        false,
        args -> {
            RetrieverBuilder retrieverBuilder = (RetrieverBuilder) args[0];
            Float weight = (Float) args[1];
            return new LinearRetrieverComponent(retrieverBuilder, weight);
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

    public static LinearRetrieverComponent fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        return PARSER.apply(parser, context);
    }
}
