/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.search.retriever.LinearRetrieverBuilder.DEFAULT_NORMALIZER;
import static org.elasticsearch.search.retriever.LinearRetrieverBuilder.DEFAULT_WEIGHT;
import static org.elasticsearch.search.retriever.LinearRetrieverBuilder.RETRIEVERS_FIELD;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class WrapperRetrieverBuilder implements ToXContentObject {

    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");
    public static final ParseField WEIGHT_FIELD = new ParseField("weight");
    public static final ParseField NORMALIZER_FIELD = new ParseField("normalizer");

    public static final String NAME = "component";
    static final ConstructingObjectParser<WrapperRetrieverBuilder, RetrieverParserContext> PARSER = new ConstructingObjectParser<>(
        NAME,
        false,
        args -> {
            RetrieverBuilder base = (RetrieverBuilder) args[0];
            float weight = args[1] == null ? DEFAULT_WEIGHT : (float) args[1];
            LinearRetrieverBuilder.ScoreNormalizer normalizer = args[2] == null
                ? DEFAULT_NORMALIZER
                : LinearRetrieverBuilder.ScoreNormalizer.find((String) args[2]);
            return new WrapperRetrieverBuilder(base, weight, normalizer);
        }
    );

    static {
        PARSER.declareNamedObject(constructorArg(), (p, c, n) -> {
            RetrieverBuilder retrieverBuilder = p.namedObject(RetrieverBuilder.class, n, c);
            c.trackRetrieverUsage(retrieverBuilder.getName());
            return retrieverBuilder;
        }, RETRIEVER_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), WEIGHT_FIELD);
        PARSER.declareString(optionalConstructorArg(), NORMALIZER_FIELD);
    }

    RetrieverBuilder retriever;
    float weight;
    LinearRetrieverBuilder.ScoreNormalizer normalizer;

    public WrapperRetrieverBuilder(RetrieverBuilder base, float weight, LinearRetrieverBuilder.ScoreNormalizer normalizer) {
        this.retriever = base;
        this.weight = weight;
        this.normalizer = normalizer;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(RETRIEVERS_FIELD.getPreferredName(), retriever);
        return builder;
    }

    public static WrapperRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context) throws IOException {
        return PARSER.apply(parser, context);
    }
}
