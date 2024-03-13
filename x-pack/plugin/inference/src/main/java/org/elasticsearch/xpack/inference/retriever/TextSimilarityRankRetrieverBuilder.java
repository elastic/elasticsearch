/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.retriever;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.inference.rank.TextSimilarityRankBuilder;

import java.io.IOException;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_WINDOW_SIZE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TextSimilarityRankRetrieverBuilder extends RetrieverBuilder {

    public static final String NAME = "text_similarity_rank";
    public static final NodeFeature NODE_FEATURE = new NodeFeature(NAME + "_retriever");

    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");
    public static final ParseField MODEL_ID_FIELD = new ParseField("model_id");
    public static final ParseField MODEL_TEXT_FIELD = new ParseField("model_text");

    public static final ConstructingObjectParser<TextSimilarityRankRetrieverBuilder, RetrieverParserContext> PARSER =
        new ConstructingObjectParser<>(NAME, args -> {
            int argIndex = 0;
            RetrieverBuilder retrieverBuilder = (RetrieverBuilder) args[argIndex++];
            String field = (String) args[argIndex++];
            int windowSize = args[2] == null ? DEFAULT_WINDOW_SIZE : (int) args[argIndex++];
            String modelId = (String) args[argIndex++];
            String modelText = (String) args[argIndex];

            return new TextSimilarityRankRetrieverBuilder(retrieverBuilder, field, windowSize, modelId, modelText);
        });

    static {
        PARSER.declareNamedObject(
            constructorArg(),
            (p, c, n) -> p.namedObject(RetrieverBuilder.class, n, c),
            RETRIEVER_FIELD
        );
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareInt(optionalConstructorArg(), WINDOW_SIZE_FIELD);
        PARSER.declareString(constructorArg(), MODEL_ID_FIELD);
        PARSER.declareString(constructorArg(), MODEL_TEXT_FIELD);

        RetrieverBuilder.declareBaseParserFields(NAME, PARSER);
    }

    public static TextSimilarityRankRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context)
        throws IOException {
        if (context.clusterSupportsFeature(NODE_FEATURE) == false) {
            throw new ParsingException(parser.getTokenLocation(), "unknown retriever [" + NAME + "]");
        }
        return PARSER.apply(parser, context);
    }

    private final RetrieverBuilder retrieverBuilder;
    private final String field;
    private final int windowSize;
    private final String modelId;
    private final String modelQuery;

    public TextSimilarityRankRetrieverBuilder(RetrieverBuilder retrieverBuilder, String field, int windowSize, String modelId, String modelQuery) {
        this.retrieverBuilder = retrieverBuilder;
        this.field = field;
        this.windowSize = windowSize;
        this.modelId = modelId;
        this.modelQuery = modelQuery;
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        retrieverBuilder.extractToSearchSourceBuilder(searchSourceBuilder, compoundUsed);

        if (searchSourceBuilder.rankBuilder() == null) {
            searchSourceBuilder.rankBuilder(new TextSimilarityRankBuilder(this.field, this.modelId, this.modelQuery, this.windowSize));
        }
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {

    }

    @Override
    protected boolean doEquals(Object o) {
        return false;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }
}
