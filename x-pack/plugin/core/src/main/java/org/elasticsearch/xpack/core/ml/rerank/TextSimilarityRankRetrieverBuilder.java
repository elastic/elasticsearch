/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.rerank;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.RetrieverParserContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class TextSimilarityRankRetrieverBuilder extends RetrieverBuilder {

    public static final NodeFeature NODE_FEATURE = new NodeFeature(XPackField.TEXT_SIMILARITY_RERANKER + "_retriever");

    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");
    public static final ParseField INFERENCE_ID_FIELD = new ParseField("inference_id");
    public static final ParseField INFERENCE_TEXT_FIELD = new ParseField("inference_text");
    public static final ParseField MIN_SCORE_FIELD = new ParseField("min_score");

    public static final float DEFAULT_MIN_SCORE = 0.0f;

    public static final ConstructingObjectParser<TextSimilarityRankRetrieverBuilder, RetrieverParserContext> PARSER =
        new ConstructingObjectParser<>(XPackField.TEXT_SIMILARITY_RERANKER, args -> {
            RetrieverBuilder retrieverBuilder = (RetrieverBuilder) args[0];
            String field = (String) args[1];
            int windowSize = args[2] == null ? DEFAULT_RANK_WINDOW_SIZE : (int) args[2];
            String inferenceId = (String) args[3];
            String inferenceText = (String) args[4];
            float minScore = args[5] == null ? DEFAULT_MIN_SCORE : (float) args[5];

            return new TextSimilarityRankRetrieverBuilder(retrieverBuilder, field, windowSize, inferenceId, inferenceText, minScore);
        });

    static {
        PARSER.declareNamedObject(constructorArg(), (p, c, n) -> p.namedObject(RetrieverBuilder.class, n, c), RETRIEVER_FIELD);
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareInt(optionalConstructorArg(), WINDOW_SIZE_FIELD);
        PARSER.declareString(constructorArg(), INFERENCE_ID_FIELD);
        PARSER.declareString(constructorArg(), INFERENCE_TEXT_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), MIN_SCORE_FIELD);

        RetrieverBuilder.declareBaseParserFields(XPackField.TEXT_SIMILARITY_RERANKER, PARSER);
    }

    public static TextSimilarityRankRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context)
        throws IOException {
        if (context.clusterSupportsFeature(NODE_FEATURE) == false) {
            throw new ParsingException(parser.getTokenLocation(), "unknown retriever [" + XPackField.TEXT_SIMILARITY_RERANKER + "]");
        }
        return PARSER.apply(parser, context);
    }

    private final RetrieverBuilder retrieverBuilder;
    private final String field;
    private final int windowSize;
    private final String inferenceId;
    private final String inferenceText;
    private final float minScore;

    public TextSimilarityRankRetrieverBuilder(
        RetrieverBuilder retrieverBuilder,
        String field,
        int windowSize,
        String inferenceId,
        String inferenceText,
        float minScore
    ) {
        this.retrieverBuilder = retrieverBuilder;
        this.field = field;
        this.windowSize = windowSize;
        this.inferenceId = inferenceId;
        this.inferenceText = inferenceText;
        this.minScore = minScore;
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        retrieverBuilder.extractToSearchSourceBuilder(searchSourceBuilder, compoundUsed);

        if (searchSourceBuilder.rankBuilder() == null) {
            searchSourceBuilder.rankBuilder(
                new TextSimilarityRankBuilder(this.field, this.inferenceId, this.inferenceText, this.windowSize, this.minScore)
            );
        }
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) {

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
