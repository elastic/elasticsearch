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
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackField;

import java.io.IOException;

import static org.elasticsearch.search.rank.RankBuilder.DEFAULT_RANK_WINDOW_SIZE;

public class TextSimilarityRankRetrieverBuilder extends RetrieverBuilder {

    public static final NodeFeature NODE_FEATURE = new NodeFeature(XPackField.TEXT_SIMILARITY_RERANKER + "_retriever");

    public static final ParseField RETRIEVER_FIELD = new ParseField("retriever");
    public static final ParseField FIELD_FIELD = new ParseField("field");
    public static final ParseField WINDOW_SIZE_FIELD = new ParseField("window_size");
    public static final ParseField INFERENCE_ID_FIELD = new ParseField("inference_id");
    public static final ParseField INFERENCE_TEXT_FIELD = new ParseField("inference_text");
    public static final ParseField MIN_SCORE_FIELD = new ParseField("min_score");

    public static final float DEFAULT_MIN_SCORE = Float.NEGATIVE_INFINITY;

    public static final ObjectParser<TextSimilarityRankRetrieverBuilder, RetrieverParserContext> PARSER = new ObjectParser<>(
        XPackField.TEXT_SIMILARITY_RERANKER,
        TextSimilarityRankRetrieverBuilder::new
    );

    static {
        PARSER.declareObject((r, v) -> r.retrieverBuilder = v, (p, c) -> {
            RetrieverBuilder retrieverBuilder = RetrieverBuilder.parseInnerRetrieverBuilder(p, c);
            c.trackSectionUsage(XPackField.TEXT_SIMILARITY_RERANKER + ":" + RETRIEVER_FIELD.getPreferredName());
            return retrieverBuilder;
        }, RETRIEVER_FIELD);

        PARSER.declareField((r, v) -> r.field = v, (p, c) -> {
            String field = p.text();
            c.trackSectionUsage(XPackField.TEXT_SIMILARITY_RERANKER + ":" + FIELD_FIELD.getPreferredName());
            return field;
        }, FIELD_FIELD, ObjectParser.ValueType.STRING);

        PARSER.declareField((r, v) -> r.windowSize = v, (p, c) -> {
            int windowSize = p.intValue();
            c.trackSectionUsage(XPackField.TEXT_SIMILARITY_RERANKER + ":" + WINDOW_SIZE_FIELD.getPreferredName());
            return windowSize;
        }, WINDOW_SIZE_FIELD, ObjectParser.ValueType.INT_OR_NULL);

        PARSER.declareField((r, v) -> r.inferenceId = v, (p, c) -> {
            String inferenceId = p.text();
            c.trackSectionUsage(XPackField.TEXT_SIMILARITY_RERANKER + ":" + INFERENCE_ID_FIELD.getPreferredName());
            return inferenceId;
        }, INFERENCE_ID_FIELD, ObjectParser.ValueType.STRING);

        PARSER.declareField((r, v) -> r.inferenceText = v, (p, c) -> {
            String inferenceText = p.text();
            c.trackSectionUsage(XPackField.TEXT_SIMILARITY_RERANKER + ":" + INFERENCE_TEXT_FIELD.getPreferredName());
            return inferenceText;
        }, INFERENCE_TEXT_FIELD, ObjectParser.ValueType.STRING);

        PARSER.declareField((r, v) -> r.minScore = v, (p, c) -> {
            float minScore = p.floatValue();
            c.trackSectionUsage(XPackField.TEXT_SIMILARITY_RERANKER + ":" + MIN_SCORE_FIELD.getPreferredName());
            return minScore;
        }, MIN_SCORE_FIELD, ObjectParser.ValueType.FLOAT_OR_NULL);

        RetrieverBuilder.declareBaseParserFields(XPackField.TEXT_SIMILARITY_RERANKER, PARSER);
    }

    public static TextSimilarityRankRetrieverBuilder fromXContent(XContentParser parser, RetrieverParserContext context)
        throws IOException {
        if (context.clusterSupportsFeature(NODE_FEATURE) == false) {
            throw new ParsingException(parser.getTokenLocation(), "unknown retriever [" + XPackField.TEXT_SIMILARITY_RERANKER + "]");
        }
        return PARSER.apply(parser, context);
    }

    private RetrieverBuilder retrieverBuilder;
    private String field;
    private int windowSize;
    private String inferenceId;
    private String inferenceText;
    private float minScore;

    public TextSimilarityRankRetrieverBuilder() {
        this.windowSize = DEFAULT_RANK_WINDOW_SIZE;
        this.minScore = DEFAULT_MIN_SCORE;
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
