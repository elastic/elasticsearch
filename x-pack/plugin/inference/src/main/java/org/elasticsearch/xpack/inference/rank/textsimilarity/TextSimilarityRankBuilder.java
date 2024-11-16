/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.license.License;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.rerank.RerankingRankFeaturePhaseRankShardContext;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankRetrieverBuilder.FIELD_FIELD;
import static org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankRetrieverBuilder.INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankRetrieverBuilder.INFERENCE_TEXT_FIELD;
import static org.elasticsearch.xpack.inference.rank.textsimilarity.TextSimilarityRankRetrieverBuilder.MIN_SCORE_FIELD;

/**
 * A {@code RankBuilder} that enables ranking with text similarity model inference. Supports parameters for configuring the inference call.
 */
public class TextSimilarityRankBuilder extends RankBuilder {

    public static final String NAME = "text_similarity_reranker";

    public static final LicensedFeature.Momentary TEXT_SIMILARITY_RERANKER_FEATURE = LicensedFeature.momentary(
        null,
        "text-similarity-reranker",
        License.OperationMode.ENTERPRISE
    );

    static final ConstructingObjectParser<TextSimilarityRankBuilder, Void> PARSER = new ConstructingObjectParser<>(NAME, args -> {
        String inferenceId = (String) args[0];
        String inferenceText = (String) args[1];
        String field = (String) args[2];
        Integer rankWindowSize = args[3] == null ? DEFAULT_RANK_WINDOW_SIZE : (Integer) args[3];
        Float minScore = (Float) args[4];

        return new TextSimilarityRankBuilder(field, inferenceId, inferenceText, rankWindowSize, minScore);
    });

    static {
        PARSER.declareString(constructorArg(), INFERENCE_ID_FIELD);
        PARSER.declareString(constructorArg(), INFERENCE_TEXT_FIELD);
        PARSER.declareString(constructorArg(), FIELD_FIELD);
        PARSER.declareInt(optionalConstructorArg(), RANK_WINDOW_SIZE_FIELD);
        PARSER.declareFloat(optionalConstructorArg(), MIN_SCORE_FIELD);
    }

    private final String inferenceId;
    private final String inferenceText;
    private final String field;
    private final Float minScore;

    public TextSimilarityRankBuilder(String field, String inferenceId, String inferenceText, int rankWindowSize, Float minScore) {
        super(rankWindowSize);
        this.inferenceId = inferenceId;
        this.inferenceText = inferenceText;
        this.field = field;
        this.minScore = minScore;
    }

    public TextSimilarityRankBuilder(StreamInput in) throws IOException {
        super(in);
        // rankWindowSize deserialization is handled by the parent class RankBuilder
        this.inferenceId = in.readString();
        this.inferenceText = in.readString();
        this.field = in.readString();
        this.minScore = in.readOptionalFloat();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_15_0;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        // rankWindowSize serialization is handled by the parent class RankBuilder
        out.writeString(inferenceId);
        out.writeString(inferenceText);
        out.writeString(field);
        out.writeOptionalFloat(minScore);
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        // rankWindowSize serialization is handled by the parent class RankBuilder
        builder.field(INFERENCE_ID_FIELD.getPreferredName(), inferenceId);
        builder.field(INFERENCE_TEXT_FIELD.getPreferredName(), inferenceText);
        builder.field(FIELD_FIELD.getPreferredName(), field);
        if (minScore != null) {
            builder.field(MIN_SCORE_FIELD.getPreferredName(), minScore);
        }
    }

    @Override
    public boolean isCompoundBuilder() {
        return false;
    }

    @Override
    public Explanation explainHit(Explanation baseExplanation, RankDoc scoreDoc, List<String> queryNames) {
        if (scoreDoc == null) {
            return baseExplanation;
        }
        if (false == baseExplanation.isMatch()) {
            return baseExplanation;
        }

        assert scoreDoc instanceof RankFeatureDoc : "ScoreDoc is not an instance of RankFeatureDoc";
        RankFeatureDoc rrfRankDoc = (RankFeatureDoc) scoreDoc;

        return Explanation.match(
            rrfRankDoc.score,
            "rank after reranking: ["
                + rrfRankDoc.rank
                + "] with score: ["
                + rrfRankDoc.score
                + "], using inference endpoint: ["
                + inferenceId
                + "] on document field: ["
                + field
                + "]",
            baseExplanation
        );
    }

    @Override
    public QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
        return null;
    }

    @Override
    public QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
        return null;
    }

    @Override
    public RankFeaturePhaseRankShardContext buildRankFeaturePhaseShardContext() {
        return new RerankingRankFeaturePhaseRankShardContext(field);
    }

    @Override
    public RankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(int size, int from, Client client) {
        return new TextSimilarityRankFeaturePhaseRankCoordinatorContext(
            size,
            from,
            rankWindowSize(),
            client,
            inferenceId,
            inferenceText,
            minScore
        );
    }

    public String field() {
        return field;
    }

    public String inferenceId() {
        return inferenceId;
    }

    public String inferenceText() {
        return inferenceText;
    }

    public Float minScore() {
        return minScore;
    }

    @Override
    protected boolean doEquals(RankBuilder other) {
        TextSimilarityRankBuilder that = (TextSimilarityRankBuilder) other;
        return Objects.equals(inferenceId, that.inferenceId)
            && Objects.equals(inferenceText, that.inferenceText)
            && Objects.equals(field, that.field)
            && Objects.equals(minScore, that.minScore);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(inferenceId, inferenceText, field, minScore);
    }
}
