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
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

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

    private final String inferenceId;
    private final String inferenceText;
    private final String field;
    private final Float minScore;
    private final boolean failuresAllowed;

    public TextSimilarityRankBuilder(
        String field,
        String inferenceId,
        String inferenceText,
        int rankWindowSize,
        Float minScore,
        boolean failuresAllowed
    ) {
        super(rankWindowSize);
        this.inferenceId = inferenceId;
        this.inferenceText = inferenceText;
        this.field = field;
        this.minScore = minScore;
        this.failuresAllowed = failuresAllowed;
    }

    public TextSimilarityRankBuilder(StreamInput in) throws IOException {
        super(in);
        // rankWindowSize deserialization is handled by the parent class RankBuilder
        this.inferenceId = in.readString();
        this.inferenceText = in.readString();
        this.field = in.readString();
        this.minScore = in.readOptionalFloat();
        if (in.getTransportVersion().onOrAfter(TransportVersions.RERANKER_FAILURES_ALLOWED)) {
            this.failuresAllowed = in.readBoolean();
        } else {
            this.failuresAllowed = false;
        }
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
        if (out.getTransportVersion().onOrAfter(TransportVersions.RERANKER_FAILURES_ALLOWED)) {
            out.writeBoolean(failuresAllowed);
        }
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException("This should not be XContent serialized");
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
            minScore,
            failuresAllowed
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

    public boolean failuresAllowed() {
        return failuresAllowed;
    }

    @Override
    protected boolean doEquals(RankBuilder other) {
        TextSimilarityRankBuilder that = (TextSimilarityRankBuilder) other;
        return Objects.equals(inferenceId, that.inferenceId)
            && Objects.equals(inferenceText, that.inferenceText)
            && Objects.equals(field, that.field)
            && Objects.equals(minScore, that.minScore)
            && failuresAllowed == that.failuresAllowed;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(inferenceId, inferenceText, field, minScore, failuresAllowed);
    }
}
