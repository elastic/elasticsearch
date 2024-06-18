/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.rerank;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.QueryPhaseRankShardContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankShardContext;
import org.elasticsearch.search.rank.rerank.RerankingQueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.rerank.RerankingQueryPhaseRankShardContext;
import org.elasticsearch.search.rank.rerank.RerankingRankFeaturePhaseRankShardContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ml.rerank.TextSimilarityRankRetrieverBuilder.FIELD_FIELD;
import static org.elasticsearch.xpack.core.ml.rerank.TextSimilarityRankRetrieverBuilder.INFERENCE_ID_FIELD;
import static org.elasticsearch.xpack.core.ml.rerank.TextSimilarityRankRetrieverBuilder.INFERENCE_TEXT_FIELD;
import static org.elasticsearch.xpack.core.ml.rerank.TextSimilarityRankRetrieverBuilder.MIN_SCORE_FIELD;

public class TextSimilarityRankBuilder extends RankBuilder {

    public static final String NAME = "text_similarity_rank_builder";

    private final String inferenceId;
    private final String inferenceText;
    private final String field;
    private final float minScore;

    public TextSimilarityRankBuilder(String field, String inferenceId, String inferenceText, int rankWindowSize, float minScore) {
        super(rankWindowSize);
        this.inferenceId = inferenceId;
        this.inferenceText = inferenceText;
        this.field = field;
        this.minScore = minScore;
    }

    public TextSimilarityRankBuilder(StreamInput in) throws IOException {
        super(in);
        this.inferenceId = in.readString();
        this.inferenceText = in.readString();
        this.field = in.readString();
        this.minScore = in.readFloat();
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.TEXT_SIMILARITY_RERANKER_RETRIEVER;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(inferenceId);
        out.writeString(inferenceText);
        out.writeString(field);
        out.writeFloat(minScore);
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(INFERENCE_ID_FIELD.getPreferredName(), inferenceId);
        builder.field(INFERENCE_TEXT_FIELD.getPreferredName(), inferenceText);
        builder.field(FIELD_FIELD.getPreferredName(), field);
        builder.field(MIN_SCORE_FIELD.getPreferredName(), minScore);
    }

    @Override
    public boolean isCompoundBuilder() {
        return false;
    }

    @Override
    public Explanation explainHit(Explanation baseExplanation, RankDoc scoreDoc, List<String> queryNames) {
        return null;
    }

    @Override
    public QueryPhaseRankShardContext buildQueryPhaseShardContext(List<Query> queries, int from) {
        return new RerankingQueryPhaseRankShardContext(queries, rankWindowSize());
    }

    @Override
    public QueryPhaseRankCoordinatorContext buildQueryPhaseCoordinatorContext(int size, int from) {
        return new RerankingQueryPhaseRankCoordinatorContext(rankWindowSize());
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
