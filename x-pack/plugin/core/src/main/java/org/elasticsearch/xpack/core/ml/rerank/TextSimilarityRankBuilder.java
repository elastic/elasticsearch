/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.rerank;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.rank.RankBuilder;
import org.elasticsearch.search.rank.rerank.RerankingRankBuilder;
import org.elasticsearch.search.rank.rerank.RerankingRankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

// RankBuilder is passed to shards in the query phase
// NEW: also in fetch (or rank) phase -> access to request context, rank builder in it
// NEW: rankRankShardContext, rankRankCoordinatorContext
// Rank phase -> 1. extract field for inference (rankRankShardContext), 2. run inference (rankRankCoordinatorContext)
// Top K trimming at query phase

public class TextSimilarityRankBuilder extends RerankingRankBuilder {

    private final String inferenceId;
    private final String inferenceText;
    private final float minScore;

    public TextSimilarityRankBuilder(String field, String inferenceId, String inferenceText, int windowSize, float minScore) {
        super(windowSize, field);
        this.inferenceId = inferenceId;
        this.inferenceText = inferenceText;
        this.minScore = minScore;
    }

    public TextSimilarityRankBuilder(StreamInput in) throws IOException {
        super(in);
        this.inferenceId = in.readString();
        this.inferenceText = in.readString();
        this.minScore = in.readFloat();
    }

    @Override
    public String getWriteableName() {
        return "text_similarity_rank_builder";
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.V_8_11_X;
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        out.writeString(inferenceId);
        out.writeString(inferenceText);
    }

    @Override
    public void doXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public RerankingRankFeaturePhaseRankCoordinatorContext buildRankFeaturePhaseCoordinatorContext(int size, int from, Client client) {
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
        return false;
    }

    @Override
    protected int doHashCode() {
        return 0;
    }

}
