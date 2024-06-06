/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.rerank;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.search.rank.rerank.ActionRequestRankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.List;
import java.util.Map;

public class TextSimilarityRankFeaturePhaseRankCoordinatorContext extends ActionRequestRankFeaturePhaseRankCoordinatorContext<
    InferenceAction.Request,
    InferenceAction.Response> {

    /**
     * Negative relevance score for marking docs that should be dropped because their score does not reach minScore.
     */
    private static final float DROP_SCORE = -1.0f;

    private final float minScore;

    public TextSimilarityRankFeaturePhaseRankCoordinatorContext(
        int size,
        int from,
        int windowSize,
        Client client,
        String inferenceId,
        String inferenceText,
        float minScore
    ) {
        super(size, from, windowSize, client, inferenceId, inferenceText);

        this.minScore = minScore;
    }

    @Override
    protected InferenceAction.Request generateRequest(List<String> docFeatures) {
        return new InferenceAction.Request(
            TaskType.RERANK,
            inferenceId,
            inferenceText,
            docFeatures,
            Map.of(),
            InputType.SEARCH,
            InferenceAction.Request.DEFAULT_TIMEOUT
        );
    }

    @Override
    protected ActionType<InferenceAction.Response> actionType() {
        return InferenceAction.INSTANCE;
    }

    @Override
    protected float[] extractScoresFromResponse(InferenceAction.Response response) {
        InferenceServiceResults results = response.getResults();
        assert results instanceof RankedDocsResults;

        List<RankedDocsResults.RankedDoc> rankedDocs = ((RankedDocsResults) results).getRankedDocs();
        float[] scores = new float[rankedDocs.size()];
        for (RankedDocsResults.RankedDoc rankedDoc : rankedDocs) {
            scores[rankedDoc.index()] = rankedDoc.relevanceScore();
        }

        return scores;
    }
}
