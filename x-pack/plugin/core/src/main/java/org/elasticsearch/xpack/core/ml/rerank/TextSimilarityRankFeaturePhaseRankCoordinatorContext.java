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
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

public class TextSimilarityRankFeaturePhaseRankCoordinatorContext extends InferenceRankFeaturePhaseRankCoordinatorContext<
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
    protected InferenceAction.Request request(List<String> docFeatures) {
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
    protected ActionType<InferenceAction.Response> action() {
        return InferenceAction.INSTANCE;
    }

    @Override
    protected void processResponse(InferenceAction.Response response, BiConsumer<Integer, Float> scoreConsumer) {
        InferenceServiceResults results = response.getResults();
        assert results instanceof RankedDocsResults;
        ((RankedDocsResults) results).getRankedDocs()
            .stream()
            .sorted(Comparator.comparingInt(RankedDocsResults.RankedDoc::index))
            .forEach((doc) -> scoreConsumer.accept(doc.index(), doc.relevanceScore() >= minScore ? doc.relevanceScore() : DROP_SCORE));
    }
}
