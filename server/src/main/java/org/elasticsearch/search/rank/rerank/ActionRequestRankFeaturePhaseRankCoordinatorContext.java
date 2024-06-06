/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank.rerank;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;

import java.util.Arrays;
import java.util.List;

/**
 * A global reranker that computes the updated scores for the top-K results from all shards, through an inference service.
 * This coordinator context, that's executed at the end of the {@link org.elasticsearch.action.search.RankFeaturePhase},
 * needs an {@code inferenceId} and {@code inferenceText} to generate the request to the inference service
 * and then iterates over the results to update the scores in-place.
 * The {@code Client} should ensure that this is done in an efficient non-blocking manner,
 * and then pass the results back through the provided {@code ActionListener}.
 * Each implementation of this class needs to define the request generation logic, the action type, and how to actually read the scores
 * from the service's response.
 */
public abstract class ActionRequestRankFeaturePhaseRankCoordinatorContext<Request extends ActionRequest, Response extends ActionResponse>
    extends RankFeaturePhaseRankCoordinatorContext {

    protected final String inferenceId;
    protected final String inferenceText;

    protected final Client client;

    public ActionRequestRankFeaturePhaseRankCoordinatorContext(
        int size,
        int from,
        int windowSize,
        Client client,
        String inferenceId,
        String inferenceText
    ) {
        super(size, from, windowSize);
        this.client = client;
        this.inferenceId = inferenceId;
        this.inferenceText = inferenceText;
    }

    /**
     * This generates the appropriate {@code Request} to be passed to the {@code Client} for making a proper call
     * to the specified inference service.
     */
    protected abstract Request generateRequest(List<String> docFeatures);

    /**
     * The {@code ActionType} that is used to make the call to the inference service.
     */
    protected abstract ActionType<Response> actionType();

    /**
     * This method is responsible for extracting the scores from the response of the inference service.
     * This should return a {@code float[]} whose length should be equal to the number of documents provided in the request, where
     * {@code scores[i]} should be the score computed for document at position {@code i} in {@code featureDocs}.
     */
    protected abstract float[] extractScoresFromResponse(Response response);

    protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {
        // Wrap the provided rankListener to an ActionListener that would handle the response from the inference service
        // and then pass the results
        final ActionListener<Response> actionListener = scoreListener.delegateFailureAndWrap((l, r) -> {
            float[] scores = extractScoresFromResponse(r);
            assert scores.length == featureDocs.length;
            l.onResponse(scores);
        });

        List<String> featureData = Arrays.stream(featureDocs).map(x -> x.featureData).toList();
        Request request = generateRequest(featureData);
        try {
            ActionType<Response> action = actionType();
            client.execute(action, request, actionListener);
        } finally {
            if (request != null) {
                request.decRef();
            }
        }
    }
}
