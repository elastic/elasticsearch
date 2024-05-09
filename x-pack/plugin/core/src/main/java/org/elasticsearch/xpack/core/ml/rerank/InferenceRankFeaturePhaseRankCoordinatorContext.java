/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.rerank;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.search.rank.rerank.RerankingRankFeaturePhaseRankCoordinatorContext;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

public abstract class InferenceRankFeaturePhaseRankCoordinatorContext<Request extends ActionRequest, Response extends ActionResponse>
    extends RerankingRankFeaturePhaseRankCoordinatorContext {

    private static final Logger logger = LogManager.getLogger(InferenceRankFeaturePhaseRankCoordinatorContext.class);
    protected final String inferenceId;
    protected final String inferenceText;

    protected final Client client;

    public InferenceRankFeaturePhaseRankCoordinatorContext(
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

    protected abstract Request request(List<String> docFeatures);

    protected abstract ActionType<Response> action();

    protected abstract void processResponse(Response response, BiConsumer<Integer, Float> scoreConsumer);

    @Override
    protected void computeScores(RankFeatureDoc[] featureDocs, BiConsumer<Integer, Float> scoreConsumer, Runnable onFinish) {
        List<String> features = Arrays.stream(featureDocs).map(x -> x.featureData).toList();
        final ActionListener<Response> actionListener = listener(scoreConsumer, onFinish);
        Request req = request(features);
        ActionType<Response> action = action();
        client.execute(action, req, actionListener);
    }

    private ActionListener<Response> listener(BiConsumer<Integer, Float> scoreConsumer, Runnable onFinish) {
        return new ActionListener<>() {
            @Override
            public void onResponse(Response response) {
                try {
                    if (response != null) {
                        processResponse(response, scoreConsumer);
                    }
                } finally {
                    if (response != null) {
                        response.decRef();
                    }
                    onFinish.run();

                }
            }

            @Override
            public void onFailure(Exception e) {
                try {
                    logger.error("Failed to process inference response", e);
                } finally {
                    onFinish.run();
                }
            }
        };
    }
}
