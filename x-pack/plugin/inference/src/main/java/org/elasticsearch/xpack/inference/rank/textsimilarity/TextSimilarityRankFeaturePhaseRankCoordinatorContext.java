/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;
import org.elasticsearch.xpack.inference.services.cohere.rerank.CohereRerankTaskSettings;
import org.elasticsearch.xpack.inference.services.googlevertexai.rerank.GoogleVertexAiRerankTaskSettings;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.ClientHelper.INFERENCE_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * A {@code RankFeaturePhaseRankCoordinatorContext} that performs a rerank inference call to determine relevance scores for documents within
 * the provided rank window.
 */
public class TextSimilarityRankFeaturePhaseRankCoordinatorContext extends RankFeaturePhaseRankCoordinatorContext {

    protected final Client client;
    protected final String inferenceId;
    protected final String inferenceText;
    protected final Float minScore;

    public TextSimilarityRankFeaturePhaseRankCoordinatorContext(
        int size,
        int from,
        int rankWindowSize,
        Client client,
        String inferenceId,
        String inferenceText,
        Float minScore
    ) {
        super(size, from, rankWindowSize);
        this.client = client;
        this.inferenceId = inferenceId;
        this.inferenceText = inferenceText;
        this.minScore = minScore;
    }

    @Override
    protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {
        // Wrap the provided rankListener to an ActionListener that would handle the response from the inference service
        // and then pass the results
        final ActionListener<InferenceAction.Response> inferenceListener = scoreListener.delegateFailureAndWrap((l, r) -> {
            InferenceServiceResults results = r.getResults();
            assert results instanceof RankedDocsResults;

            // Ensure we get exactly as many scores as the number of docs we passed, otherwise we may return incorrect results
            List<RankedDocsResults.RankedDoc> rankedDocs = ((RankedDocsResults) results).getRankedDocs();

            if (rankedDocs.size() != featureDocs.length) {
                l.onFailure(
                    new IllegalStateException(
                        "Reranker input document count and returned score count mismatch: ["
                            + featureDocs.length
                            + "] vs ["
                            + rankedDocs.size()
                            + "]"
                    )
                );
            } else {
                float[] scores = extractScoresFromRankedDocs(rankedDocs);
                l.onResponse(scores);
            }
        });

        // top N listener
        ActionListener<GetInferenceModelAction.Response> topNListener = scoreListener.delegateFailureAndWrap((l, r) -> {
            // The rerank inference endpoint may have an override to return top N documents only, in that case let's fail fast to avoid
            // assigning scores to the wrong input
            Integer configuredTopN = null;
            if (r.getEndpoints().isEmpty() == false
                && r.getEndpoints().get(0).getTaskSettings() instanceof CohereRerankTaskSettings cohereTaskSettings) {
                configuredTopN = cohereTaskSettings.getTopNDocumentsOnly();
            } else if (r.getEndpoints().isEmpty() == false
                && r.getEndpoints().get(0).getTaskSettings() instanceof GoogleVertexAiRerankTaskSettings googleVertexAiTaskSettings) {
                    configuredTopN = googleVertexAiTaskSettings.topN();
                }
            if (configuredTopN != null && configuredTopN < rankWindowSize) {
                l.onFailure(
                    new IllegalArgumentException(
                        "Inference endpoint ["
                            + inferenceId
                            + "] is configured to return the top ["
                            + configuredTopN
                            + "] results, but rank_window_size is ["
                            + rankWindowSize
                            + "]. Reduce rank_window_size to be less than or equal to the configured top N value."
                    )
                );
                return;
            }

            // Short circuit on empty results after request validation
            if (featureDocs.length == 0) {
                inferenceListener.onResponse(new InferenceAction.Response(new RankedDocsResults(List.of())));
            } else {
                List<String> featureData = Arrays.stream(featureDocs).map(x -> x.featureData).toList();
                InferenceAction.Request inferenceRequest = generateRequest(featureData);
                try {
                    executeAsyncWithOrigin(client, INFERENCE_ORIGIN, InferenceAction.INSTANCE, inferenceRequest, inferenceListener);
                } finally {
                    inferenceRequest.decRef();
                }
            }
        });

        GetInferenceModelAction.Request getModelRequest = new GetInferenceModelAction.Request(inferenceId, TaskType.RERANK);
        client.execute(GetInferenceModelAction.INSTANCE, getModelRequest, topNListener);
    }

    /**
     * Sorts documents by score descending and discards those with a score less than minScore.
     * @param originalDocs documents to process
     */
    @Override
    protected RankFeatureDoc[] preprocess(RankFeatureDoc[] originalDocs) {
        List<RankFeatureDoc> docs = new ArrayList<>();
        for (RankFeatureDoc doc : originalDocs) {
            if (minScore == null || doc.score >= minScore) {
                doc.score = normalizeScore(doc.score);
                docs.add(doc);
            }
        }
        docs.sort(RankFeatureDoc::compareTo);
        return docs.toArray(new RankFeatureDoc[0]);
    }

    protected InferenceAction.Request generateRequest(List<String> docFeatures) {
        return new InferenceAction.Request(
            TaskType.RERANK,
            inferenceId,
            inferenceText,
            docFeatures,
            Map.of(),
            InputType.SEARCH,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            false
        );
    }

    private float[] extractScoresFromRankedDocs(List<RankedDocsResults.RankedDoc> rankedDocs) {
        float[] scores = new float[rankedDocs.size()];
        for (RankedDocsResults.RankedDoc rankedDoc : rankedDocs) {
            scores[rankedDoc.index()] = rankedDoc.relevanceScore();
        }
        return scores;
    }

    private static float normalizeScore(float score) {
        // As some models might produce negative scores, we want to ensure that all scores will be positive
        // so we will make use of the following normalization formula:
        // score = max(score, 0) + min(exp(score), 1)
        // this will ensure that all positive scores lie in the [1, inf) range,
        // while negative values (and 0) will be shifted to (0, 1]
        return Math.max(score, 0) + Math.min((float) Math.exp(score), 1);
    }
}
