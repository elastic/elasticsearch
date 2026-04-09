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
import org.elasticsearch.inference.TopNProvider;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
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
        Float minScore,
        boolean failuresAllowed
    ) {
        super(size, from, rankWindowSize, failuresAllowed);
        this.client = client;
        this.inferenceId = inferenceId;
        this.inferenceText = inferenceText;
        this.minScore = minScore;
    }

    @Override
    protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {
        // This method relies on callers filtering out feature docs with null feature data
        assert Arrays.stream(featureDocs).noneMatch(featureDoc -> featureDoc.featureData == null);

        ActionListener<InferenceAction.Response> inferenceListener = scoreListener.delegateFailureAndWrap((l, r) -> {
            InferenceServiceResults results = r.getResults();
            if (results instanceof RankedDocsResults rankedDocsResults) {
                l.onResponse(extractScoresFromRankedDocs(rankedDocsResults.getRankedDocs(), featureDocs));
            } else {
                throw new IllegalStateException(
                    "Expected results to be of type [" + RankedDocsResults.class + "], got [" + results.getClass() + "]"
                );
            }
        });

        ActionListener<GetInferenceModelAction.Response> topNListener = scoreListener.delegateFailureAndWrap((l, r) -> {
            Integer configuredTopN = null;
            if (r.getEndpoints().isEmpty() == false) {
                var taskSettings = r.getEndpoints().get(0).getTaskSettings();
                if (taskSettings instanceof TopNProvider topNProvider) {
                    configuredTopN = topNProvider.getTopN();
                }
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

            // Short circuit on no docs to rerank
            if (featureDocs.length == 0) {
                inferenceListener.onResponse(new InferenceAction.Response(new RankedDocsResults(List.of())));
            } else {
                List<String> inferenceInputs = Arrays.stream(featureDocs).flatMap(featureDoc -> featureDoc.featureData.stream()).toList();
                InferenceAction.Request inferenceRequest = generateRequest(inferenceInputs);
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
     *
     * @param originalDocs   documents to process
     * @param rerankedScores {@code true} if the document scores have been reranked
     */
    @Override
    protected RankFeatureDoc[] preprocess(RankFeatureDoc[] originalDocs, boolean rerankedScores) {
        if (rerankedScores == false) {
            // just return, don't normalize or apply minScore to scores that haven't been modified
            return originalDocs;
        }
        List<RankFeatureDoc> docs = new ArrayList<>(originalDocs.length);
        for (RankFeatureDoc doc : originalDocs) {
            if (minScore == null || doc.score >= minScore) {
                doc.score = normalizeScore(doc.score);
                docs.add(doc);
            }
        }
        docs.sort(null);
        return docs.toArray(RankFeatureDoc[]::new);
    }

    protected InferenceAction.Request generateRequest(List<String> docFeatures) {
        return new InferenceAction.Request(
            TaskType.RERANK,
            inferenceId,
            inferenceText,
            null,
            null,
            docFeatures,
            Map.of(),
            InputType.INTERNAL_SEARCH,
            InferenceAction.Request.DEFAULT_TIMEOUT,
            false
        );
    }

    static float[] extractScoresFromRankedDocs(List<RankedDocsResults.RankedDoc> rankedDocs, RankFeatureDoc[] featureDocs) {
        Map<Integer, Float> scores = new HashMap<>();

        // Feature docs can be composed of multiple features (when chunking is applied, for instance), each of which is transformed into a
        // separate ranked doc by the reranker service. Build a data structure that allows us to map the ranked doc index to the feature
        // doc index. An array-backed list works for this purpose. The list index serves as the ranked doc index and the value serves as the
        // feature doc index.
        List<Integer> rankedDocToFeatureDoc = new ArrayList<>();
        for (int i = 0; i < featureDocs.length; i++) {
            RankFeatureDoc featureDoc = featureDocs[i];
            if (featureDoc.featureData.isEmpty()) {
                throw new IllegalStateException("Feature doc at index " + i + " does not have any features");
            }

            for (int j = 0; j < featureDoc.featureData.size(); j++) {
                rankedDocToFeatureDoc.add(i);
            }
        }

        if (rankedDocToFeatureDoc.size() != rankedDocs.size()) {
            throw new IllegalStateException(
                "Expected ranked doc size to be "
                    + rankedDocToFeatureDoc.size()
                    + ", got "
                    + rankedDocs.size()
                    + ". Is the reranker service using an unreported top N task setting?"
            );
        }

        for (RankedDocsResults.RankedDoc rankedDoc : rankedDocs) {
            int featureDocIndex = rankedDocToFeatureDoc.get(rankedDoc.index());
            float score = rankedDoc.relevanceScore();
            scores.compute(featureDocIndex, (k, v) -> v == null ? score : Math.max(v, score));
        }

        float[] result = new float[featureDocs.length];
        for (int i = 0; i < featureDocs.length; i++) {
            result[i] = scores.get(i);
        }

        return result;
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
