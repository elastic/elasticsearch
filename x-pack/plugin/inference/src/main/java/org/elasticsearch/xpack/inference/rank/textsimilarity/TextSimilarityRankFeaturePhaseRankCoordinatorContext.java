/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.textsimilarity;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.core.Nullable;
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
import org.elasticsearch.xpack.inference.services.huggingface.rerank.HuggingFaceRerankTaskSettings;

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
    protected final SnippetConfig snippetConfig;

    public TextSimilarityRankFeaturePhaseRankCoordinatorContext(
        int size,
        int from,
        int rankWindowSize,
        Client client,
        String inferenceId,
        String inferenceText,
        Float minScore,
        boolean failuresAllowed,
        @Nullable SnippetConfig snippetConfig
    ) {
        super(size, from, rankWindowSize, failuresAllowed);
        this.client = client;
        this.inferenceId = inferenceId;
        this.inferenceText = inferenceText;
        this.minScore = minScore;
        this.snippetConfig = snippetConfig;
    }

    @Override
    protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {

        // Wrap the provided rankListener to an ActionListener that would handle the response from the inference service
        // and then pass the results
        final ActionListener<InferenceAction.Response> inferenceListener = scoreListener.delegateFailureAndWrap((l, r) -> {
            InferenceServiceResults results = r.getResults();
            assert results instanceof RankedDocsResults;

            // If we have an empty list of ranked docs, simply return the original scores
            List<RankedDocsResults.RankedDoc> rankedDocs = ((RankedDocsResults) results).getRankedDocs();
            if (rankedDocs.isEmpty()) {
                float[] originalScores = new float[featureDocs.length];
                for (int i = 0; i < featureDocs.length; i++) {
                    originalScores[i] = featureDocs[i].score;
                }
                l.onResponse(originalScores);
            } else {
                final float[] scores;
                if (this.snippetConfig != null) {
                    scores = extractScoresFromRankedSnippets(rankedDocs, featureDocs);
                } else {
                    scores = extractScoresFromRankedDocs(rankedDocs);
                }

                // Ensure we get exactly as many final scores as the number of docs we passed, otherwise we may return incorrect results
                if (scores.length != featureDocs.length) {
                    l.onFailure(
                        new IllegalStateException(
                            "Reranker input document count and returned score count mismatch: ["
                                + featureDocs.length
                                + "] vs ["
                                + scores.length
                                + "]"
                        )
                    );
                } else {
                    l.onResponse(scores);
                }
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
                } else if (r.getEndpoints().isEmpty() == false
                    && r.getEndpoints().get(0).getTaskSettings() instanceof HuggingFaceRerankTaskSettings huggingFaceRerankTaskSettings) {
                        configuredTopN = huggingFaceRerankTaskSettings.getTopNDocumentsOnly();
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
                List<String> inferenceInputs = Arrays.stream(featureDocs)
                    .filter(featureDoc -> featureDoc.featureData != null)
                    .flatMap(featureDoc -> featureDoc.featureData.stream())
                    .toList();
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

    float[] extractScoresFromRankedDocs(List<RankedDocsResults.RankedDoc> rankedDocs) {
        float[] scores = new float[rankedDocs.size()];
        for (RankedDocsResults.RankedDoc rankedDoc : rankedDocs) {
            scores[rankedDoc.index()] = rankedDoc.relevanceScore();
        }
        return scores;
    }

    float[] extractScoresFromRankedSnippets(List<RankedDocsResults.RankedDoc> rankedDocs, RankFeatureDoc[] featureDocs) {
        float[] scores = new float[featureDocs.length];
        boolean[] hasScore = new boolean[featureDocs.length];

        // We need to correlate the index/doc values of each RankedDoc in correlation with its associated RankFeatureDoc.
        int[] rankedDocToFeatureDoc = Arrays.stream(featureDocs)
            .flatMapToInt(
                doc -> java.util.stream.IntStream.generate(() -> Arrays.asList(featureDocs).indexOf(doc)).limit(doc.featureData.size())
            )
            .limit(rankedDocs.size())
            .toArray();

        for (RankedDocsResults.RankedDoc rankedDoc : rankedDocs) {
            int docId = rankedDocToFeatureDoc[rankedDoc.index()];
            float score = rankedDoc.relevanceScore();
            scores[docId] = hasScore[docId] == false ? score : Math.max(scores[docId], score);
            hasScore[docId] = true;
        }

        float[] result = new float[featureDocs.length];
        for (int i = 0; i < featureDocs.length; i++) {
            result[i] = hasScore[i] ? scores[i] : 0f;
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
