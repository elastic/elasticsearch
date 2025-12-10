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
import org.elasticsearch.inference.ChunkingSettings;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.inference.InputType;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;
import org.elasticsearch.xpack.core.inference.action.GetInferenceModelAction;
import org.elasticsearch.xpack.core.inference.action.GetRerankerWindowSizeAction;
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
    protected final ChunkScorerConfig chunkScorerConfig;

    public TextSimilarityRankFeaturePhaseRankCoordinatorContext(
        int size,
        int from,
        int rankWindowSize,
        Client client,
        String inferenceId,
        String inferenceText,
        Float minScore,
        boolean failuresAllowed,
        @Nullable ChunkScorerConfig chunkScorerConfig
    ) {
        super(size, from, rankWindowSize, failuresAllowed);
        this.client = client;
        this.inferenceId = inferenceId;
        this.inferenceText = inferenceText;
        this.minScore = minScore;
        this.chunkScorerConfig = chunkScorerConfig;
    }

    @Override
    protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {

        ActionListener<ChunkScorerConfig> resolvedConfigListener = scoreListener.delegateFailureAndWrap((l, resolvedChunkScorerConfig) -> {
            // Wrap the provided rankListener to an ActionListener that would handle the response from the inference service
            // and then pass the results
            ActionListener<InferenceAction.Response> inferenceListener = scoreListener.delegateFailureAndWrap((l2, r2) -> {
                InferenceServiceResults results = r2.getResults();
                assert results instanceof RankedDocsResults;

                // If we have an empty list of ranked docs, simply return the original scores
                List<RankedDocsResults.RankedDoc> rankedDocs = ((RankedDocsResults) results).getRankedDocs();
                if (rankedDocs.isEmpty()) {
                    float[] originalScores = new float[featureDocs.length];
                    for (int i = 0; i < featureDocs.length; i++) {
                        originalScores[i] = featureDocs[i].score;
                    }
                    l2.onResponse(originalScores);
                } else {
                    final float[] scores;
                    if (resolvedChunkScorerConfig != null) {
                        scores = extractScoresFromRankedChunks(rankedDocs, featureDocs);
                    } else {
                        scores = extractScoresFromRankedDocs(rankedDocs);
                    }

                    // Ensure we get exactly as many final scores as the number of docs we passed, otherwise we may return incorrect results
                    if (scores.length != featureDocs.length) {
                        l2.onFailure(
                            new IllegalStateException(
                                "Reranker input document count and returned score count mismatch: ["
                                    + featureDocs.length
                                    + "] vs ["
                                    + scores.length
                                    + "]"
                            )
                        );
                    } else {
                        l2.onResponse(scores);
                    }
                }
            });

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

        ActionListener<GetInferenceModelAction.Response> topNListener = scoreListener.delegateFailureAndWrap((l, r) -> {
            Integer configuredTopN = null;
            if (r.getEndpoints().isEmpty() == false) {
                var taskSettings = r.getEndpoints().get(0).getTaskSettings();
                if (taskSettings instanceof CohereRerankTaskSettings cohere) {
                    configuredTopN = cohere.getTopNDocumentsOnly();
                } else if (taskSettings instanceof GoogleVertexAiRerankTaskSettings google) {
                    configuredTopN = google.topN();
                } else if (taskSettings instanceof HuggingFaceRerankTaskSettings hf) {
                    configuredTopN = hf.getTopNDocumentsOnly();
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

            if (chunkScorerConfig != null && chunkScorerConfig.chunkingSettings() == null) {
                GetRerankerWindowSizeAction.Request req = new GetRerankerWindowSizeAction.Request(inferenceId);
                ActionListener<GetRerankerWindowSizeAction.Response> windowSizeListener = resolvedConfigListener.map(
                    r2 -> resolveChunkingSettings(r2.getWindowSize())
                );
                client.execute(GetRerankerWindowSizeAction.INSTANCE, req, windowSizeListener);
            } else {
                resolvedConfigListener.onResponse(resolveChunkingSettings(-1));
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

    float[] extractScoresFromRankedChunks(List<RankedDocsResults.RankedDoc> rankedDocs, RankFeatureDoc[] featureDocs) {
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

    ChunkScorerConfig resolveChunkingSettings(int windowSize) {
        if (chunkScorerConfig == null) {
            return null;
        }

        if (chunkScorerConfig.chunkingSettings() != null) {
            return chunkScorerConfig;
        }

        if (windowSize <= 0) {
            throw new IllegalStateException("Unable to determine reranker window size for inference endpoint [" + inferenceId + "]");
        }
        ChunkingSettings endpointSettings = ChunkScorerConfig.defaultChunkingSettings(windowSize);
        return new ChunkScorerConfig(chunkScorerConfig.size(), chunkScorerConfig.inferenceText(), endpointSettings);
    }
}
