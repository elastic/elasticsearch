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

    /**
     * Creates a scoring listener that uses the resolved chunking config for proper chunking behavior.
     */
    private ActionListener<InferenceAction.Response> createScoringListener(
        RankFeatureDoc[] featureDocs,
        ActionListener<float[]> scoreListener,
        ChunkScorerConfig resolvedConfig
    ) {
        return scoreListener.delegateFailureAndWrap((l, r) -> {
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
                if (resolvedConfig != null) {
                    scores = extractScoresFromRankedChunks(rankedDocs, featureDocs);
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
    }

    @Override
    protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {

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

            // Resolve chunking settings if missing
            ChunkScorerConfig resolvedChunkScorerConfig = resolveChunkingSettings(r);
            if (resolvedChunkScorerConfig == null) {
                l.onFailure(new IllegalArgumentException("Failed to resolve chunking settings for chunk_rescorer"));
                return;
            }

            // Update the chunkScorerConfig with resolved settings for use in shard operations
            // This ensures shards receive the correct chunking settings from the inference endpoint
            // Create the scoring listener with the resolved config
            ActionListener<InferenceAction.Response> inferenceListener = createScoringListener(
                featureDocs,
                scoreListener,
                resolvedChunkScorerConfig
            );

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

    /**
     * Resolves chunking settings for chunk_rescorer based on inference endpoint configuration.
     * Precedence:
     * 1. If chunking_settings are explicitly provided, use them
     * 2. If chunk_rescorer.inference_id is specified, load settings from that endpoint
     * 3. If only reranker inference_id is available, try to load settings from that endpoint
     * 4. If no settings found when explicitly requested, return null (will cause error)
     */
    private ChunkScorerConfig resolveChunkingSettings(GetInferenceModelAction.Response response) {
        // If chunking_settings are already provided, use them
        if (chunkScorerConfig.chunkingSettings() != null) {
            return chunkScorerConfig;
        }

        // Determine which inference_id to use for chunking settings
        String targetInferenceId = chunkScorerConfig.inferenceId();
        if (targetInferenceId == null) {
            targetInferenceId = inferenceId; // fall back to reranker inference_id
        }

        // If no inference_id available, keep current config (with defaults)
        if (targetInferenceId == null) {
            return chunkScorerConfig;
        }

        // Look for chunking settings in the response
        if (response.getEndpoints().isEmpty()) {
            return chunkScorerConfig; // no endpoints, keep defaults
        }

        // Check if the target inference_id matches the current response
        if (targetInferenceId.equals(inferenceId)) {
            // Use current response
            return extractChunkingSettingsFromResponse(response);
        } else {
            // Need to fetch the specific inference_id - for now, return current config
            // This would require an additional async call, which we'll implement later
            return chunkScorerConfig;
        }
    }

    private ChunkScorerConfig extractChunkingSettingsFromResponse(GetInferenceModelAction.Response response) {
        for (var endpoint : response.getEndpoints()) {
            ChunkingSettings endpointChunkingSettings = endpoint.getChunkingSettings();
            if (endpointChunkingSettings != null) {
                // Create new config with resolved chunking settings
                return new ChunkScorerConfig(
                    chunkScorerConfig.size(),
                    chunkScorerConfig.inferenceId(),
                    chunkScorerConfig.inferenceText(),
                    endpointChunkingSettings
                );
            }
        }

        // If chunk_rescorer.inference_id was explicitly specified but no settings found, return null to trigger error
        if (chunkScorerConfig.inferenceId() != null) {
            return null;
        }

        // Otherwise, keep current config (with defaults)
        return chunkScorerConfig;
    }
}
