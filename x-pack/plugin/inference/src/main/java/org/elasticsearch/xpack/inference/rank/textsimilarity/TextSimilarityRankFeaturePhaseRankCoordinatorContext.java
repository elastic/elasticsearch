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
import org.elasticsearch.xpack.core.inference.action.InferenceAction;
import org.elasticsearch.xpack.core.inference.results.RankedDocsResults;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

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
        final ActionListener<InferenceAction.Response> actionListener = scoreListener.delegateFailureAndWrap((l, r) -> {
            InferenceServiceResults results = r.getResults();
            assert results instanceof RankedDocsResults;

            // Ensure we get exactly as many scores as the number of docs we passed, otherwise we may return incorrect results
            List<RankedDocsResults.RankedDoc> rankedDocs = ((RankedDocsResults) results).getRankedDocs();
            if (rankedDocs.size() != featureDocs.length) {
                l.onFailure(
                    new IllegalStateException(
                        "Document and score count mismatch: ["
                            + featureDocs.length
                            + "] vs ["
                            + rankedDocs.size()
                            + "]. Check your rerank inference endpoint configuration and ensure it returns rank_window_size scores for "
                            + "rank_window_size input documents."
                    )
                );
            } else {
                float[] scores = extractScoresFromRankedDocs(rankedDocs);
                l.onResponse(scores);
            }
        });

        List<String> featureData = Arrays.stream(featureDocs).map(x -> x.featureData).toList();
        InferenceAction.Request request = generateRequest(featureData);
        try {
            client.execute(InferenceAction.INSTANCE, request, actionListener);
        } finally {
            request.decRef();
        }
    }

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

    private float[] extractScoresFromRankedDocs(List<RankedDocsResults.RankedDoc> rankedDocs) {
        float[] scores = new float[rankedDocs.size()];
        for (RankedDocsResults.RankedDoc rankedDoc : rankedDocs) {
            scores[rankedDoc.index()] = rankedDoc.relevanceScore();
        }

        return scores;
    }

    /**
     * Sorts documents by score descending and discards those with a score less than minScore.
     * @param originalDocs documents to process
     */
    @Override
    protected RankFeatureDoc[] preprocess(RankFeatureDoc[] originalDocs) {
        return Arrays.stream(originalDocs)
            .filter(doc -> minScore == null || doc.score >= minScore)
            .sorted(Comparator.comparing((RankFeatureDoc doc) -> doc.score).reversed())
            .toArray(RankFeatureDoc[]::new);
    }
}
