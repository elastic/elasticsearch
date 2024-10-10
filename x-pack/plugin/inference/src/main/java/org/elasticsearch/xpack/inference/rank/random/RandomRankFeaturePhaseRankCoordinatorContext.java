/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.rank.random;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.search.rank.context.RankFeaturePhaseRankCoordinatorContext;
import org.elasticsearch.search.rank.feature.RankFeatureDoc;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;

/**
 * A {@code RankFeaturePhaseRankCoordinatorContext} that performs a rerank inference call to determine relevance scores for documents within
 * the provided rank window.
 */
public class RandomRankFeaturePhaseRankCoordinatorContext extends RankFeaturePhaseRankCoordinatorContext {

    private final Integer seed;

    public RandomRankFeaturePhaseRankCoordinatorContext(int size, int from, int rankWindowSize, Integer seed) {
        super(size, from, rankWindowSize);
        this.seed = seed;
    }

    @Override
    protected void computeScores(RankFeatureDoc[] featureDocs, ActionListener<float[]> scoreListener) {
        // Generate random scores seeded by doc
        float[] scores = new float[featureDocs.length];
        for (int i = 0; i < featureDocs.length; i++) {
            RankFeatureDoc featureDoc = featureDocs[i];
            int doc = featureDoc.doc;
            long docSeed = seed != null ? seed + doc : doc;
            scores[i] = new Random(docSeed).nextFloat();
        }
        scoreListener.onResponse(scores);
    }

    /**
     * Sorts documents by score descending.
     * @param originalDocs documents to process
     */
    @Override
    protected RankFeatureDoc[] preprocess(RankFeatureDoc[] originalDocs) {
        return Arrays.stream(originalDocs)
            .sorted(Comparator.comparing((RankFeatureDoc doc) -> doc.score).reversed())
            .toArray(RankFeatureDoc[]::new);
    }

}
