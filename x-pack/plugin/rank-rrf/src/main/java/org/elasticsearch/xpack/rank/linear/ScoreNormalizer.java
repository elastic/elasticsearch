/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.linear;

import org.apache.lucene.search.ScoreDoc;

/**
 * A no-op {@link ScoreNormalizer} that does not modify the scores.
 */
public abstract class ScoreNormalizer {

    public static ScoreNormalizer valueOf(String normalizer) {
        if (MinMaxScoreNormalizer.NAME.equalsIgnoreCase(normalizer)) {
            return MinMaxScoreNormalizer.INSTANCE;
        } else if (L2ScoreNormalizer.NAME.equalsIgnoreCase(normalizer)) {
            return L2ScoreNormalizer.INSTANCE;

        } else if (IdentityScoreNormalizer.NAME.equalsIgnoreCase(normalizer)) {
            return IdentityScoreNormalizer.INSTANCE;

        } else {
            throw new IllegalArgumentException("Unknown normalizer [" + normalizer + "]");
        }
    }

    public abstract String getName();

    public abstract ScoreDoc[] normalizeScores(ScoreDoc[] docs);
}
