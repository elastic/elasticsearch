/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;

import java.util.Arrays;

import static org.elasticsearch.xpack.ml.aggs.categorization.CategorizationBytesRefHash.WILD_CARD_ID;

/**
 * A text categorization group that provides methods for:
 *  - calculating similarity between it and a new text
 *  - expanding the existing categorization by adding a new array of tokens
 */
class TextCategorization implements Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(TextCategorization.class);
    private final long id;
    private final int[] categorization;
    private final long[] tokenCounts;
    private long count;

    // Used at the shard level for tracking the bucket ordinal for collecting sub aggregations
    long bucketOrd;

    TextCategorization(int[] tokenIds, long count, long id) {
        this.id = id;
        this.categorization = tokenIds;
        this.count = count;
        this.tokenCounts = new long[tokenIds.length];
        Arrays.fill(this.tokenCounts, count);
    }

    public long getId() {
        return id;
    }

    int[] getCategorization() {
        return categorization;
    }

    public long getCount() {
        return count;
    }

    Similarity calculateSimilarity(int[] tokenIds) {
        assert tokenIds.length == this.categorization.length;
        int eqParams = 0;
        long tokenCount = 0;
        long tokensKept = 0;
        for (int i = 0; i < tokenIds.length; i++) {
            if (tokenIds[i] == this.categorization[i]) {
                tokensKept += tokenCounts[i];
                tokenCount += tokenCounts[i];
            } else if (this.categorization[i] == WILD_CARD_ID) {
                eqParams++;
            } else {
                tokenCount += tokenCounts[i];
            }
        }
        return new Similarity((double) tokensKept / tokenCount, eqParams);
    }

    void addTokens(int[] tokenIds, long docCount) {
        assert tokenIds.length == this.categorization.length;
        for (int i = 0; i < tokenIds.length; i++) {
            if (tokenIds[i] != this.categorization[i]) {
                this.categorization[i] = WILD_CARD_ID;
            } else {
                tokenCounts[i] += docCount;
            }
        }
        this.count += docCount;
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(categorization) // categorization token Ids
            + RamUsageEstimator.sizeOf(tokenCounts); // tokenCounts
    }

    static class Similarity implements Comparable<Similarity> {
        private final double similarity;
        private final int wildCardCount;

        private Similarity(double similarity, int wildCardCount) {
            this.similarity = similarity;
            this.wildCardCount = wildCardCount;
        }

        @Override
        public int compareTo(Similarity o) {
            int d = Double.compare(similarity, o.similarity);
            if (d != 0) {
                return d;
            }
            return Integer.compare(wildCardCount, o.wildCardCount);
        }

        public double getSimilarity() {
            return similarity;
        }

        public int getWildCardCount() {
            return wildCardCount;
        }
    }

}
