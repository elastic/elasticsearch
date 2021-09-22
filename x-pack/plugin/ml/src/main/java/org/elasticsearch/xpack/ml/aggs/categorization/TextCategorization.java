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
 *  - calculating similarity between it and a new log
 *  - expanding the existing log group by adding a new log
 */
class TextCategorization implements Accountable {

    private final long id;
    // TODO Do we want to just make this native arrays?
    private final long[] categorization;
    private final long[] tokenCounts;
    private long count;

    // Used at the shard level for tracking the bucket ordinal for collecting sub aggregations
    long bucketOrd;

    TextCategorization(long[] logTokenIds, long count, long id) {
        this.id = id;
        this.categorization = logTokenIds;
        this.count = count;
        this.tokenCounts = new long[logTokenIds.length];
        Arrays.fill(this.tokenCounts, count);
    }

    public long getId() {
        return id;
    }

    long[] getCategorization() {
        return categorization;
    }

    public long getCount() {
        return count;
    }

    Similarity calculateSimilarity(long[] logEvent) {
        assert logEvent.length == this.categorization.length;
        int eqParams = 0;
        long tokenCount = 0;
        long tokensKept = 0;
        for (int i = 0; i < logEvent.length; i++) {
            if (logEvent[i] == this.categorization[i]) {
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

    void addLog(long[] logEvent, long docCount) {
        assert logEvent.length == this.categorization.length;
        for (int i = 0; i < logEvent.length; i++) {
            if (logEvent[i] != this.categorization[i]) {
                this.categorization[i] = WILD_CARD_ID;
            } else {
                tokenCounts[i] += docCount;
            }
        }
        this.count += docCount;
    }

    @Override
    public long ramBytesUsed() {
        return Long.BYTES // id
            + RamUsageEstimator.NUM_BYTES_OBJECT_REF // categorization reference
            + RamUsageEstimator.sizeOf(categorization) // categorization token Ids
            + RamUsageEstimator.NUM_BYTES_OBJECT_REF // tokenCounts reference
            + RamUsageEstimator.sizeOf(tokenCounts) // tokenCounts
            + Long.BYTES; // count
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
