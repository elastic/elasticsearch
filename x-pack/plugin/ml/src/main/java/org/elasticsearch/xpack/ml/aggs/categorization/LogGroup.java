/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.categorization;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.core.Tuple;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ml.aggs.categorization.CategorizationTokenTree.WILD_CARD;

/**
 * A log group that provides methods for:
 *  - calculating similarity between it and a new log
 *  - expanding the existing log group by adding a new log
 */
class LogGroup implements Accountable {

    private final long id;
    private final BytesRef[] logEvent;
    private final long[] tokenCounts;
    private long count;

    // Used at the shard level for tracking the bucket ordinal for collecting sub aggregations
    long bucketOrd;

    @Override
    public String toString() {
        return "LogGroup{"
            + "id="
            + id
            + ", logEvent="
            + Arrays.stream(logEvent).map(BytesRef::utf8ToString).collect(Collectors.joining(", ", "[", "]"))
            + ", count="
            + count
            + '}';
    }

    LogGroup(BytesRef[] logTokens, long count, long id) {
        this.id = id;
        this.logEvent = logTokens;
        this.count = count;
        this.tokenCounts = new long[logTokens.length];
        Arrays.fill(this.tokenCounts, count);
    }

    public long getId() {
        return id;
    }

    BytesRef[] getLogEvent() {
        return logEvent;
    }

    public long getCount() {
        return count;
    }

    Tuple<Double, Integer> calculateSimilarity(BytesRef[] logEvent) {
        assert logEvent.length == this.logEvent.length;
        int eqParams = 0;
        long tokenCount = 0;
        long tokensKept = 0;
        for (int i = 0; i < logEvent.length; i++) {
            if (logEvent[i].equals(this.logEvent[i])) {
                tokensKept += tokenCounts[i];
                tokenCount += tokenCounts[i];
            } else if (this.logEvent[i].equals(WILD_CARD)) {
                eqParams++;
            } else {
                tokenCount += tokenCounts[i];
            }
        }
        return new Tuple<>((double) tokensKept / tokenCount, eqParams);
    }

    void addLog(BytesRef[] logEvent, long docCount) {
        assert logEvent.length == this.logEvent.length;
        for (int i = 0; i < logEvent.length; i++) {
            if (logEvent[i].equals(this.logEvent[i]) == false) {
                this.logEvent[i] = WILD_CARD;
            } else {
                tokenCounts[i] += docCount;
            }
        }
        this.count += docCount;
    }

    @Override
    public long ramBytesUsed() {
        return Long.BYTES // id
            + (long) logEvent.length * RamUsageEstimator.NUM_BYTES_ARRAY_HEADER // logEvent
            + RamUsageEstimator.NUM_BYTES_OBJECT_REF + ((long) logEvent.length * Long.BYTES) + RamUsageEstimator.NUM_BYTES_ARRAY_HEADER
            + RamUsageEstimator.NUM_BYTES_OBJECT_REF + Long.BYTES; // count
    }

}
