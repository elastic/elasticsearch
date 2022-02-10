/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.eql.execution.sampling;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xpack.eql.execution.search.HitReference;
import org.elasticsearch.xpack.eql.execution.sequence.SequenceKey;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.logging.LoggerMessageFormat.format;

/**
 * Holder class representing the instance of a sampling. Used at runtime by the engine to track samplings.
 * Defined by its key and stage.
 * This class is NOT immutable (to optimize memory) which means its associations need to be managed.
 */
class Sampling implements Accountable {

    private static final long SHALLOW_SIZE = RamUsageEstimator.shallowSizeOfInstance(Sampling.class);

    private final SequenceKey key;
    private final HitReference[] matches;

    Sampling(SequenceKey key, List<SearchHit> searchHits) {
        this.key = key;
        this.matches = new HitReference[searchHits.size()];

        for (int i = 0; i < searchHits.size(); i++) {
            this.matches[i] = new HitReference(searchHits.get(i));
        }
    }

    public SequenceKey key() {
        return key;
    }

    public List<HitReference> hits() {
        return Arrays.asList(matches);
    }

    @Override
    public long ramBytesUsed() {
        return SHALLOW_SIZE + RamUsageEstimator.sizeOf(key) + RamUsageEstimator.sizeOf(matches);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        Sampling other = (Sampling) obj;
        return Objects.equals(key, other.key);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(format(null, "[Samp<{}>]", key));

        for (int i = 0; i < matches.length; i++) {
            sb.append(format(null, "\n [{}]={{}}", i, matches[i]));
        }

        return sb.toString();
    }
}
