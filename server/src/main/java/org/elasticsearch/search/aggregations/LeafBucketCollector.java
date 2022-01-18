/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.elasticsearch.search.aggregations.bucket.terms.LongKeyedBucketOrds;

import java.io.IOException;

/**
 * Collects results for a particular segment. See the docs for
 * {@link LeafBucketCollector#collect(int, long)} for <strong>how</strong>
 * to do the collecting.
 */
public abstract class LeafBucketCollector implements LeafCollector {
    /**
     * A {@linkplain LeafBucketCollector} that doesn't collect anything.
     * {@link Aggregator}s will return this if they've already collected
     * their results and don't need to hook into the primary search. It is
     * always safe to skip calling this calling {@link #setScorer} and
     * {@link #collect} on this collector.
     */
    public static final LeafBucketCollector NO_OP_COLLECTOR = new LeafBucketCollector() {
        @Override
        public void setScorer(Scorable arg0) throws IOException {
            // no-op
        }

        @Override
        public void collect(int doc, long bucket) {
            // no-op
        }

        @Override
        public boolean isNoop() {
            return true;
        }
    };

    /**
     * Collect the given {@code doc} in the bucket owned by
     * {@code owningBucketOrd}.
     * <p>
     * The implementation of this method metric aggregations is generally
     * something along the lines of
     * <pre>{@code
     * array[owningBucketOrd] += loadValueFromDoc(doc)
     * }</pre>
     * <p>Bucket aggregations have more trouble because their job is to
     * <strong>make</strong> new ordinals. So their implementation generally
     * looks kind of like
     * <pre>{@code
     * long myBucketOrd = mapOwningBucketAndValueToMyOrd(owningBucketOrd, loadValueFromDoc(doc));
     * collectBucket(doc, myBucketOrd);
     * }</pre>
     * <p>
     * Some bucket aggregations "know" how many ordinals each owning ordinal
     * needs so they can map "densely". The {@code range} aggregation, for
     * example, can perform this mapping with something like:
     * <pre>{@code
     * return rangeCount * owningBucketOrd + matchingRange(value);
     * }</pre>
     * Other aggregations don't know how many buckets will fall into any
     * particular owning bucket. The {@code terms} aggregation, for example,
     * uses {@link LongKeyedBucketOrds} which amounts to a hash lookup.
     */
    public abstract void collect(int doc, long owningBucketOrd) throws IOException;

    /**
     * Does this collector collect anything? If this returns true we can safely
     * just never call {@link #collect}.
     */
    public boolean isNoop() {
        return false;
    }

    @Override
    public final void collect(int doc) throws IOException {
        collect(doc, 0);
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        // no-op by default
    }
}
