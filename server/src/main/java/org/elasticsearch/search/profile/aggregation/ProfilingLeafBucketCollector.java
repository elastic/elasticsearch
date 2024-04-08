/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.aggregation;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorable;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.profile.Timer;

import java.io.IOException;

public class ProfilingLeafBucketCollector extends LeafBucketCollector {

    private LeafBucketCollector delegate;
    private Timer collectTimer;

    public ProfilingLeafBucketCollector(LeafBucketCollector delegate, AggregationProfileBreakdown profileBreakdown) {
        this.delegate = delegate;
        this.collectTimer = profileBreakdown.getNewTimer(AggregationTimingType.COLLECT);
    }

    @Override
    public void collect(int doc, long bucket) throws IOException {
        collectTimer.start();
        try {
            delegate.collect(doc, bucket);
        } finally {
            collectTimer.stop();
        }
    }

    @Override
    public DocIdSetIterator competitiveIterator() throws IOException {
        return delegate.competitiveIterator();
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        delegate.setScorer(scorer);
    }

    @Override
    public boolean isNoop() {
        return delegate.isNoop();
    }

}
