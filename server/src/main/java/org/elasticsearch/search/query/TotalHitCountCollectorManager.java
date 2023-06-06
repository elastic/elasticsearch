/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.TotalHitCountCollector;

import java.io.IOException;
import java.util.Collection;

/**
 * Collector manager based on {@link TotalHitCountCollector} that allows users to parallelize
 * counting the number of hits. Same as {@link org.apache.lucene.search.TotalHitCountCollectorManager} but with a <code>Void</code>
 * return type, whereas the total hit count can be retrieved via {@link #getTotalHitCount()}.
 */
class TotalHitCountCollectorManager implements CollectorManager<TotalHitCountCollector, Void> {
    private volatile int totalHitCount;

    @Override
    public TotalHitCountCollector newCollector() {
        return new TotalHitCountCollector();
    }

    @Override
    public Void reduce(Collection<TotalHitCountCollector> collectors) throws IOException {
        int totalHits = 0;
        for (TotalHitCountCollector collector : collectors) {
            totalHits += collector.getTotalHits();
        }
        this.totalHitCount = totalHits;
        return null;
    }

    int getTotalHitCount() {
        return totalHitCount;
    }
}
