/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * {@link CollectorManager} implementation based on {@link QueryPhaseCollector}.
 * Wraps two {@link CollectorManager}: one required for top docs collection, and one optional for aggs collection.
 * Applies terminate_after consistently across the different collectors by sharing an atomic counter of collected docs.
 */
class QueryPhaseCollectorManager implements CollectorManager<QueryPhaseCollector, Void> {
    private final Weight postFilterWeight;
    private final QueryPhaseCollector.TerminateAfterChecker terminateAfterChecker;
    private final Float minScore;
    private final CollectorManager<? extends Collector, Void> topDocsCollectorManager;
    private final CollectorManager<? extends Collector, Void> aggsCollectorManager;

    private boolean terminatedEarly;

    QueryPhaseCollectorManager(
        CollectorManager<? extends Collector, Void> topDocsCollectorManager,
        Weight postFilterWeight,
        QueryPhaseCollector.TerminateAfterChecker terminateAfterChecker,
        CollectorManager<? extends Collector, Void> aggsCollectorManager,
        Float minScore
    ) {
        this.topDocsCollectorManager = topDocsCollectorManager;
        this.postFilterWeight = postFilterWeight;
        this.terminateAfterChecker = terminateAfterChecker;
        this.aggsCollectorManager = aggsCollectorManager;
        this.minScore = minScore;
    }

    @Override
    public QueryPhaseCollector newCollector() throws IOException {
        Collector aggsCollector = aggsCollectorManager == null ? null : aggsCollectorManager.newCollector();
        return new QueryPhaseCollector(
            topDocsCollectorManager.newCollector(),
            postFilterWeight,
            terminateAfterChecker,
            aggsCollector,
            minScore
        );
    }

    @Override
    public Void reduce(Collection<QueryPhaseCollector> collectors) throws IOException {
        List<Collector> topDocsCollectors = new ArrayList<>();
        List<Collector> aggsCollectors = new ArrayList<>();
        for (QueryPhaseCollector collector : collectors) {
            topDocsCollectors.add(collector.getTopDocsCollector());
            aggsCollectors.add(collector.getAggsCollector());
            if (collector.isTerminatedAfter()) {
                terminatedEarly = true;
            }
        }
        @SuppressWarnings("unchecked")
        CollectorManager<Collector, Void> topDocsManager = (CollectorManager<Collector, Void>) topDocsCollectorManager;
        topDocsManager.reduce(topDocsCollectors);
        if (aggsCollectorManager != null) {
            @SuppressWarnings("unchecked")
            CollectorManager<Collector, Void> aggsManager = (CollectorManager<Collector, Void>) aggsCollectorManager;
            aggsManager.reduce(aggsCollectors);
        }
        return null;
    }

    public boolean isTerminatedEarly() {
        return terminatedEarly;
    }
}
