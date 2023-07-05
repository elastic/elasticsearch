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

class QueryPhaseCollectorManager<TD, AG> implements CollectorManager<QueryPhaseCollector, QueryPhaseCollectorManager.Result<TD, AG>> {
    private final Weight postFilterWeight;
    private final QueryPhaseCollector.TerminateAfterChecker terminateAfterChecker;
    private final Float minScore;
    private final CollectorManager<? extends Collector, TD> topDocsCollectorManager;
    private final CollectorManager<? extends Collector, AG> aggsCollectorManager;

    QueryPhaseCollectorManager(
        CollectorManager<? extends Collector, TD> topDocsCollectorManager,
        Weight postFilterWeight,
        QueryPhaseCollector.TerminateAfterChecker terminateAfterChecker,
        CollectorManager<? extends Collector, AG> aggsCollectorManager,
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
    public Result<TD, AG> reduce(Collection<QueryPhaseCollector> collectors) throws IOException {
        boolean terminatedAfter = false;
        List<Collector> topDocsCollectors = new ArrayList<>();
        List<Collector> aggsCollectors = new ArrayList<>();
        for (QueryPhaseCollector collector : collectors) {
            topDocsCollectors.add(collector.getTopDocsCollector());
            aggsCollectors.add(collector.getAggsCollector());
            if (collector.isTerminatedAfter()) {
                terminatedAfter = true;
            }
        }
        @SuppressWarnings("unchecked")
        CollectorManager<Collector, TD> topDocsManager = (CollectorManager<Collector, TD>) topDocsCollectorManager;
        TD topDocs = topDocsManager.reduce(topDocsCollectors);
        AG aggs = null;
        if (aggsCollectorManager != null) {
            @SuppressWarnings("unchecked")
            CollectorManager<Collector, AG> aggsManager = (CollectorManager<Collector, AG>) aggsCollectorManager;
            aggs = aggsManager.reduce(aggsCollectors);
        }
        return new Result<>(topDocs, aggs, terminatedAfter);
    }

    record Result<T, A>(T topDocs, A aggs, boolean terminatedAfter) {}
}
