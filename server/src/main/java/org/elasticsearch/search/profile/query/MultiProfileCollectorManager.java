/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * A {@link CollectorManager} that wraps instances created by another CollectorManager before passing them on.
 * It delegates all the profiling to the generated collectors via {@link #getCollectorTree()} and joins them up in one
 * {@link CollectorResult}.
 */
public final class MultiProfileCollectorManager<T> implements CollectorManager<Collector, T> {

    protected final CollectorManager<Collector, T> collectorManager;
    private final List<Collector> wrappedCollectors = new ArrayList<>();
    private final List<InternalProfileCollector> profileCollectors = new ArrayList<>();

    public MultiProfileCollectorManager(CollectorManager<Collector, T> collectorManager) {
        this.collectorManager = collectorManager;
    }

    @Override
    public Collector newCollector() throws IOException {
        Collector collector = collectorManager.newCollector();
        wrappedCollectors.add(collector);
        InternalProfileCollector internalProfileCollector = new InternalProfileCollector(collector, CollectorResult.REASON_SEARCH_TOP_HITS);
        profileCollectors.add(internalProfileCollector);
        return internalProfileCollector;
    }

    public T reduce(Collection<Collector> collectors) throws IOException {
        return collectorManager.reduce(wrappedCollectors);
    }

    public CollectorResult getCollectorTree() {
        List<CollectorResult> delegateResults = new ArrayList<>(profileCollectors.size());
        for (InternalProfileCollector ipc : profileCollectors) {
            delegateResults.add(ipc.getCollectorTree());
        }
        CollectorResult firstCollector = delegateResults.get(0);
        // TODO maybe we should get max time here?
        return new CollectorResult(firstCollector.getName(), firstCollector.getReason(), firstCollector.getTime(), delegateResults);
    }
}
