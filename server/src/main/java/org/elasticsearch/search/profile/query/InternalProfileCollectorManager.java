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

public final class InternalProfileCollectorManager implements CollectorManager<InternalProfileCollector, Void> {
    private final CollectorManager<? extends Collector, Void> collectorManager;
    private final String reason;
    private final InternalProfileCollectorManager[] childCollectorManagers;
    private InternalProfileCollector profileCollector;
    private CollectorResult collectorResult;

    public InternalProfileCollectorManager(
        CollectorManager<? extends Collector, Void> collectorManager,
        String reason,
        InternalProfileCollectorManager... childCollectorManagers
    ) {
        this.collectorManager = collectorManager;
        this.reason = reason;
        this.childCollectorManagers = childCollectorManagers;
    }

    @Override
    public InternalProfileCollector newCollector() throws IOException {
        Collector collector = collectorManager.newCollector();
        InternalProfileCollector[] childProfileCollectors = new InternalProfileCollector[childCollectorManagers.length];
        for (int i = 0; i < childCollectorManagers.length; i++) {
            InternalProfileCollector childProfileCollector = childCollectorManagers[i].profileCollector;
            assert childProfileCollector != null;
            childProfileCollectors[i] = childProfileCollector;
        }
        profileCollector = new InternalProfileCollector(collector, reason, childProfileCollectors);
        return profileCollector;
    }

    @Override
    public Void reduce(Collection<InternalProfileCollector> collectors) throws IOException {
        List<Collector> innerCollectors = new ArrayList<>(collectors.size());
        List<CollectorResult> delegateResults = new ArrayList<>(collectors.size());
        for (InternalProfileCollector collector : collectors) {
            innerCollectors.add(collector.getCollector());
            delegateResults.add(collector.getCollectorTree());
        }

        CollectorResult firstCollector = delegateResults.get(0);
        // TODO double check that the output is the desired one
        collectorResult = new CollectorResult(
            firstCollector.getName(),
            firstCollector.getReason(),
            firstCollector.getTime(),
            delegateResults
        );

        @SuppressWarnings("unchecked")
        CollectorManager<Collector, Void> manager = (CollectorManager<Collector, Void>) collectorManager;
        return manager.reduce(innerCollectors);
    }

    public CollectorResult getCollectorTree() {
        return collectorResult;
    }
}
