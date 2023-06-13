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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

/**
 * Collector manager that allows to early terminate any collection. Relies on {@link EarlyTerminatingCollector}s that safely
 * share the number of collected hits through the wrapped {@link Collector}s.
 */
class EarlyTerminatingCollectorManager implements CollectorManager<EarlyTerminatingCollector, Void> {
    private final CollectorManager<? extends Collector, Void> innerCollectorManager;
    private final EarlyTerminatingCollector.ThresholdChecker thresholdChecker;
    private boolean earlyTerminated;

    EarlyTerminatingCollectorManager(CollectorManager<? extends Collector, Void> innerCollectorManager, int numHitsThreshold) {
        this.innerCollectorManager = innerCollectorManager;
        this.thresholdChecker = EarlyTerminatingCollector.ThresholdChecker.createShared(numHitsThreshold);
    }

    @Override
    public EarlyTerminatingCollector newCollector() throws IOException {
        Collector collector = innerCollectorManager.newCollector();
        return new EarlyTerminatingCollector(collector, thresholdChecker, false);
    }

    @Override
    public Void reduce(Collection<EarlyTerminatingCollector> collectors) throws IOException {
        Collection<Collector> innerCollectors = new ArrayList<>();
        for (EarlyTerminatingCollector collector : collectors) {
            if (collector.hasEarlyTerminated()) {
                this.earlyTerminated = true;
            }
            innerCollectors.add(collector.getInnerCollector());
        }
        @SuppressWarnings("unchecked")
        CollectorManager<Collector, Void> cm = (CollectorManager<Collector, Void>) innerCollectorManager;
        return cm.reduce(innerCollectors);
    }

    boolean hasEarlyTerminated() {
        return earlyTerminated;
    }
}
