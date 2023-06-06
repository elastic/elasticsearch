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
 *
 * @param <C> The type of collector for which the collection has to be early terminated.
 */
class EarlyTerminatingCollectorManager<C extends Collector> implements CollectorManager<EarlyTerminatingCollector, Void> {
    private final CollectorManager<C, Void> innerCollectorManager;
    private final EarlyTerminatingCollector.ThresholdChecker thresholdChecker;
    private boolean earlyTerminated;

    EarlyTerminatingCollectorManager(CollectorManager<C, Void> innerCollectorManager, int numHitsThreshold) {
        this.innerCollectorManager = innerCollectorManager;
        this.thresholdChecker = EarlyTerminatingCollector.ThresholdChecker.createShared(numHitsThreshold);
    }

    @Override
    public EarlyTerminatingCollector newCollector() throws IOException {
        C collector = innerCollectorManager.newCollector();
        return new EarlyTerminatingCollector(collector, thresholdChecker, false);
    }

    @Override
    public Void reduce(Collection<EarlyTerminatingCollector> collectors) throws IOException {
        Collection<C> innerCollectors = new ArrayList<>();
        for (EarlyTerminatingCollector collector : collectors) {
            if (collector.hasEarlyTerminated()) {
                this.earlyTerminated = true;
            }
            @SuppressWarnings("unchecked")
            C innerCollector = (C) collector.getInnerCollector();
            innerCollectors.add(innerCollector);
        }
        innerCollectorManager.reduce(innerCollectors);
        return null;
    }

    boolean hasEarlyTerminated() {
        return earlyTerminated;
    }
}
