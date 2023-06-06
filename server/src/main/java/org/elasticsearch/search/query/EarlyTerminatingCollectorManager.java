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
import java.util.Collection;

class EarlyTerminatingCollectorManager implements CollectorManager<EarlyTerminatingCollector, Void> {
    private final CollectorManager<? extends Collector, ?> innerCollectorManager;
    private final EarlyTerminatingCollector.ThresholdChecker thresholdChecker;
    private boolean earlyTerminated;

    EarlyTerminatingCollectorManager(CollectorManager<? extends Collector, ?> innerCollectorManager, int numHitsThreshold) {
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
        for (EarlyTerminatingCollector collector : collectors) {
            if (collector.hasEarlyTerminated()) {
                this.earlyTerminated = true;
                break;
            }
        }
        return null;
    }

    boolean hasEarlyTerminated() {
        return earlyTerminated;
    }
}
