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
import org.apache.lucene.search.MultiCollector;

import java.io.IOException;
import java.util.List;

public class MultiCollectorManager extends BaseCollectorManager {

    private final List<CollectorManager<Collector, Void>> collectorManagers;

    public MultiCollectorManager(final List<CollectorManager<Collector, Void>> collectorManagers) {
        if (collectorManagers.size() == 0) {
            throw new IllegalArgumentException("There must be at least one collector manager");
        }

        for (CollectorManager<Collector, Void> collectorManager : collectorManagers) {
            if (collectorManager == null) {
                throw new IllegalArgumentException("Collector managers should all be non-null");
            }
        }
        this.collectorManagers = collectorManagers;
    }

    @Override
    public Collector newCollector() throws IOException {
        Collector[] collectors = new Collector[collectorManagers.size()];
        for (int i = 0; i < collectorManagers.size(); i++) {
            collectors[i] = collectorManagers.get(i).newCollector();
        }
        return MultiCollector.wrap(collectors);
    }
}
