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
import java.util.ArrayList;
import java.util.List;

/** A {@link SingleThreadCollectorManager} implements which wrap a set of CollectorManager,
 * It requires to be created with 2 or more collector managers. */
public class SingleThreadMultiCollectorManager extends SingleThreadCollectorManager {

    private final List<CollectorManager<Collector, Void>> collectorManagers;

    public SingleThreadMultiCollectorManager(List<CollectorManager<Collector, Void>> collectorManagers) {
        assert collectorManagers.size() > 1 : "should be called with at least two collection managers";
        this.collectorManagers = collectorManagers;
    }

    @Override
    public Collector getNewCollector() throws IOException {
        List<Collector> collectors = new ArrayList<>(collectorManagers.size());
        for (CollectorManager<Collector, Void> collectorManager : collectorManagers) {
            collectors.add(collectorManager.newCollector());
        }
        return MultiCollector.wrap(collectors);
    }

    @Override
    public void reduce(Collector collector) throws IOException {
        assert collector instanceof MultiCollector;
        Collector[] subCollectors = ((MultiCollector) collector).getCollectors();
        assert subCollectors.length == collectorManagers.size();
        for (int i = 0; i < collectorManagers.size(); i++) {
            collectorManagers.get(i).reduce(List.of(subCollectors[i]));
        }
    }
}
