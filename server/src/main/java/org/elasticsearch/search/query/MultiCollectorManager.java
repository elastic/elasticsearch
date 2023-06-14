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

public class MultiCollectorManager implements CollectorManager<Collector, Void> {
    private final org.apache.lucene.search.MultiCollectorManager multiCollectorManager;

    public MultiCollectorManager(
        final CollectorManager<? extends Collector, ?> collectorManager1,
        final CollectorManager<? extends Collector, ?> collectorManager2
    ) {
        this.multiCollectorManager = new org.apache.lucene.search.MultiCollectorManager(collectorManager1, collectorManager2);
    }

    @Override
    public Collector newCollector() throws IOException {
        return multiCollectorManager.newCollector();
    }

    @Override
    public Void reduce(Collection<Collector> collectors) throws IOException {
        multiCollectorManager.reduce(collectors);
        return null;
    }
}
