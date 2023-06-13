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
import org.elasticsearch.common.lucene.search.FilteredCollector;

import java.io.IOException;
import java.util.Collection;

/**
 * {@link CollectorManager} for {@link FilteredCollector}, which enables inter-segment search concurrency
 * when a <code>post_filter</code> is provided as part of a search request.
 */
class FilteredCollectorManager implements CollectorManager<FilteredCollector, Void> {
    private final CollectorManager<? extends Collector, Void> innerCollectorManager;
    private final Weight filter;

    FilteredCollectorManager(CollectorManager<? extends Collector, Void> innerCollectorManager, Weight filter) {
        this.innerCollectorManager = innerCollectorManager;
        this.filter = filter;
    }

    @Override
    public FilteredCollector newCollector() throws IOException {
        return new FilteredCollector(innerCollectorManager.newCollector(), filter);
    }

    @Override
    public Void reduce(Collection<FilteredCollector> collectors) throws IOException {
        @SuppressWarnings("unchecked")
        CollectorManager<Collector, Void> cm = (CollectorManager<Collector, Void>) innerCollectorManager;
        return cm.reduce(collectors.stream().map(FilteredCollector::getCollector).toList());
    }
}
