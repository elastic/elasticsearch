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
import java.util.Collection;
import java.util.List;

/**
 * This class wraps a Lucene Collector Manager. It assumes execution on a single thread
 * so it delegates all the profiling to the generated collector via {@link #getCollectorTree()}.
 */
public final class InternalProfileCollectorManager implements CollectorManager<Collector, Void> {

    private final CollectorManager<Collector, Void> in;
    private final String profilerName;
    private final List<InternalProfileCollectorManager> children;
    private InternalProfileCollector rootCollector;

    public InternalProfileCollectorManager(
        CollectorManager<Collector, Void> in,
        String profilerName,
        List<InternalProfileCollectorManager> children
    ) {
        this.in = in;
        this.profilerName = profilerName;
        this.children = children;
    }

    @Override
    public Collector newCollector() throws IOException {
        assert rootCollector == null;
        rootCollector = new InternalProfileCollector(in.newCollector(), profilerName, children);
        return rootCollector;
    }

    @Override
    public Void reduce(Collection<Collector> collectors) throws IOException {
        assert collectors.size() == 1;
        assert this.rootCollector == collectors.iterator().next();
        return null;
    }

    public CollectorResult getCollectorTree() {
        return rootCollector.getCollectorTree();
    }
}
