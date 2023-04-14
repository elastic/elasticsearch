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
import org.elasticsearch.search.query.SingleThreadCollectorManager;

import java.io.IOException;
import java.util.List;

/**
 * This class wraps a Lucene Collector Manager. It assumes execution on a single thread
 * so it delegates all the profiling to the generated collector via {@link #getCollectorTree()}.
 */
public class InternalProfileCollectorManager extends SingleThreadCollectorManager {

    private final CollectorManager<Collector, Void> in;
    private final String profilerName;
    private InternalProfileCollector rootCollector;

    public InternalProfileCollectorManager(CollectorManager<Collector, Void> in, String profilerName) {
        this.in = in;
        this.profilerName = profilerName;
    }

    @Override
    protected Collector getNewCollector() throws IOException {
        assert rootCollector == null;
        rootCollector = new InternalProfileCollector(in.newCollector(), profilerName, List.of());
        return rootCollector;
    }

    @Override
    protected void reduce(Collector collector) throws IOException {
        assert collector instanceof InternalProfileCollector;
        in.reduce(List.of(((InternalProfileCollector) collector).getDelegate()));
    }

    public CollectorResult getCollectorTree() {
        return rootCollector.getCollectorTree();
    }
}
