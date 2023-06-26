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

/** {@link CollectorManager} that runs in a non-concurrent search. */
public class SingleThreadCollectorManager implements CollectorManager<Collector, Void> {
    protected final Collector collector;
    private boolean newCollectorCalled;

    public SingleThreadCollectorManager(Collector collector) {
        this.collector = collector;
    }

    @Override
    public Collector newCollector() {
        assert newCollectorCalled == false;
        newCollectorCalled = true;
        return collector;
    }

    @Override
    public Void reduce(Collection<Collector> collectors) throws IOException {
        assert collectors.size() == 1;
        assert this.collector == collectors.iterator().next();
        return null;
    }
}
