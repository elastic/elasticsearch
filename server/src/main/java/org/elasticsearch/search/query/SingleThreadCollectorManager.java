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

/** {@link CollectorManager} implementation that assumes it is running non-concurrently. Therefore
 * {@link #newCollector()} will be called only once. */
public abstract class SingleThreadCollectorManager implements CollectorManager<Collector, Void> {

    private Collector collector;

    @Override
    public final Collector newCollector() throws IOException {
        assert collector == null;
        collector = getNewCollector();
        return collector;
    }

    /** Return a new Collector. This method will be called just once */
    protected abstract Collector getNewCollector() throws IOException;

    @Override
    public final Void reduce(Collection<Collector> collectors) throws IOException {
        assert collectors.size() == 1;
        Collector collector = collectors.iterator().next();
        assert this.collector == collector;
        reduce(collector);
        return null;
    }

    /** Reduce call with the collector provided in {@link #getNewCollector()}. */
    protected void reduce(Collector collector) throws IOException {}

    /** Wraps the provided {@link Collector} as a single thread collector manager. */
    public static SingleThreadCollectorManager wrap(Collector collector) {
        return new SingleThreadCollectorManager() {
            @Override
            protected Collector getNewCollector() {
                return collector;
            }
        };
    }
}
