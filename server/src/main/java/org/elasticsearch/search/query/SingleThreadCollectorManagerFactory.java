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

import java.util.Collection;

/** Factory to create {@link CollectorManager} that running as non-concurrent process. */
public final class SingleThreadCollectorManagerFactory {

    /** Wraps the provided {@link Collector} as a single thread collector manager. */
    public static CollectorManager<Collector, Void> wrap(Collector in) {
        return new CollectorManager<>() {
            private Collector collector;

            @Override
            public Collector newCollector() {
                assert collector == null;
                return collector = in;
            }

            @Override
            public Void reduce(Collection<Collector> collectors) {
                assert collectors.size() == 1;
                assert this.collector == collectors.iterator().next();
                return null;
            }
        };
    }
}
