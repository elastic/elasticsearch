/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.search.CollectorManager;

import java.io.IOException;
import java.util.Collection;
import java.util.function.Supplier;

/** Collector manager that produces {@link AggregatorCollector}. */
public class AggregatorCollectorManager implements CollectorManager<AggregatorCollector, Void> {

    private final Supplier<AggregatorCollector> collectorSupplier;

    public AggregatorCollectorManager(Supplier<AggregatorCollector> collectorSupplier) {
        this.collectorSupplier = collectorSupplier;
    }

    @Override
    public AggregatorCollector newCollector() throws IOException {
        return collectorSupplier.get();
    }

    @Override
    public Void reduce(Collection<AggregatorCollector> collectors) throws IOException {
        // we cannot run Aggregator#buildTopLevel here because we need to do it after the optional timeout
        // has been removed from the index searcher. Therefore, we delay this processing to the
        // AggregationPhase#execute method.
        return null;
    }
}
