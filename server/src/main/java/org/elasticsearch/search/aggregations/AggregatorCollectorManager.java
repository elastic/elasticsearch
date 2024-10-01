/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.search.CollectorManager;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Collector manager that produces {@link AggregatorCollector} and merges them during the reduce phase. */
public class AggregatorCollectorManager implements CollectorManager<AggregatorCollector, Void> {

    private final Supplier<AggregatorCollector> collectorSupplier;
    private final Consumer<InternalAggregations> internalAggregationsConsumer;
    private final Supplier<AggregationReduceContext> reduceContextSupplier;

    public AggregatorCollectorManager(
        Supplier<AggregatorCollector> collectorSupplier,
        Consumer<InternalAggregations> internalAggregationsConsumer,
        Supplier<AggregationReduceContext> reduceContextSupplier
    ) {
        this.collectorSupplier = collectorSupplier;
        this.internalAggregationsConsumer = internalAggregationsConsumer;
        this.reduceContextSupplier = reduceContextSupplier;
    }

    @Override
    public AggregatorCollector newCollector() throws IOException {
        return collectorSupplier.get();
    }

    @Override
    public Void reduce(Collection<AggregatorCollector> collectors) throws IOException {
        if (collectors.size() > 1) {
            // we execute this search using more than one slice. In order to keep memory requirements
            // low, we do a partial reduction here.
            final List<InternalAggregations> internalAggregations = new ArrayList<>(collectors.size());
            collectors.forEach(c -> internalAggregations.add(InternalAggregations.from(c.internalAggregations)));
            internalAggregationsConsumer.accept(InternalAggregations.topLevelReduce(internalAggregations, reduceContextSupplier.get()));
        } else if (collectors.size() == 1) {
            internalAggregationsConsumer.accept(InternalAggregations.from(collectors.iterator().next().internalAggregations));
        }
        return null;
    }
}
