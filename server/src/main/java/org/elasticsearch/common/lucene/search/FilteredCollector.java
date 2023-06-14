/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common.lucene.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Collector that wraps another collector and collects only documents that match the provided filter.
 * Given that this collector filters documents out, it must not propagate the {@link Weight} to its
 * inner collector, as that may lead to exposing total hit count that does not reflect the filtering.
 */
public class FilteredCollector implements Collector {

    private final Collector collector;
    private final Weight filter;

    public FilteredCollector(Collector collector, Weight filter) {
        this.collector = collector;
        this.filter = filter;
    }

    @Override
    public final void setWeight(Weight weight) {
        // no-op: this collector filters documents out hence it must not propagate the weight to its inner collector,
        // otherwise the total hit count may not reflect the filtering
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        final ScorerSupplier filterScorerSupplier = filter.scorerSupplier(context);
        final LeafCollector in = collector.getLeafCollector(context);
        final Bits bits = Lucene.asSequentialAccessBits(context.reader().maxDoc(), filterScorerSupplier);

        return new FilterLeafCollector(in) {
            @Override
            public void collect(int doc) throws IOException {
                if (bits.get(doc)) {
                    in.collect(doc);
                }
            }
        };
    }

    @Override
    public ScoreMode scoreMode() {
        return collector.scoreMode();
    }

    /**
     * Creates a {@link CollectorManager} for {@link FilteredCollector}, which enables inter-segment search concurrency
     * when a <code>post_filter</code> is provided as part of a search request.
     */
    public static <C extends Collector, T> CollectorManager<FilteredCollector, T> createManager(
        CollectorManager<C, T> collectorManager,
        Weight filter
    ) {
        AtomicInteger counter = new AtomicInteger(0);
        return new CollectorManager<>() {
            @Override
            public FilteredCollector newCollector() throws IOException {
                return new FilteredCollector(collectorManager.newCollector(), filter);
            }

            @Override
            public T reduce(Collection<FilteredCollector> collectors) throws IOException {
                @SuppressWarnings("unchecked")
                List<C> innerCollectors = collectors.stream().map(filteredCollector -> (C) filteredCollector.collector).toList();
                return collectorManager.reduce(innerCollectors);
            }
        };
    }
}
