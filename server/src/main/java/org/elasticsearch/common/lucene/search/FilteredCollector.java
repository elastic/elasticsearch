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
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;

public class FilteredCollector implements Collector {

    private final Collector collector;
    private final Weight filter;

    public FilteredCollector(Collector collector, Weight filter) {
        this.collector = collector;
        this.filter = filter;
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
}
