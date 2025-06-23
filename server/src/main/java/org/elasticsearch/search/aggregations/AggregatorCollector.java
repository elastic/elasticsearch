/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.search.internal.TwoPhaseCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Collector that controls the life cycle of an aggregation document collection. */
public class AggregatorCollector implements TwoPhaseCollector {
    final Aggregator[] aggregators;
    final BucketCollector bucketCollector;
    final List<InternalAggregation> internalAggregations;

    public AggregatorCollector(Aggregator[] aggregators, BucketCollector bucketCollector) {
        this.aggregators = aggregators;
        this.bucketCollector = bucketCollector;
        this.internalAggregations = new ArrayList<>(aggregators.length);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        return bucketCollector.getLeafCollector(new AggregationExecutionContext(context, null, null, null));
    }

    @Override
    public ScoreMode scoreMode() {
        return bucketCollector.scoreMode();
    }

    @Override
    public void doPostCollection() throws IOException {
        bucketCollector.postCollection();
        for (Aggregator aggregator : aggregators) {
            internalAggregations.add(aggregator.buildTopLevel());
            // release the aggregator to claim the used bytes as we don't need it anymore
            aggregator.releaseAggregations();
        }
    }

    @Override
    public String toString() {
        String[] aggNames = new String[aggregators.length];
        for (int i = 0; i < aggregators.length; i++) {
            aggNames[i] = aggregators[i].name();
        }
        return Arrays.toString(aggNames);
    }
}
