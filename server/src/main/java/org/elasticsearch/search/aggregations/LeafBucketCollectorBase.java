/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.apache.lucene.search.Scorable;
import org.elasticsearch.common.lucene.ScorerAware;

import java.io.IOException;

/**
 * A {@link LeafBucketCollector} that delegates all calls to the sub leaf
 * aggregator and sets the scorer on its source of values if it implements
 * {@link ScorerAware}.
 */
public class LeafBucketCollectorBase extends LeafBucketCollector {

    private final LeafBucketCollector sub;
    private final ScorerAware values;

    /**
     * @param sub    The leaf collector for sub aggregations.
     * @param values The values. {@link ScorerAware#setScorer} will be called automatically on them if they implement {@link ScorerAware}.
     */
    public LeafBucketCollectorBase(LeafBucketCollector sub, Object values) {
        this.sub = sub;
        if (values instanceof ScorerAware) {
            this.values = (ScorerAware) values;
        } else {
            this.values = null;
        }
    }

    @Override
    public void setScorer(Scorable s) throws IOException {
        sub.setScorer(s);
        if (values != null) {
            values.setScorer(s);
        }
    }

    @Override
    public void collect(int doc, long bucket) throws IOException {
        sub.collect(doc, bucket);
    }

}
