/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.rank;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Weight;

import java.io.IOException;

public abstract class RankCollector extends SimpleCollector {

    private final Collector childCollector;
    private LeafCollector childLeafCollector;

    public RankCollector(Collector childCollector) {
        this.childCollector = childCollector;
    }

    public abstract void setNextReader(LeafReaderContext context) throws IOException;

    protected final void doSetNextReader(LeafReaderContext context) throws IOException {
        setNextReader(context);
        childLeafCollector = childCollector.getLeafCollector(context);
        lrc = context;
    }

    public abstract void doSetWeight(Weight weight);

    LeafReaderContext lrc;
    Weight weight;

    @Override
    public void setWeight(Weight weight) {
        childCollector.setWeight(weight);
        this.weight = weight;
    }

    public abstract void doSetScorer(Scorable scorable) throws IOException;

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        doSetScorer(scorer);
        childLeafCollector.setScorer(scorer);
    }

    public abstract void doCollect(int doc) throws IOException;

    @Override
    public void collect(int doc) throws IOException {
        Explanation e = weight.explain(lrc, doc);
        doCollect(doc);
        childLeafCollector.collect(doc);
    }
}
