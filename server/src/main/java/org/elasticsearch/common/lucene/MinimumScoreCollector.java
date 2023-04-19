/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreCachingWrappingScorer;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.Weight;

import java.io.IOException;

/**
 * Collector that wraps another collector and collects only documents that have a score that's greater or equal than the
 * provided minimum score. Given that this collector filters documents out, it must not propagate the {@link Weight} to its
 * inner collector, as that may lead to exposing total hit count that does not reflect the filtering.
 */
public class MinimumScoreCollector extends SimpleCollector {

    private final Collector collector;
    private final float minimumScore;

    private Scorable scorer;
    private LeafCollector leafCollector;

    public MinimumScoreCollector(Collector collector, float minimumScore) {
        this.collector = collector;
        this.minimumScore = minimumScore;
    }

    @Override
    public final void setWeight(Weight weight) {
        // no-op: this collector filters documents out hence it must not propagate the weight to its inner collector,
        // otherwise the total hit count may not reflect the filtering
    }

    @Override
    public void setScorer(Scorable scorer) throws IOException {
        if ((scorer instanceof ScoreCachingWrappingScorer) == false) {
            scorer = ScoreCachingWrappingScorer.wrap(scorer);
        }
        this.scorer = scorer;
        leafCollector.setScorer(scorer);
    }

    @Override
    public void collect(int doc) throws IOException {
        if (scorer.score() >= minimumScore) {
            leafCollector.collect(doc);
        }
    }

    @Override
    public void doSetNextReader(LeafReaderContext context) throws IOException {
        leafCollector = collector.getLeafCollector(context);
    }

    @Override
    public ScoreMode scoreMode() {
        return collector.scoreMode() == ScoreMode.TOP_SCORES ? ScoreMode.TOP_SCORES : ScoreMode.COMPLETE;
    }
}
