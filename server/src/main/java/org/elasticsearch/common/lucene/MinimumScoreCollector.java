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

import java.io.IOException;

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
        return collector.scoreMode() == ScoreMode.TOP_SCORES ? ScoreMode.TOP_SCORES :  ScoreMode.COMPLETE;
    }
}
