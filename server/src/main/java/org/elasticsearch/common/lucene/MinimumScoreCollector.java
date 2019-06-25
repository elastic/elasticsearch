/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
        if (!(scorer instanceof ScoreCachingWrappingScorer)) {
            scorer = new ScoreCachingWrappingScorer(scorer);
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
