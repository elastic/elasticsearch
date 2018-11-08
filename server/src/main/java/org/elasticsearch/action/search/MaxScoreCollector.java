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

package org.elasticsearch.action.search;

import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.SimpleCollector;

import java.io.IOException;

/**
 * A collector that computes the maximum score.
 */
public class MaxScoreCollector extends SimpleCollector {

    private Scorable scorer;
    private float maxScore = Float.NEGATIVE_INFINITY;
    private boolean hasHits = false;

    @Override
    public void setScorer(Scorable scorer) {
        this.scorer = scorer;
    }

    @Override
    public ScoreMode scoreMode() {
        // Could be TOP_SCORES but it is always used in a MultiCollector anyway, so this saves some wrapping.
        return ScoreMode.COMPLETE;
    }

    @Override
    public void collect(int doc) throws IOException {
        hasHits = true;
        maxScore = Math.max(maxScore, scorer.score());
    }

    /**
     * Get the maximum score. This returns {@link Float#NaN} if no hits were
     * collected.
     */
    public float getMaxScore() {
        return hasHits ? maxScore : Float.NaN;
    }

}
