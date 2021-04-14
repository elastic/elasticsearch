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

package org.elasticsearch.index.query.first;

import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/*
 *  Based on org.apache.lucene.search.DisjunctionScorer
 */
class FirstScorer extends Scorer {
    private final DisiPriorityQueue pq;
    private final DocIdSetIterator approximation;
    private final List<Scorer> subScorers;
    protected FirstScorer(Weight weight, List<Scorer> subScorers) {
        super(weight);
        this.subScorers = subScorers;
        this.pq = new DisiPriorityQueue(subScorers.size());
        for (int i = 0; i < subScorers.size(); i++) {
            final DisiOrderedWrapper w = new DisiOrderedWrapper(subScorers.get(i), i);
            pq.add(w);
        }
        this.approximation = new DisjunctionDISIApproximation(pq);
    }

    @Override
    public DocIdSetIterator iterator() {
        return approximation;
    }

    @Override
    public final int docID() {
        return pq.top().doc;
    }

    DisiWrapper getSubMatches() {
        return pq.topList();
    }

    @Override
    public final float score() throws IOException {
        return score(getSubMatches());
    }

    // get score from the 1st scorer according to the order
    private float score(DisiWrapper topList) throws IOException {
        Scorer scorer = null;
        int minOrder = Integer.MAX_VALUE;
        for (DisiOrderedWrapper w = (DisiOrderedWrapper) topList; w != null; w = (DisiOrderedWrapper) w.next) {
            if (w.order < minOrder) {
                minOrder = w.order;
                scorer = w.scorer;
            }
        }
        return scorer.score();
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        float scoreMax = 0;
        for (Scorer scorer : subScorers) {
            if (scorer.docID() <= upTo) {
                float subScore = scorer.getMaxScore(upTo);
                if (subScore > scoreMax) {
                    scoreMax = subScore;
                }
            }
        }
        return scoreMax;
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
        for (Scorer scorer : subScorers) {
            scorer.setMinCompetitiveScore(minScore);
        }
    }

    @Override
    public final Collection<ChildScorable> getChildren() {
        ArrayList<ChildScorable> children = new ArrayList<>();
        for (DisiWrapper scorer = getSubMatches(); scorer != null; scorer = scorer.next) {
            children.add(new ChildScorable(scorer.scorer, "SHOULD"));
        }
        return children;
    }
}
