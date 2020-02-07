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

package org.elasticsearch.index.query.compound;

import org.apache.lucene.search.DisiPriorityQueue;
import org.apache.lucene.search.DisiWrapper;
import org.apache.lucene.search.DisjunctionDISIApproximation;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.query.compound.CompoundQuery.CombineMode;

/*
 *  Based on org.apache.lucene.search.DisjunctionScorer
 */
class DisjunctionCombineScorer extends Scorer {

    private final DisiPriorityQueue subScorers;
    private final DocIdSetIterator approximation;
    private final CombineMode combineMode;
    protected DisjunctionCombineScorer(Weight weight, List<Scorer> subScorers, CombineMode combineMode, ScoreMode scoreMode)
            throws IOException {
        super(weight);
        if (subScorers.size() <= 1) {
            throw new IllegalArgumentException("There must be at least 2 subScorers");
        }
        this.combineMode = combineMode;
        this.subScorers = new DisiPriorityQueue(subScorers.size());
        for (int i = 0; i < subScorers.size(); i++) {
            final DisiOrderedWrapper w = new DisiOrderedWrapper(subScorers.get(i), i);
            this.subScorers.add(w);
        }
        this.approximation = new DisjunctionDISIApproximation(this.subScorers);
    }

    @Override
    public DocIdSetIterator iterator() {
        return approximation;
    }

    @Override
    public final int docID() {
        return subScorers.top().doc;
    }

    DisiWrapper getSubMatches() {
        return subScorers.topList();
    }

    @Override
    public final float score() throws IOException {
        return score(getSubMatches());
    }

    private float score(DisiWrapper topList) throws IOException {
        float score;
        switch(combineMode) {
            case FIRST:
                Scorer scorer = null;
                int minOrder = Integer.MAX_VALUE;
                for (DisiOrderedWrapper w = (DisiOrderedWrapper) topList; w != null; w = (DisiOrderedWrapper) w.next) {
                    if (w.order < minOrder) {
                        minOrder = w.order;
                        scorer = w.scorer;
                    }
                }
                score = scorer.score();
                break;
            case MAX:
                score = 0;
                for (DisiWrapper w = topList; w != null; w = w.next) {
                    float curScore = w.scorer.score();
                    if (curScore > score) {
                        score = curScore;
                    }
                }
                break;
            case MIN:
                score = Float.MAX_VALUE;
                for (DisiWrapper w = topList; w != null; w = w.next) {
                    float curScore = w.scorer.score();
                    if (curScore < score) {
                        score = curScore;
                    }
                }
                break;
            case MULTIPLY:
                score = 1;
                for (DisiWrapper w = topList; w != null; w = w.next) {
                    score *= w.scorer.score();
                }
                break;
            default: // SUM or AVG
                score = 0;
                int scorersCount = 0;
                for (DisiWrapper w = topList; w != null; w = w.next) {
                    score += w.scorer.score();
                    scorersCount++;
                }
                if (combineMode == CombineMode.AVG) {
                    score = score / scorersCount;
                }
        }
        return score;
    }

    @Override
    public float getMaxScore(int upTo) {
        return Float.MAX_VALUE;
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
