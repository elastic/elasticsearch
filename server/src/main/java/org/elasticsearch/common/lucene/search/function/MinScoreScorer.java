/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

import java.io.IOException;

/** A {@link Scorer} that filters out documents that have a score that is
 *  lower than a configured constant. */
final class MinScoreScorer extends Scorer {

    private final Scorer in;
    private final float minScore;

    private float curScore;

    MinScoreScorer(Weight weight, Scorer scorer, float minScore) {
        super(weight);
        this.in = scorer;
        this.minScore = minScore;
    }

    public Scorer getScorer() {
        return in;
    }

    @Override
    public int docID() {
        return in.docID();
    }

    @Override
    public float score() {
        return curScore;
    }

    @Override
    public int advanceShallow(int target) throws IOException {
        return in.advanceShallow(target);
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        return in.getMaxScore(upTo);
    }

    @Override
    public DocIdSetIterator iterator() {
        return TwoPhaseIterator.asDocIdSetIterator(twoPhaseIterator());
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
        final TwoPhaseIterator inTwoPhase = this.in.twoPhaseIterator();
        final DocIdSetIterator approximation = inTwoPhase == null ? in.iterator() : inTwoPhase.approximation();
        return new TwoPhaseIterator(approximation) {

            @Override
            public boolean matches() throws IOException {
                // we need to check the two-phase iterator first
                // otherwise calling score() is illegal
                if (inTwoPhase != null && inTwoPhase.matches() == false) {
                    return false;
                }
                curScore = in.score();
                return curScore >= minScore;
            }

            @Override
            public float matchCost() {
                return 1000f // random constant for the score computation
                        + (inTwoPhase == null ? 0 : inTwoPhase.matchCost());
            }
        };
    }
}
