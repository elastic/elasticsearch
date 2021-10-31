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

    private static class DocIdSetIteratorWrapper extends DocIdSetIterator {
        private final DocIdSetIterator disi;

        DocIdSetIteratorWrapper(DocIdSetIterator disi) {
            this.disi = disi;
        }

        @Override
        public int docID() {
            return disi.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return disi.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return disi.advance(target);
        }

        @Override
        public long cost() {
            return disi.cost();
        }
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
        final TwoPhaseIterator inTwoPhase = this.in.twoPhaseIterator();
        DocIdSetIterator approximation = inTwoPhase != null ? inTwoPhase.approximation() : in.iterator();
        // If the approximation is a wrapper of TwoPhaseIterator, then ConjunctionScorer can add it the list of TwoPhaseIterator
        // after the TwoPhaseIterator of MinScoreScorer. Then, the `matches()` method of the approximation can be called twice:
        // one before `score()` and one after `score()`. This doesn't work well with ToParentBlockJoinQuery with which `matches()`
        // can't be called after `score()`. Here we wrap the approximation to prevent it from unwrapping as a TwoPhaseIterator.
        if (inTwoPhase == null && TwoPhaseIterator.unwrap(approximation) != null) {
            approximation = new DocIdSetIteratorWrapper(approximation);
        }
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
