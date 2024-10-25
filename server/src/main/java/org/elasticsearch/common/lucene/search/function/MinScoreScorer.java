/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.lucene.search.function;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;

import java.io.IOException;

/** A {@link Scorer} that filters out documents that have a score that is
 *  lower than a configured constant. */
public final class MinScoreScorer extends Scorer {

    private final Scorer in;
    private final float minScore;
    private float curScore;
    private final float boost;

    public MinScoreScorer(Scorer scorer, float minScore) {
        this(scorer, minScore, 1f);
    }

    public MinScoreScorer(Scorer scorer, float minScore, float boost) {
        this.in = scorer;
        this.minScore = minScore;
        this.boost = boost;
    }

    @Override
    public int docID() {
        return in.docID();
    }

    @Override
    public float score() {
        return curScore * boost;
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
        TwoPhaseIterator inTwoPhase = in.twoPhaseIterator();
        DocIdSetIterator approximation;
        if (inTwoPhase == null) {
            approximation = in.iterator();
            if (TwoPhaseIterator.unwrap(approximation) != null) {
                inTwoPhase = TwoPhaseIterator.unwrap(approximation);
                approximation = inTwoPhase.approximation();
            }
        } else {
            approximation = inTwoPhase.approximation();
        }
        final TwoPhaseIterator finalTwoPhase = inTwoPhase;
        return new TwoPhaseIterator(approximation) {

            @Override
            public boolean matches() throws IOException {
                // we need to check the two-phase iterator first
                // otherwise calling score() is illegal
                if (finalTwoPhase != null && finalTwoPhase.matches() == false) {
                    return false;
                }
                curScore = in.score();
                return curScore >= minScore;
            }

            @Override
            public float matchCost() {
                return 1000f // random constant for the score computation
                    + (finalTwoPhase == null ? 0 : finalTwoPhase.matchCost());
            }
        };
    }
}
