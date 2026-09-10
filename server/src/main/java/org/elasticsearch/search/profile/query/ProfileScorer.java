/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.elasticsearch.search.profile.Timer;

import java.io.IOException;
import java.util.Collection;

/**
 * {@link Scorer} wrapper that will compute how much time is spent on moving
 * the iterator, confirming matches and computing scores.
 */
final class ProfileScorer extends Scorer {

    private final Scorer scorer;

    private final Timer scoreTimer, nextDocTimer, advanceTimer, matchTimer, shallowAdvanceTimer, computeMaxScoreTimer,
        setMinCompetitiveScoreTimer;

    ProfileScorer(Scorer scorer, QueryProfileBreakdown profile) {
        this.scorer = scorer;
        scoreTimer = profile.getNewTimer(QueryTimingType.SCORE);
        nextDocTimer = profile.getNewTimer(QueryTimingType.NEXT_DOC);
        advanceTimer = profile.getNewTimer(QueryTimingType.ADVANCE);
        matchTimer = profile.getNewTimer(QueryTimingType.MATCH);
        shallowAdvanceTimer = profile.getNewTimer(QueryTimingType.SHALLOW_ADVANCE);
        computeMaxScoreTimer = profile.getNewTimer(QueryTimingType.COMPUTE_MAX_SCORE);
        setMinCompetitiveScoreTimer = profile.getNewTimer(QueryTimingType.SET_MIN_COMPETITIVE_SCORE);
    }

    @Override
    public int docID() {
        return scorer.docID();
    }

    @Override
    public float score() throws IOException {
        scoreTimer.start();
        try {
            return scorer.score();
        } finally {
            scoreTimer.stop();
        }
    }

    @Override
    public Collection<ChildScorable> getChildren() throws IOException {
        return scorer.getChildren();
    }

    @Override
    public DocIdSetIterator iterator() {
        return new TimedDocIdSetIterator(scorer.iterator());
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
        final TwoPhaseIterator in = scorer.twoPhaseIterator();
        if (in == null) {
            return null;
        }
        return new TwoPhaseIterator(new TimedDocIdSetIterator(in.approximation())) {
            @Override
            public boolean matches() throws IOException {
                matchTimer.start();
                try {
                    return in.matches();
                } finally {
                    matchTimer.stop();
                }
            }

            @Override
            public float matchCost() {
                return in.matchCost();
            }
        };
    }

    @Override
    public int advanceShallow(int target) throws IOException {
        shallowAdvanceTimer.start();
        try {
            return scorer.advanceShallow(target);
        } finally {
            shallowAdvanceTimer.stop();
        }
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        computeMaxScoreTimer.start();
        try {
            return scorer.getMaxScore(upTo);
        } finally {
            computeMaxScoreTimer.stop();
        }
    }

    @Override
    public void setMinCompetitiveScore(float minScore) throws IOException {
        setMinCompetitiveScoreTimer.start();
        try {
            scorer.setMinCompetitiveScore(minScore);
        } finally {
            setMinCompetitiveScoreTimer.stop();
        }
    }

    private class TimedDocIdSetIterator extends DocIdSetIterator {

        private final DocIdSetIterator in;

        TimedDocIdSetIterator(DocIdSetIterator in) {
            this.in = in;
        }

        @Override
        public int advance(int target) throws IOException {
            advanceTimer.start();
            try {
                return in.advance(target);
            } finally {
                advanceTimer.stop();
            }
        }

        @Override
        public int nextDoc() throws IOException {
            nextDocTimer.start();
            try {
                return in.nextDoc();
            } finally {
                nextDocTimer.stop();
            }
        }

        @Override
        public int docID() {
            return in.docID();
        }

        @Override
        public long cost() {
            return in.cost();
        }
    }
}
