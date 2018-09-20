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

package org.elasticsearch.search.profile.query;

import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;
import org.elasticsearch.search.profile.Timer;

import java.io.IOException;
import java.util.Collection;

/**
 * {@link Scorer} wrapper that will compute how much time is spent on moving
 * the iterator, confirming matches and computing scores.
 */
final class ProfileScorer extends Scorer {

    private final Scorer scorer;
    private ProfileWeight profileWeight;

    private final Timer scoreTimer, nextDocTimer, advanceTimer, matchTimer, shallowAdvanceTimer, computeMaxScoreTimer;
    private final boolean isConstantScoreQuery;


    ProfileScorer(ProfileWeight w, Scorer scorer, QueryProfileBreakdown profile) throws IOException {
        super(w);
        this.scorer = scorer;
        this.profileWeight = w;
        scoreTimer = profile.getTimer(QueryTimingType.SCORE);
        nextDocTimer = profile.getTimer(QueryTimingType.NEXT_DOC);
        advanceTimer = profile.getTimer(QueryTimingType.ADVANCE);
        matchTimer = profile.getTimer(QueryTimingType.MATCH);
        shallowAdvanceTimer = profile.getTimer(QueryTimingType.SHALLOW_ADVANCE);
        computeMaxScoreTimer = profile.getTimer(QueryTimingType.COMPUTE_MAX_SCORE);
        ProfileScorer profileScorer = null;
        if (w.getQuery() instanceof ConstantScoreQuery && scorer instanceof ProfileScorer) {
            //Case when we have a totalHits query and it is not cached
            profileScorer = (ProfileScorer) scorer;
        } else if (w.getQuery() instanceof ConstantScoreQuery && scorer.getChildren().size() == 1) {
            //Case when we have a top N query. If the scorer has no children, it is because it is cached
            //and in that case we do not do any special treatment
            Scorable childScorer = scorer.getChildren().iterator().next().child;
            if (childScorer instanceof ProfileScorer) {
                profileScorer = (ProfileScorer) childScorer;
            }
        }
        if (profileScorer != null) {
            isConstantScoreQuery = true;
            profile.setTimer(QueryTimingType.NEXT_DOC, profileScorer.nextDocTimer);
            profile.setTimer(QueryTimingType.ADVANCE, profileScorer.advanceTimer);
            profile.setTimer(QueryTimingType.MATCH, profileScorer.matchTimer);
        } else {
            isConstantScoreQuery = false;
        }
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
    public Weight getWeight() {
        return profileWeight;
    }

    @Override
    public Collection<ChildScorable> getChildren() throws IOException {
        return scorer.getChildren();
    }

    @Override
    public DocIdSetIterator iterator() {
        if (isConstantScoreQuery) {
            return scorer.iterator();
        }
        final DocIdSetIterator in = scorer.iterator();
        return new DocIdSetIterator() {

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
        };
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
        if (isConstantScoreQuery) {
            return scorer.twoPhaseIterator();
        }
        final TwoPhaseIterator in = scorer.twoPhaseIterator();
        if (in == null) {
            return null;
        }
        final DocIdSetIterator inApproximation = in.approximation();
        final DocIdSetIterator approximation = new DocIdSetIterator() {

            @Override
            public int advance(int target) throws IOException {
                advanceTimer.start();
                try {
                    return inApproximation.advance(target);
                } finally {
                    advanceTimer.stop();
                }
            }

            @Override
            public int nextDoc() throws IOException {
                nextDocTimer.start();
                try {
                    return inApproximation.nextDoc();
                } finally {
                    nextDocTimer.stop();
                }
            }

            @Override
            public int docID() {
                return inApproximation.docID();
            }

            @Override
            public long cost() {
                return inApproximation.cost();
            }
        };
        return new TwoPhaseIterator(approximation) {
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
}
