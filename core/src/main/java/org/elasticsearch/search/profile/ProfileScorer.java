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

package org.elasticsearch.search.profile;

import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TwoPhaseIterator;
import org.apache.lucene.search.Weight;

import java.io.IOException;
import java.util.Collection;

/**
 * {@link Scorer} wrapper that will compute how much time is spent on moving
 * the iterator, confirming matches and computing scores.
 */
final class ProfileScorer extends Scorer {

    private final Scorer scorer;
    private ProfileWeight profileWeight;
    private final ProfileBreakdown profile;

    ProfileScorer(ProfileWeight w, Scorer scorer, ProfileBreakdown profile) throws IOException {
        super(w);
        this.scorer = scorer;
        this.profileWeight = w;
        this.profile = profile;
    }

    @Override
    public int docID() {
        return scorer.docID();
    }

    @Override
    public float score() throws IOException {
        profile.startTime(ProfileBreakdown.TimingType.SCORE);
        try {
            return scorer.score();
        } finally {
            profile.stopAndRecordTime();
        }
    }

    @Override
    public int freq() throws IOException {
        return scorer.freq();
    }

    @Override
    public Weight getWeight() {
        return profileWeight;
    }

    @Override
    public Collection<ChildScorer> getChildren() {
        return scorer.getChildren();
    }

    @Override
    public DocIdSetIterator iterator() {
        final DocIdSetIterator in = scorer.iterator();
        return new DocIdSetIterator() {
            
            @Override
            public int advance(int target) throws IOException {
                profile.startTime(ProfileBreakdown.TimingType.ADVANCE);
                try {
                    return in.advance(target);
                } finally {
                    profile.stopAndRecordTime();
                }
            }

            @Override
            public int nextDoc() throws IOException {
                profile.startTime(ProfileBreakdown.TimingType.NEXT_DOC);
                try {
                    return in.nextDoc();
                } finally {
                    profile.stopAndRecordTime();
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
        final TwoPhaseIterator in = scorer.twoPhaseIterator();
        if (in == null) {
            return null;
        }
        final DocIdSetIterator inApproximation = in.approximation();
        final DocIdSetIterator approximation = new DocIdSetIterator() {

            @Override
            public int advance(int target) throws IOException {
                profile.startTime(ProfileBreakdown.TimingType.ADVANCE);
                try {
                    return inApproximation.advance(target);
                } finally {
                    profile.stopAndRecordTime();
                }
            }

            @Override
            public int nextDoc() throws IOException {
                profile.startTime(ProfileBreakdown.TimingType.NEXT_DOC);
                try {
                    return inApproximation.nextDoc();
                } finally {
                    profile.stopAndRecordTime();
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
                profile.startTime(ProfileBreakdown.TimingType.MATCH);
                try {
                    return in.matches();
                } finally {
                    profile.stopAndRecordTime();
                }
            }

            @Override
            public float matchCost() {
                return in.matchCost();
            }
        };
    }
}
