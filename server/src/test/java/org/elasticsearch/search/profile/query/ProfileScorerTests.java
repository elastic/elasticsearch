/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;

public class ProfileScorerTests extends ESTestCase {

    private static class FakeScorer extends Scorer {

        public float maxScore, minCompetitiveScore;

        @Override
        public DocIdSetIterator iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public float getMaxScore(int upTo) throws IOException {
            return maxScore;
        }

        @Override
        public float score() throws IOException {
            return 1f;
        }

        @Override
        public int docID() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setMinCompetitiveScore(float minScore) {
            this.minCompetitiveScore = minScore;
        }
    }

    private static class FakeWeight extends Weight {

        protected FakeWeight(Query query) {
            super(query);
        }

        @Override
        public Explanation explain(LeafReaderContext context, int doc) throws IOException {
            return Explanation.match(1, "fake_description");
        }

        @Override
        public ScorerSupplier scorerSupplier(LeafReaderContext context) {
            return new ScorerSupplier() {
                private long cost = 0;

                @Override
                public Scorer get(long leadCost) {
                    return new Scorer() {
                        @Override
                        public DocIdSetIterator iterator() {
                            return null;
                        }

                        @Override
                        public float getMaxScore(int upTo) {
                            return 42f;
                        }

                        @Override
                        public float score() {
                            return 0;
                        }

                        @Override
                        public int docID() {
                            return 0;
                        }
                    };
                }

                @Override
                public long cost() {
                    return cost;
                }

                @Override
                public void setTopLevelScoringClause() {
                    cost = 42;
                }
            };
        }

        @Override
        public boolean isCacheable(LeafReaderContext ctx) {
            return false;
        }

        @Override
        public Matches matches(LeafReaderContext context, int doc) throws IOException {
            return new Matches() {
                @Override
                public MatchesIterator getMatches(String field) throws IOException {
                    return new MatchesIterator() {
                        @Override
                        public boolean next() throws IOException {
                            return false;
                        }

                        @Override
                        public int startPosition() {
                            return 42;
                        }

                        @Override
                        public int endPosition() {
                            return 43;
                        }

                        @Override
                        public int startOffset() throws IOException {
                            return 44;
                        }

                        @Override
                        public int endOffset() throws IOException {
                            return 45;
                        }

                        @Override
                        public MatchesIterator getSubMatches() throws IOException {
                            return null;
                        }

                        @Override
                        public Query getQuery() {
                            return parentQuery;
                        }
                    };
                }

                @Override
                public Collection<Matches> getSubMatches() {
                    return Collections.emptyList();
                }

                @Override
                public Iterator<String> iterator() {
                    return null;
                }
            };
        }
    }

    public void testPropagateMinCompetitiveScore() throws IOException {
        FakeScorer fakeScorer = new FakeScorer();
        QueryProfileBreakdown profile = new QueryProfileBreakdown();
        ProfileScorer profileScorer = new ProfileScorer(fakeScorer, profile);
        profileScorer.setMinCompetitiveScore(0.42f);
        assertEquals(0.42f, fakeScorer.minCompetitiveScore, 0f);
    }

    public void testPropagateMaxScore() throws IOException {
        FakeScorer fakeScorer = new FakeScorer();
        QueryProfileBreakdown profile = new QueryProfileBreakdown();
        ProfileScorer profileScorer = new ProfileScorer(fakeScorer, profile);
        profileScorer.setMinCompetitiveScore(0.42f);
        fakeScorer.maxScore = 42f;
        assertEquals(42f, profileScorer.getMaxScore(DocIdSetIterator.NO_MORE_DOCS), 0f);
    }

    // tests that ProfileWeight correctly propagates the wrapped inner weight
    public void testPropagateSubWeight() throws IOException {
        Query query = new MatchAllDocsQuery();
        Weight fakeWeight = new FakeWeight(query);
        QueryProfileBreakdown profile = new QueryProfileBreakdown();
        ProfileWeight profileWeight = new ProfileWeight(query, fakeWeight, profile);
        assertEquals(42f, profileWeight.scorer(null).getMaxScore(DocIdSetIterator.NO_MORE_DOCS), 0f);
        assertEquals(42, profileWeight.matches(null, 1).getMatches("some_field").startPosition());
        assertEquals("fake_description", profileWeight.explain(null, 1).getDescription());
    }

    public void testPropagateTopLevelScoringClause() throws IOException {
        Query query = new MatchAllDocsQuery();
        Weight fakeWeight = new FakeWeight(query);
        QueryProfileBreakdown profile = new QueryProfileBreakdown();
        ProfileWeight profileWeight = new ProfileWeight(query, fakeWeight, profile);
        ScorerSupplier scorerSupplier = profileWeight.scorerSupplier(null);
        scorerSupplier.setTopLevelScoringClause();
        assertEquals(42, scorerSupplier.cost());
    }
}
