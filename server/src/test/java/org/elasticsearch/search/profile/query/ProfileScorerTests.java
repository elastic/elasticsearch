/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.profile.query;

import org.apache.lucene.index.MultiReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ProfileScorerTests extends ESTestCase {

    private static class FakeScorer extends Scorer {

        public float maxScore, minCompetitiveScore;

        protected FakeScorer(Weight weight) {
            super(weight);
        }

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

    public void testPropagateMinCompetitiveScore() throws IOException {
        Query query = new MatchAllDocsQuery();
        Weight weight = query.createWeight(new IndexSearcher(new MultiReader()), ScoreMode.TOP_SCORES, 1f);
        FakeScorer fakeScorer = new FakeScorer(weight);
        QueryProfileBreakdown profile = new QueryProfileBreakdown();
        ProfileWeight profileWeight = new ProfileWeight(query, weight, profile);
        ProfileScorer profileScorer = new ProfileScorer(profileWeight, fakeScorer, profile);
        profileScorer.setMinCompetitiveScore(0.42f);
        assertEquals(0.42f, fakeScorer.minCompetitiveScore, 0f);
    }

    public void testPropagateMaxScore() throws IOException {
        Query query = new MatchAllDocsQuery();
        Weight weight = query.createWeight(new IndexSearcher(new MultiReader()), ScoreMode.TOP_SCORES, 1f);
        FakeScorer fakeScorer = new FakeScorer(weight);
        QueryProfileBreakdown profile = new QueryProfileBreakdown();
        ProfileWeight profileWeight = new ProfileWeight(query, weight, profile);
        ProfileScorer profileScorer = new ProfileScorer(profileWeight, fakeScorer, profile);
        profileScorer.setMinCompetitiveScore(0.42f);
        fakeScorer.maxScore = 42f;
        assertEquals(42f, profileScorer.getMaxScore(DocIdSetIterator.NO_MORE_DOCS), 0f);
    }
}
