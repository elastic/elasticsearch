/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.query.functionscore;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RandomApproximationQuery;
import org.apache.lucene.search.SearchEquivalenceTestBase;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.bootstrap.BootstrapForTesting;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;

public class FunctionScoreEquivalenceTests extends SearchEquivalenceTestBase {
    static {
        try {
            Class.forName("org.elasticsearch.test.ESTestCase");
        } catch (ClassNotFoundException e) {
            throw new AssertionError(e);
        }
        BootstrapForTesting.ensureInitialized();
    }

    public void testMinScoreAllIncluded() throws Exception {
        Term term = randomTerm();
        Query query = new TermQuery(term);

        FunctionScoreQuery fsq = new FunctionScoreQuery(query, null, Float.POSITIVE_INFINITY);
        assertSameScores(query, fsq);

        FunctionScoreQuery ffsq = new FunctionScoreQuery(query, 0f, Float.POSITIVE_INFINITY);
        assertSameScores(query, ffsq);
    }

    public void testMinScoreAllExcluded() throws Exception {
        Term term = randomTerm();
        Query query = new TermQuery(term);

        FunctionScoreQuery fsq = new FunctionScoreQuery(query, Float.POSITIVE_INFINITY, Float.POSITIVE_INFINITY);
        assertSameScores(new MatchNoDocsQuery(), fsq);
    }

    public void testTwoPhaseMinScore() throws Exception {
        Term term = randomTerm();
        Query query = new TermQuery(term);
        Float minScore = random().nextFloat();

        FunctionScoreQuery fsq1 = new FunctionScoreQuery(query, minScore, Float.POSITIVE_INFINITY);
        FunctionScoreQuery fsq2 = new FunctionScoreQuery(new RandomApproximationQuery(query, random()), minScore, Float.POSITIVE_INFINITY);
        assertSameScores(fsq1, fsq2);

        FunctionScoreQuery ffsq1 = new FunctionScoreQuery(query, minScore, Float.POSITIVE_INFINITY);
        FunctionScoreQuery ffsq2 = new FunctionScoreQuery(query, minScore, Float.POSITIVE_INFINITY);
        assertSameScores(ffsq1, ffsq2);
    }
}
