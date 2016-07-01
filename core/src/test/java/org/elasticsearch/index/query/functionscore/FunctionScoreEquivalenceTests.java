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
package org.elasticsearch.index.query.functionscore;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RandomApproximationQuery;
import org.apache.lucene.search.SearchEquivalenceTestBase;
import org.apache.lucene.search.TermQuery;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery.FilterFunction;
import org.elasticsearch.common.lucene.search.function.FiltersFunctionScoreQuery.ScoreMode;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;

public class FunctionScoreEquivalenceTests extends SearchEquivalenceTestBase {

    public void testMinScoreAllIncluded() throws Exception {
        Term term = randomTerm();
        Query query = new TermQuery(term);

        FunctionScoreQuery fsq = new FunctionScoreQuery(query, null, 0f, null, Float.POSITIVE_INFINITY);
        assertSameScores(query, fsq);

        FiltersFunctionScoreQuery ffsq = new FiltersFunctionScoreQuery(query, ScoreMode.SUM, new FilterFunction[0], Float.POSITIVE_INFINITY,
                0f, CombineFunction.MULTIPLY);
        assertSameScores(query, ffsq);
    }

    public void testMinScoreAllExcluded() throws Exception {
        Term term = randomTerm();
        Query query = new TermQuery(term);

        FunctionScoreQuery fsq = new FunctionScoreQuery(query, null, Float.POSITIVE_INFINITY, null, Float.POSITIVE_INFINITY);
        assertSameScores(new MatchNoDocsQuery(), fsq);

        FiltersFunctionScoreQuery ffsq = new FiltersFunctionScoreQuery(query, ScoreMode.SUM, new FilterFunction[0], Float.POSITIVE_INFINITY,
                Float.POSITIVE_INFINITY, CombineFunction.MULTIPLY);
        assertSameScores(new MatchNoDocsQuery(), ffsq);
    }

    public void testTwoPhaseMinScore() throws Exception {
        Term term = randomTerm();
        Query query = new TermQuery(term);
        Float minScore = random().nextFloat();

        FunctionScoreQuery fsq1 = new FunctionScoreQuery(query, null, minScore, null, Float.POSITIVE_INFINITY);
        FunctionScoreQuery fsq2 = new FunctionScoreQuery(new RandomApproximationQuery(query, random()), null, minScore, null,
                Float.POSITIVE_INFINITY);
        assertSameScores(fsq1, fsq2);

        FiltersFunctionScoreQuery ffsq1 = new FiltersFunctionScoreQuery(query, ScoreMode.SUM, new FilterFunction[0],
                Float.POSITIVE_INFINITY, minScore, CombineFunction.MULTIPLY);
        FiltersFunctionScoreQuery ffsq2 = new FiltersFunctionScoreQuery(query, ScoreMode.SUM, new FilterFunction[0],
                Float.POSITIVE_INFINITY, minScore, CombineFunction.MULTIPLY);
        assertSameScores(ffsq1, ffsq2);
    }

}
