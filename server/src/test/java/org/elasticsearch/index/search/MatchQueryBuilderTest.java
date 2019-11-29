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

package org.elasticsearch.index.search;

import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermAndBoost;
import org.apache.lucene.search.SynonymQuery;
import org.elasticsearch.index.mapper.KeywordFieldMapper;
import org.elasticsearch.index.search.MatchQuery.MatchQueryBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

/**
 * Unit tests for {@link MatchQueryBuilder}
 */
public class MatchQueryBuilderTest extends ESTestCase {

    public void testNewSynonymQuery() {
        MatchQueryBuilder mqb = new MatchQuery(null).new MatchQueryBuilder(new MockAnalyzer(random()),
                new KeywordFieldMapper.KeywordFieldType(), false, false);
        List<TermAndBoost> termsAndBoosts = new ArrayList<TermAndBoost>();

        SynonymQuery.Builder expected = new SynonymQuery.Builder("field");
        assertEquals(expected.build(), mqb.newSynonymQuery("field", termsAndBoosts));
        Term term = new Term("field","term1");

        // when all boosts are the same, we expect boosts in query to be the default of 1.0f
        termsAndBoosts.add(new TermAndBoost(term, MatchQuery.DEFAULT_SYNONYM_BOOST));
        expected.addTerm(term);
        assertEquals(expected.build(), mqb.newSynonymQuery("field", termsAndBoosts));
        termsAndBoosts.add(new TermAndBoost(term, MatchQuery.DEFAULT_SYNONYM_BOOST));
        expected.addTerm(term);
        assertEquals(expected.build(), mqb.newSynonymQuery("field", termsAndBoosts));

        // when a boost is different, we want to weigh the terms individually
        termsAndBoosts.add(new TermAndBoost(term, 1.0f));
        expected.addTerm(term, 1.0f);
        assertEquals(expected.build(), mqb.newSynonymQuery("field", termsAndBoosts));
    }

}
