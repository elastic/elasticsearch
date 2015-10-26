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

package org.elasticsearch.common.lucene.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.MockTokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

public class MinScoreQueryTests extends ESTestCase {

    private static String[] terms;
    private static Directory dir;
    private static IndexReader r;
    private static IndexSearcher s;

    @BeforeClass
    public static void before() throws IOException {
        dir = newDirectory();
        terms = new String[TestUtil.nextInt(random(), 2, 15)];
        for (int i = 0; i < terms.length; ++i) {
            terms[i] = TestUtil.randomSimpleString(random());
        }
        final int numDocs = TestUtil.nextInt(random(), 1, 200);
        Analyzer analyzer = new MockAnalyzer(random(), MockTokenizer.WHITESPACE, false);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, newIndexWriterConfig(analyzer));
        for (int i = 0; i < numDocs; ++i) {
            StringBuilder value = new StringBuilder();
            final int numTerms = random().nextInt(10);
            for (int j = 0; j < numTerms; ++j) {
                if (j > 0) {
                    value.append(' ');
                }
                // simulate zipf distribution
                String term = terms[TestUtil.nextInt(random(), 0, TestUtil.nextInt(random(), 0, terms.length - 1))];
                value.append(term);
            }
            Document doc = new Document();
            doc.add(new TextField("field", value.toString(), Store.NO));
            w.addDocument(doc);
        }
        r = w.getReader();
        s = newSearcher(r);
        w.close();
    }

    @AfterClass
    public static void after() throws IOException {
        IOUtils.close(r, dir);
        terms = null;
        r = null;
        s = null;
        dir = null;
    }

    private static Term randomTerm() {
        return new Term("field", terms[random().nextInt(terms.length)]);
    }

    public void testEquals() throws IOException {
        QueryUtils.checkEqual(new MinScoreQuery(new MatchAllDocsQuery(), 0.5f), new MinScoreQuery(new MatchAllDocsQuery(), 0.5f));
        QueryUtils.checkUnequal(new MinScoreQuery(new MatchAllDocsQuery(), 0.5f), new MinScoreQuery(new MatchAllDocsQuery(), 0.3f));
        QueryUtils.checkUnequal(new MinScoreQuery(new MatchAllDocsQuery(), 0.5f), new MinScoreQuery(new MatchNoDocsQuery(), 0.5f));

        IndexSearcher s1 = new IndexSearcher(new MultiReader());
        IndexSearcher s2 = new IndexSearcher(new MultiReader());
        QueryUtils.checkEqual(new MinScoreQuery(new MatchAllDocsQuery(), 0.5f, s1), new MinScoreQuery(new MatchAllDocsQuery(), 0.5f, s1));
        QueryUtils.checkUnequal(new MinScoreQuery(new MatchAllDocsQuery(), 0.5f, s1), new MinScoreQuery(new MatchAllDocsQuery(), 0.5f, s2));
    }

    /** pick a min score which is in the range of scores produced by the query */
    private float randomMinScore(Query query) throws IOException {
        TopDocs topDocs = s.search(query, 2);
        float base = 0;
        switch (topDocs.totalHits) {
        case 0:
            break;
        case 1:
            base = topDocs.scoreDocs[0].score;
            break;
        default:
            base = (topDocs.scoreDocs[0].score + topDocs.scoreDocs[1].score) / 2;
            break;
        }
        float delta = random().nextFloat() - 0.5f;
        return base + delta;
    }

    private void assertMinScoreEquivalence(Query query, Query minScoreQuery, float minScore) throws IOException {
        final TopDocs topDocs = s.search(query, s.getIndexReader().maxDoc());
        final TopDocs minScoreTopDocs = s.search(minScoreQuery, s.getIndexReader().maxDoc());

        int j = 0;
        for (int i = 0; i < topDocs.totalHits; ++i) {
            if (topDocs.scoreDocs[i].score >= minScore) {
                assertEquals(topDocs.scoreDocs[i].doc, minScoreTopDocs.scoreDocs[j].doc);
                assertEquals(topDocs.scoreDocs[i].score, minScoreTopDocs.scoreDocs[j].score, 1e-5f);
                j++;
            }
        }
        assertEquals(minScoreTopDocs.totalHits, j);
    }

    public void testBasics() throws Exception {
        final int iters = 5;
        for (int iter = 0; iter < iters; ++iter) {
            Term term = randomTerm();
            Query query = new TermQuery(term);
            float minScore = randomMinScore(query);
            Query minScoreQuery = new MinScoreQuery(query, minScore);
            assertMinScoreEquivalence(query, minScoreQuery, minScore);
        }
    }

    public void testFilteredApproxQuery() throws Exception {
        // same as testBasics but with a query that exposes approximations
        final int iters = 5;
        for (int iter = 0; iter < iters; ++iter) {
            Term term = randomTerm();
            Query query = new TermQuery(term);
            float minScore = randomMinScore(query);
            Query minScoreQuery = new MinScoreQuery(new RandomApproximationQuery(query, random()), minScore);
            assertMinScoreEquivalence(query, minScoreQuery, minScore);
        }
    }

    public void testNestedInConjunction() throws Exception {
        // To test scorers as well, not only bulk scorers
        Term t1 = randomTerm();
        Term t2 = randomTerm();
        Query tq1 = new TermQuery(t1);
        Query tq2 = new TermQuery(t2);
        float minScore = randomMinScore(tq1);
        BooleanQuery bq1 = new BooleanQuery.Builder()
            .add(new MinScoreQuery(tq1, minScore), Occur.MUST)
            .add(tq2, Occur.FILTER)
            .build();
        BooleanQuery bq2 = new BooleanQuery.Builder()
            .add(tq1, Occur.MUST)
            .add(tq2, Occur.FILTER)
            .build();
        assertMinScoreEquivalence(bq2, bq1, minScore);
    }

    public void testNestedInConjunctionWithApprox() throws Exception {
        // same, but with approximations
        Term t1 = randomTerm();
        Term t2 = randomTerm();
        Query tq1 = new TermQuery(t1);
        Query tq2 = new TermQuery(t2);
        float minScore = randomMinScore(tq1);
        BooleanQuery bq1 = new BooleanQuery.Builder()
            .add(new MinScoreQuery(new RandomApproximationQuery(tq1, random()), minScore), Occur.MUST)
            .add(tq2, Occur.FILTER)
            .build();
        BooleanQuery bq2 = new BooleanQuery.Builder()
            .add(tq1, Occur.MUST)
            .add(tq2, Occur.FILTER)
            .build();
        assertMinScoreEquivalence(bq2, bq1, minScore);
    }
}
