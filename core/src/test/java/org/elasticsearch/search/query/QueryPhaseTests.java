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

package org.elasticsearch.search.query;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.query.ParsedQuery;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TestSearchContext;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class QueryPhaseTests extends ESTestCase {

    private void countTestCase(Query query, IndexReader reader, boolean shouldCollect) throws Exception {
        TestSearchContext context = new TestSearchContext(null);
        context.parsedQuery(new ParsedQuery(query));
        context.setSize(0);

        IndexSearcher searcher = new IndexSearcher(reader);
        final AtomicBoolean collected = new AtomicBoolean();
        IndexSearcher contextSearcher = new IndexSearcher(reader) {
            protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
                collected.set(true);
                super.search(leaves, weight, collector);
            }
        };

        final boolean rescore = QueryPhase.execute(context, contextSearcher);
        assertFalse(rescore);
        assertEquals(searcher.count(query), context.queryResult().topDocs().totalHits);
        assertEquals(shouldCollect, collected.get());
    }

    private void countTestCase(boolean withDeletions) throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);
        final int numDocs = scaledRandomIntBetween(100, 200);
        for (int i = 0; i < numDocs; ++i) {
            Document doc = new Document();
            if (randomBoolean()) {
                doc.add(new StringField("foo", "bar", Store.NO));
            }
            if (randomBoolean()) {
                doc.add(new StringField("foo", "baz", Store.NO));
            }
            if (withDeletions && (rarely() || i == 0)) {
                doc.add(new StringField("delete", "yes", Store.NO));
            }
            w.addDocument(doc);
        }
        if (withDeletions) {
            w.deleteDocuments(new Term("delete", "yes"));
        }
        final IndexReader reader = w.getReader();
        Query matchAll = new MatchAllDocsQuery();
        Query matchAllCsq = new ConstantScoreQuery(matchAll);
        Query tq = new TermQuery(new Term("foo", "bar"));
        Query tCsq = new ConstantScoreQuery(tq);
        BooleanQuery bq = new BooleanQuery.Builder()
            .add(matchAll, Occur.SHOULD)
            .add(tq, Occur.MUST)
            .build();

        countTestCase(matchAll, reader, false);
        countTestCase(matchAllCsq, reader, false);
        countTestCase(tq, reader, withDeletions);
        countTestCase(tCsq, reader, withDeletions);
        countTestCase(bq, reader, true);
        reader.close();
        w.close();
        dir.close();
    }

    public void testCountWithoutDeletions() throws Exception {
        countTestCase(false);
    }

    public void testCountWithDeletions() throws Exception {
        countTestCase(true);
    }

    public void testPostFilterDisablesCountOptimization() throws Exception {
        TestSearchContext context = new TestSearchContext(null);
        context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
        context.setSize(0);

        final AtomicBoolean collected = new AtomicBoolean();
        IndexSearcher contextSearcher = new IndexSearcher(new MultiReader()) {
            protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
                collected.set(true);
                super.search(leaves, weight, collector);
            }
        };

        QueryPhase.execute(context, contextSearcher);
        assertEquals(0, context.queryResult().topDocs().totalHits);
        assertFalse(collected.get());

        context.parsedPostFilter(new ParsedQuery(new MatchNoDocsQuery()));
        QueryPhase.execute(context, contextSearcher);
        assertEquals(0, context.queryResult().topDocs().totalHits);
        assertTrue(collected.get());
    }

    public void testMinScoreDisablesCountOptimization() throws Exception {
        TestSearchContext context = new TestSearchContext(null);
        context.parsedQuery(new ParsedQuery(new MatchAllDocsQuery()));
        context.setSize(0);

        final AtomicBoolean collected = new AtomicBoolean();
        IndexSearcher contextSearcher = new IndexSearcher(new MultiReader()) {
            protected void search(List<LeafReaderContext> leaves, Weight weight, Collector collector) throws IOException {
                collected.set(true);
                super.search(leaves, weight, collector);
            }
        };

        QueryPhase.execute(context, contextSearcher);
        assertEquals(0, context.queryResult().topDocs().totalHits);
        assertFalse(collected.get());

        context.minimumScore(1);
        QueryPhase.execute(context, contextSearcher);
        assertEquals(0, context.queryResult().topDocs().totalHits);
        assertTrue(collected.get());
    }

}
