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

package org.elasticsearch.index.similarity;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;
import org.elasticsearch.script.SimilarityScript;
import org.elasticsearch.script.SimilarityWeightScript;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

public class ScriptedSimilarityTests extends ESTestCase {

    public void testSameNormsAsBM25CountOverlaps() {
        doTestSameNormsAsBM25(false);
    }

    public void testSameNormsAsBM25DiscountOverlaps() {
        doTestSameNormsAsBM25(true);
    }

    private void doTestSameNormsAsBM25(boolean discountOverlaps) {
        ScriptedSimilarity sim1 = new ScriptedSimilarity("foobar", null, "foobaz", null, discountOverlaps);
        BM25Similarity sim2 = new BM25Similarity();
        sim2.setDiscountOverlaps(discountOverlaps);
        for (int iter = 0; iter < 100; ++iter) {
            final int length = TestUtil.nextInt(random(), 1, 100);
            final int position = random().nextInt(length);
            final int numOverlaps = random().nextInt(length);
            int maxTermFrequency = TestUtil.nextInt(random(), 1, 10);
            int uniqueTermCount = TestUtil.nextInt(random(), 1, 10);
            FieldInvertState state = new FieldInvertState(Version.LATEST.major, "foo", IndexOptions.DOCS_AND_FREQS, position, length,
                    numOverlaps, 100, maxTermFrequency, uniqueTermCount);
            assertEquals(
                    sim2.computeNorm(state),
                    sim1.computeNorm(state),
                    0f);
        }
    }

    public void testBasics() throws IOException {
        final AtomicBoolean called = new AtomicBoolean();
        SimilarityScript.Factory scriptFactory = () -> {
            return new SimilarityScript() {

                @Override
                public double execute(double weight, ScriptedSimilarity.Query query,
                        ScriptedSimilarity.Field field, ScriptedSimilarity.Term term,
                        ScriptedSimilarity.Doc doc) {

                    StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
                    if (Arrays.stream(stackTraceElements).anyMatch(ste -> {
                        return ste.getClassName().endsWith(".TermScorer") &&
                                ste.getMethodName().equals("score");
                            }) == false) {
                        // this might happen when computing max scores
                        return Float.MAX_VALUE;
                    }

                    assertEquals(1, weight, 0);
                    assertNotNull(doc);
                    assertEquals(2f, doc.getFreq(), 0);
                    assertEquals(3, doc.getLength(), 0);
                    assertNotNull(field);
                    assertEquals(3, field.getDocCount());
                    assertEquals(5, field.getSumDocFreq());
                    assertEquals(6, field.getSumTotalTermFreq());
                    assertNotNull(term);
                    assertEquals(2, term.getDocFreq());
                    assertEquals(3, term.getTotalTermFreq());
                    assertNotNull(query);
                    assertEquals(3.2f, query.getBoost(), 0);
                    called.set(true);
                    return 42f;
                }

            };
        };
        ScriptedSimilarity sim = new ScriptedSimilarity("foobar", null, "foobaz", scriptFactory, true);
        Directory dir = new ByteBuffersDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setSimilarity(sim));

        Document doc = new Document();
        doc.add(new TextField("f", "foo bar", Store.NO));
        doc.add(new StringField("match", "no", Store.NO));
        w.addDocument(doc);

        doc = new Document();
        doc.add(new TextField("f", "foo foo bar", Store.NO));
        doc.add(new StringField("match", "yes", Store.NO));
        w.addDocument(doc);

        doc = new Document();
        doc.add(new TextField("f", "bar", Store.NO));
        doc.add(new StringField("match", "no", Store.NO));
        w.addDocument(doc);

        IndexReader r = DirectoryReader.open(w);
        w.close();
        IndexSearcher searcher = new IndexSearcher(r);
        searcher.setSimilarity(sim);
        Query query = new BoostQuery(new BooleanQuery.Builder()
                .add(new TermQuery(new Term("f", "foo")), Occur.SHOULD)
                .add(new TermQuery(new Term("match", "yes")), Occur.FILTER)
                .build(), 3.2f);
        TopDocs topDocs = searcher.search(query, 1);
        assertEquals(1, topDocs.totalHits.value);
        assertTrue(called.get());
        assertEquals(42, topDocs.scoreDocs[0].score, 0);
        w.close();
        dir.close();
    }

    public void testInitScript() throws IOException {
        final AtomicBoolean initCalled = new AtomicBoolean();
        SimilarityWeightScript.Factory weightScriptFactory = () -> {
            return new SimilarityWeightScript() {

                @Override
                public double execute(ScriptedSimilarity.Query query, ScriptedSimilarity.Field field,
                    ScriptedSimilarity.Term term) {
                    assertEquals(3, field.getDocCount());
                    assertEquals(5, field.getSumDocFreq());
                    assertEquals(6, field.getSumTotalTermFreq());
                    assertNotNull(term);
                    assertEquals(1, term.getDocFreq());
                    assertEquals(2, term.getTotalTermFreq());
                    assertNotNull(query);
                    assertEquals(3.2f, query.getBoost(), 0);
                    initCalled.set(true);
                    return 28;
                }

            };
        };
        final AtomicBoolean called = new AtomicBoolean();
        SimilarityScript.Factory scriptFactory = () -> {
            return new SimilarityScript() {

                @Override
                public double execute(double weight, ScriptedSimilarity.Query query,
                        ScriptedSimilarity.Field field, ScriptedSimilarity.Term term,
                        ScriptedSimilarity.Doc doc) {

                    StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
                    if (Arrays.stream(stackTraceElements).anyMatch(ste -> {
                        return ste.getClassName().endsWith(".TermScorer") &&
                                ste.getMethodName().equals("score");
                            }) == false) {
                        // this might happen when computing max scores
                        return Float.MAX_VALUE;
                    }

                    assertEquals(28, weight, 0d);
                    assertNotNull(doc);
                    assertEquals(2f, doc.getFreq(), 0);
                    assertEquals(3, doc.getLength(), 0);
                    assertNotNull(field);
                    assertEquals(3, field.getDocCount());
                    assertEquals(5, field.getSumDocFreq());
                    assertEquals(6, field.getSumTotalTermFreq());
                    assertNotNull(term);
                    assertEquals(1, term.getDocFreq());
                    assertEquals(2, term.getTotalTermFreq());
                    assertNotNull(query);
                    assertEquals(3.2f, query.getBoost(), 0);
                    called.set(true);
                    return 42;
                }

            };
        };
        ScriptedSimilarity sim = new ScriptedSimilarity("foobar", weightScriptFactory, "foobaz", scriptFactory, true);
        Directory dir = new ByteBuffersDirectory();
        IndexWriter w = new IndexWriter(dir, newIndexWriterConfig().setSimilarity(sim));

        Document doc = new Document();
        doc.add(new TextField("f", "bar baz", Store.NO));
        w.addDocument(doc);

        doc = new Document();
        doc.add(new TextField("f", "foo foo bar", Store.NO));
        doc.add(new StringField("match", "yes", Store.NO));
        w.addDocument(doc);

        doc = new Document();
        doc.add(new TextField("f", "bar", Store.NO));
        w.addDocument(doc);

        IndexReader r = DirectoryReader.open(w);
        w.close();
        IndexSearcher searcher = new IndexSearcher(r);
        searcher.setSimilarity(sim);
        Query query = new BoostQuery(new TermQuery(new Term("f", "foo")), 3.2f);
        TopDocs topDocs = searcher.search(query, 1);
        assertEquals(1, topDocs.totalHits.value);
        assertTrue(initCalled.get());
        assertTrue(called.get());
        assertEquals(42, topDocs.scoreDocs[0].score, 0);
        w.close();
        dir.close();
    }
}
