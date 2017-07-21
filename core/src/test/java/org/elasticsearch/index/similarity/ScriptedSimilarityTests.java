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
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.Version;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class ScriptedSimilarityTests extends ESTestCase {

    public void testSameNormsAsBM25CountOverlaps() {
        doTestSameNormsAsBM25(false);
    }

    public void testSameNormsAsBM25DiscountOverlaps() {
        doTestSameNormsAsBM25(true);
    }

    private void doTestSameNormsAsBM25(boolean discountOverlaps) {
        ScriptedSimilarity sim1 = new ScriptedSimilarity("foobar", null, discountOverlaps);
        BM25Similarity sim2 = new BM25Similarity();
        sim2.setDiscountOverlaps(discountOverlaps);
        for (int iter = 0; iter < 100; ++iter) {
            final int length = TestUtil.nextInt(random(), 1, 100);
            final int position = random().nextInt(length);
            final int numOverlaps = random().nextInt(length);
            FieldInvertState state = new FieldInvertState(Version.LATEST.major, "foo", position, length, numOverlaps, 100);
            assertEquals(
                    sim2.computeNorm(state),
                    sim1.computeNorm(state),
                    0f);
        }
    }

    public void testBasics() throws IOException {
        final AtomicBoolean called = new AtomicBoolean();
        Supplier<ExecutableScript> scriptSupplier = () -> {
            return new ExecutableScript() {

                private ScriptedSimilarity.Stats stats;

                @Override
                public void setNextVar(String name, Object value) {
                    switch (name) {
                    case "stats":
                        stats = (ScriptedSimilarity.Stats) value;
                        break;
                    default:
                        throw new AssertionError(name);
                    }
                }

                @Override
                public Object run() {
                    assertNotNull(stats);
                    assertNotNull(stats.doc);
                    assertEquals(2f, stats.doc.getFreq(), 0);
                    assertEquals(3, stats.doc.getLength(), 0);
                    assertNotNull(stats.field);
                    assertEquals(3, stats.field.getDocCount());
                    assertEquals(5, stats.field.getSumDocFreq());
                    assertEquals(6, stats.field.getSumTotalTermFreq());
                    assertNotNull(stats.term);
                    assertEquals(2, stats.term.getDocFreq());
                    assertEquals(3, stats.term.getTotalTermFreq());
                    assertNotNull(stats.query);
                    assertEquals(3.2f, stats.query.getBoost(), 0);
                    called.set(true);
                    return 42f;
                }
                
            };
        };
        ScriptedSimilarity sim = new ScriptedSimilarity("foobar", scriptSupplier, true);
        Directory dir = new RAMDirectory();
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
        assertEquals(1, topDocs.totalHits);
        assertTrue(called.get());
        assertEquals(42, topDocs.scoreDocs[0].score, 0);
        w.close();
        dir.close();
    }

}
