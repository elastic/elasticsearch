/* @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.search;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInvertState;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

/**
 * TODO: this is temporarily copied from Lucene, remove once we update to Lucene 8.9.
 */
public class XCombinedFieldQueryTests extends LuceneTestCase {
    public void testInvalid() {
        XCombinedFieldQuery.Builder builder = new XCombinedFieldQuery.Builder();
        IllegalArgumentException exc =
            expectThrows(IllegalArgumentException.class, () -> builder.addField("foo", 0.5f));
        assertEquals(exc.getMessage(), "weight must be greater or equal to 1");
    }

    public void testRewrite() throws IOException {
        XCombinedFieldQuery.Builder builder = new XCombinedFieldQuery.Builder();
        IndexReader reader = new MultiReader();
        IndexSearcher searcher = new IndexSearcher(reader);
        Query actual = searcher.rewrite(builder.build());
        assertEquals(actual, new MatchNoDocsQuery());
        builder.addField("field", 1f);
        actual = searcher.rewrite(builder.build());
        assertEquals(actual, new MatchNoDocsQuery());
        builder.addTerm(new BytesRef("foo"));
        actual = searcher.rewrite(builder.build());
        assertEquals(actual, new TermQuery(new Term("field", "foo")));
        builder.addTerm(new BytesRef("bar"));
        actual = searcher.rewrite(builder.build());
        assertEquals(
            actual,
            new SynonymQuery.Builder("field")
                .addTerm(new Term("field", "foo"))
                .addTerm(new Term("field", "bar"))
                .build());
        builder.addField("another_field", 1f);
        Query query = builder.build();
        actual = searcher.rewrite(query);
        assertEquals(actual, query);
    }

    public void testToString() {
        assertEquals("CombinedFieldQuery(()())", new XCombinedFieldQuery.Builder().build().toString());
        XCombinedFieldQuery.Builder builder = new XCombinedFieldQuery.Builder();
        builder.addField("foo", 1f);
        assertEquals("CombinedFieldQuery((foo)())", builder.build().toString());
        builder.addTerm(new BytesRef("bar"));
        assertEquals("CombinedFieldQuery((foo)(bar))", builder.build().toString());
        builder.addField("title", 3f);
        assertEquals("CombinedFieldQuery((foo title^3.0)(bar))", builder.build().toString());
        builder.addTerm(new BytesRef("baz"));
        assertEquals("CombinedFieldQuery((foo title^3.0)(bar baz))", builder.build().toString());
    }

    public void testSameScore() throws IOException {
        Directory dir = newDirectory();
        Similarity similarity = randomCompatibleSimilarity();

        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setSimilarity(similarity);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

        Document doc = new Document();
        doc.add(new StringField("f", "a", Store.NO));
        w.addDocument(doc);

        doc = new Document();
        doc.add(new StringField("g", "a", Store.NO));
        for (int i = 0; i < 10; ++i) {
            w.addDocument(doc);
        }

        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);
        searcher.setSimilarity(similarity);
        XCombinedFieldQuery query =
            new XCombinedFieldQuery.Builder()
                .addField("f", 1f)
                .addField("g", 1f)
                .addTerm(new BytesRef("a"))
                .build();
        TopScoreDocCollector collector =
            TopScoreDocCollector.create(
                Math.min(reader.numDocs(), Integer.MAX_VALUE), null, Integer.MAX_VALUE);
        searcher.search(query, collector);
        TopDocs topDocs = collector.topDocs();
        assertEquals(new TotalHits(11, TotalHits.Relation.EQUAL_TO), topDocs.totalHits);
        // All docs must have the same score
        for (int i = 0; i < topDocs.scoreDocs.length; ++i) {
            assertEquals(topDocs.scoreDocs[0].score, topDocs.scoreDocs[i].score, 0.0f);
        }

        reader.close();
        w.close();
        dir.close();
    }

    public void testCopyField() throws IOException {
        Directory dir = newDirectory();
        Similarity similarity = randomCompatibleSimilarity();

        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setSimilarity(similarity);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

        int numMatch = atLeast(10);
        int boost1 = Math.max(1, random().nextInt(5));
        int boost2 = Math.max(1, random().nextInt(5));
        for (int i = 0; i < numMatch; i++) {
            Document doc = new Document();
            if (random().nextBoolean()) {
                doc.add(new TextField("a", "baz", Store.NO));
                doc.add(new TextField("b", "baz", Store.NO));
                for (int k = 0; k < boost1 + boost2; k++) {
                    doc.add(new TextField("ab", "baz", Store.NO));
                }
                w.addDocument(doc);
                doc.clear();
            }
            int freqA = random().nextInt(5) + 1;
            for (int j = 0; j < freqA; j++) {
                doc.add(new TextField("a", "foo", Store.NO));
            }
            int freqB = random().nextInt(5) + 1;
            for (int j = 0; j < freqB; j++) {
                doc.add(new TextField("b", "foo", Store.NO));
            }
            int freqAB = freqA * boost1 + freqB * boost2;
            for (int j = 0; j < freqAB; j++) {
                doc.add(new TextField("ab", "foo", Store.NO));
            }
            w.addDocument(doc);
        }
        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);

        searcher.setSimilarity(similarity);
        XCombinedFieldQuery query =
            new XCombinedFieldQuery.Builder()
                .addField("a", (float) boost1)
                .addField("b", (float) boost2)
                .addTerm(new BytesRef("foo"))
                .build();

        checkExpectedHits(searcher, numMatch, query, new TermQuery(new Term("ab", "foo")));

        reader.close();
        w.close();
        dir.close();
    }

    public void testCopyFieldWithMultipleTerms() throws IOException {
        Directory dir = newDirectory();
        Similarity similarity = randomCompatibleSimilarity();

        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setSimilarity(similarity);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

        int numMatch = atLeast(10);
        int boost1 = Math.max(1, random().nextInt(5));
        int boost2 = Math.max(1, random().nextInt(5));
        for (int i = 0; i < numMatch; i++) {
            Document doc = new Document();

            int freqA = random().nextInt(5) + 1;
            for (int j = 0; j < freqA; j++) {
                doc.add(new TextField("a", "foo", Store.NO));
            }
            int freqB = random().nextInt(5) + 1;
            for (int j = 0; j < freqB; j++) {
                doc.add(new TextField("b", "bar", Store.NO));
            }
            int freqAB = freqA * boost1 + freqB * boost2;
            for (int j = 0; j < freqAB; j++) {
                doc.add(new TextField("ab", "foo", Store.NO));
            }
            w.addDocument(doc);
        }
        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);

        searcher.setSimilarity(similarity);
        XCombinedFieldQuery query =
            new XCombinedFieldQuery.Builder()
                .addField("a", (float) boost1)
                .addField("b", (float) boost2)
                .addTerm(new BytesRef("foo"))
                .addTerm(new BytesRef("bar"))
                .build();

        checkExpectedHits(searcher, numMatch, query, new TermQuery(new Term("ab", "foo")));

        reader.close();
        w.close();
        dir.close();
    }

    private static Similarity randomCompatibleSimilarity() {
        return RandomPicks.randomFrom(
            random(),
            Arrays.asList(
                new BM25Similarity(),
                new BooleanSimilarity(),
                new ClassicSimilarity(),
                new LMDirichletSimilarity(),
                new LMJelinekMercerSimilarity(0.1f)));
    }

    private void checkExpectedHits(
        IndexSearcher searcher, int numHits, Query firstQuery, Query secondQuery) throws IOException {
        TopScoreDocCollector firstCollector =
            TopScoreDocCollector.create(numHits, null, Integer.MAX_VALUE);
        searcher.search(firstQuery, firstCollector);
        TopDocs firstTopDocs = firstCollector.topDocs();
        assertEquals(numHits, firstTopDocs.totalHits.value);

        TopScoreDocCollector secondCollector =
            TopScoreDocCollector.create(numHits, null, Integer.MAX_VALUE);
        searcher.search(secondQuery, secondCollector);
        TopDocs secondTopDocs = secondCollector.topDocs();
        CheckHits.checkEqual(firstQuery, secondTopDocs.scoreDocs, firstTopDocs.scoreDocs);
    }

    public void testDocWithNegativeNorms() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setSimilarity(new NegativeNormSimilarity());
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

        String queryString = "foo";

        Document doc = new Document();
        // both fields must contain tokens that match the query string "foo"
        doc.add(new TextField("f", "foo", Store.NO));
        doc.add(new TextField("g", "foo baz", Store.NO));
        w.addDocument(doc);

        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);
        searcher.setSimilarity(new BM25Similarity());
        XCombinedFieldQuery query =
            new XCombinedFieldQuery.Builder()
                .addField("f")
                .addField("g")
                .addTerm(new BytesRef(queryString))
                .build();
        TopDocs topDocs = searcher.search(query, 10);
        CheckHits.checkDocIds("queried docs do not match", new int[] {0}, topDocs.scoreDocs);

        reader.close();
        w.close();
        dir.close();
    }

    public void testMultipleDocsNegativeNorms() throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setSimilarity(new NegativeNormSimilarity());
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

        String queryString = "foo";

        Document doc0 = new Document();
        doc0.add(new TextField("f", "foo", Store.NO));
        doc0.add(new TextField("g", "foo baz", Store.NO));
        w.addDocument(doc0);

        Document doc1 = new Document();
        // add another match on the query string to the second doc
        doc1.add(new TextField("f", "foo is foo", Store.NO));
        doc1.add(new TextField("g", "foo baz", Store.NO));
        w.addDocument(doc1);

        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);
        searcher.setSimilarity(new BM25Similarity());
        XCombinedFieldQuery query =
            new XCombinedFieldQuery.Builder()
                .addField("f")
                .addField("g")
                .addTerm(new BytesRef(queryString))
                .build();
        TopDocs topDocs = searcher.search(query, 10);
        // Return doc1 ahead of doc0 since its tf is higher
        CheckHits.checkDocIds("queried docs do not match", new int[] {1, 0}, topDocs.scoreDocs);

        reader.close();
        w.close();
        dir.close();
    }

    private static final class NegativeNormSimilarity extends Similarity {
        @Override
        public long computeNorm(FieldInvertState state) {
            return -128;
        }

        @Override
        public SimScorer scorer(
            float boost, CollectionStatistics collectionStats, TermStatistics... termStats) {
            return new BM25Similarity().scorer(boost, collectionStats, termStats);
        }
    }
}
