/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

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
 *
 * Modifications copyright (C) 2020 Elasticsearch B.V.
 */

package org.elasticsearch.lucene.search;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CheckHits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.XCombinedFieldQuery;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.BooleanSimilarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.LMDirichletSimilarity;
import org.apache.lucene.search.similarities.LMJelinekMercerSimilarity;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;

import java.io.IOException;
import java.util.Arrays;

/**
 * Test for @link {@link XCombinedFieldQuery}
 * TODO remove once LUCENE 9999 is fixed and integrated and we remove our copy of the query
 *
 */
public class XCombinedFieldQueryTests extends LuceneTestCase {

    public void testRewrite() throws IOException {
        IndexReader reader = new MultiReader();
        IndexSearcher searcher = new IndexSearcher(reader);

        BooleanQuery query = new BooleanQuery.Builder()
            .add(new XCombinedFieldQuery.Builder()
                .addField("field1")
                .addField("field2")
                .addTerm(new BytesRef("value"))
                .build(), BooleanClause.Occur.SHOULD)
            .add(new XCombinedFieldQuery.Builder()
                .addField("field3")
                .addField("field4")
                .addTerm(new BytesRef("value"))
                .build(), BooleanClause.Occur.SHOULD)
            .build();
        assertEquals(query, searcher.rewrite(query));

        query = new BooleanQuery.Builder()
            .add(new XCombinedFieldQuery.Builder()
                .addField("field1", 2.0f)
                .addField("field2")
                .addTerm(new BytesRef("value"))
                .build(), BooleanClause.Occur.SHOULD)
            .add(new XCombinedFieldQuery.Builder()
                .addField("field1", 1.3f)
                .addField("field2")
                .addTerm(new BytesRef("value"))
                .build(), BooleanClause.Occur.SHOULD)
            .build();
        assertEquals(query, searcher.rewrite(query));
    }

    public void testNormsDisabled() throws IOException {
        Directory dir = newDirectory();
        Similarity similarity = randomCompatibleSimilarity();

        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setSimilarity(similarity);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

        Document doc = new Document();
        doc.add(new StringField("a", "value", Store.NO));
        doc.add(new StringField("b", "value", Store.NO));
        doc.add(new TextField("c", "value", Store.NO));
        w.addDocument(doc);
        w.commit();

        doc = new Document();
        doc.add(new StringField("a", "value", Store.NO));
        doc.add(new TextField("c", "value", Store.NO));
        w.addDocument(doc);

        IndexReader reader = w.getReader();
        IndexSearcher searcher = newSearcher(reader);

        Similarity searchSimilarity = randomCompatibleSimilarity();
        searcher.setSimilarity(searchSimilarity);
        TopScoreDocCollector collector = TopScoreDocCollector.create(10, null, 10);

        XCombinedFieldQuery query =
            new XCombinedFieldQuery.Builder()
                .addField("a", 1.0f)
                .addField("b", 1.0f)
                .addTerm(new BytesRef("value"))
                .build();
        searcher.search(query, collector);
        TopDocs topDocs = collector.topDocs();
        assertEquals(new TotalHits(2, TotalHits.Relation.EQUAL_TO), topDocs.totalHits);

        TopScoreDocCollector invalidCollector = TopScoreDocCollector.create(10, null, 10);
        XCombinedFieldQuery invalidQuery =
            new XCombinedFieldQuery.Builder()
                .addField("b", 1.0f)
                .addField("c", 1.0f)
                .addTerm(new BytesRef("value"))
                .build();
        IllegalArgumentException e =
            expectThrows(
                IllegalArgumentException.class, () -> searcher.search(invalidQuery, invalidCollector));
        assertTrue(e.getMessage().contains("requires norms to be consistent across fields"));

        reader.close();
        w.close();
        dir.close();
    }

    public void testCopyFieldWithMissingFields() throws IOException {
        Directory dir = new MMapDirectory(createTempDir());
        Similarity similarity = randomCompatibleSimilarity();

        IndexWriterConfig iwc = new IndexWriterConfig();
        iwc.setSimilarity(similarity);
        RandomIndexWriter w = new RandomIndexWriter(random(), dir, iwc);

        int boost1 = Math.max(1, random().nextInt(5));
        int boost2 = Math.max(1, random().nextInt(5));
        int numMatch = atLeast(10);
        for (int i = 0; i < numMatch; i++) {
            Document doc = new Document();
            int freqA = random().nextInt(5) + 1;
            for (int j = 0; j < freqA; j++) {
                doc.add(new TextField("a", "foo", Store.NO));
            }

            // Choose frequencies such that sometimes we don't add field B
            int freqB = random().nextInt(3);
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
        XCombinedFieldQuery query = new XCombinedFieldQuery.Builder().addField("a", boost1)
            .addField("b", boost2)
            .addTerm(new BytesRef("foo"))
            .build();

        checkExpectedHits(searcher, numMatch, query, new TermQuery(new Term("ab", "foo")));

        reader.close();
        w.close();
        dir.close();
    }

    private void checkExpectedHits(IndexSearcher searcher, int numHits, Query firstQuery, Query secondQuery) throws IOException {
        TopScoreDocCollector firstCollector = TopScoreDocCollector.create(numHits, null, Integer.MAX_VALUE);
        searcher.search(firstQuery, firstCollector);
        TopDocs firstTopDocs = firstCollector.topDocs();
        assertEquals(numHits, firstTopDocs.totalHits.value);
        TopScoreDocCollector secondCollector = TopScoreDocCollector.create(numHits, null, Integer.MAX_VALUE);
        searcher.search(secondQuery, secondCollector);
        TopDocs secondTopDocs = secondCollector.topDocs();
        CheckHits.checkEqual(firstQuery, secondTopDocs.scoreDocs, firstTopDocs.scoreDocs);
    }

    private static Similarity randomCompatibleSimilarity() {
        return RandomPicks.randomFrom(
            random(),
            Arrays.asList(
                new BM25Similarity(),
                new BooleanSimilarity(),
                new ClassicSimilarity(),
                new LMDirichletSimilarity(),
                new LMJelinekMercerSimilarity(0.1f)
            )
        );
    }
}
