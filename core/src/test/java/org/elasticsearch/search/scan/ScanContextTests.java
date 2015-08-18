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

package org.elasticsearch.search.scan;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.search.scan.ScanContext.ScanCollector;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ScanContextTests extends ESTestCase {

    private static TopDocs execute(IndexSearcher searcher, ScanContext ctx, Query query, int pageSize, boolean trackScores) throws IOException {
        query = ctx.wrapQuery(query);
        ScanCollector collector = ctx.collector(pageSize, trackScores);
        searcher.search(query, collector);
        return collector.topDocs();
    }

    public void testRandom() throws Exception {
        final int numDocs = randomIntBetween(10, 200);
        final Document doc1 = new Document();
        doc1.add(new StringField("foo", "bar", Store.NO));
        final Document doc2 = new Document();
        final Directory dir = newDirectory();
        final RandomIndexWriter w = new RandomIndexWriter(getRandom(), dir);
        for (int i = 0; i < numDocs; ++i) {
            w.addDocument(randomBoolean() ? doc1 : doc2);
        }
        final IndexReader reader = w.getReader();
        final IndexSearcher searcher = newSearcher(reader);

        final boolean trackScores = randomBoolean();
        final int pageSize = randomIntBetween(1, numDocs / 2);
        Query query = new TermQuery(new Term("foo", "bar"));
        if (trackScores == false) {
            query.setBoost(0f);
        }
        final ScoreDoc[] expected = searcher.search(query, numDocs, Sort.INDEXORDER, true, true).scoreDocs;

        final List<ScoreDoc> actual = new ArrayList<>();
        ScanContext context = new ScanContext();
        while (true) {
            final ScoreDoc[] page = execute(searcher,context, query, pageSize, trackScores).scoreDocs;
            assertTrue(page.length <= pageSize);
            if (page.length == 0) {
                assertEquals(0, execute(searcher, context, query, pageSize, trackScores).scoreDocs.length);
                break;
            }
            actual.addAll(Arrays.asList(page));
        }
        assertEquals(expected.length, actual.size());
        for (int i = 0; i < expected.length; ++i) {
            ScoreDoc sd1 = expected[i];
            ScoreDoc sd2 = actual.get(i);
            assertEquals(sd1.doc, sd2.doc);
            assertEquals(sd1.score, sd2.score, 0.001f);
        }
        w.close();
        reader.close();
        dir.close();
    }

}
