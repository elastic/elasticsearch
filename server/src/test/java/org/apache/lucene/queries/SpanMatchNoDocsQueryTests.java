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

package org.apache.lucene.queries;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.spans.SpanNearQuery;
import org.apache.lucene.search.spans.SpanOrQuery;
import org.apache.lucene.search.spans.SpanQuery;
import org.apache.lucene.search.spans.SpanTermQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class SpanMatchNoDocsQueryTests extends ESTestCase {
    public void testSimple() throws Exception {
        SpanMatchNoDocsQuery query = new SpanMatchNoDocsQuery("field", "a good reason");
        assertEquals(query.toString(), "SpanMatchNoDocsQuery(\"a good reason\")");
        Query rewrite = query.rewrite(null);
        assertTrue(rewrite instanceof SpanMatchNoDocsQuery);
        assertEquals(rewrite.toString(), "SpanMatchNoDocsQuery(\"a good reason\")");
    }

    public void testQuery() throws Exception {
        Directory dir = newDirectory();
        Analyzer analyzer = new MockAnalyzer(random());
        IndexWriter iw = new IndexWriter(dir,
            newIndexWriterConfig(analyzer).setMaxBufferedDocs(2).setMergePolicy(newLogMergePolicy()));
        addDoc("one", iw);
        addDoc("two", iw);
        addDoc("three", iw);
        IndexReader ir = DirectoryReader.open(iw);
        IndexSearcher searcher = new IndexSearcher(ir);

        Query query = new SpanMatchNoDocsQuery("unknown", "field not found");
        assertEquals(searcher.count(query), 0);

        ScoreDoc[] hits;
        hits = searcher.search(query, 1000).scoreDocs;
        assertEquals(0, hits.length);
        assertEquals(query.toString(), "SpanMatchNoDocsQuery(\"field not found\")");

        SpanOrQuery orQuery = new SpanOrQuery(
            new SpanMatchNoDocsQuery("unknown", "field not found"),
            new SpanTermQuery(new Term("unknown", "one"))
        );
        assertEquals(searcher.count(orQuery), 0);
        hits = searcher.search(orQuery, 1000).scoreDocs;
        assertEquals(0, hits.length);

        orQuery = new SpanOrQuery(
            new SpanMatchNoDocsQuery("key", "a good reason"),
            new SpanTermQuery(new Term("key", "one"))
        );
        assertEquals(searcher.count(orQuery), 1);
        hits = searcher.search(orQuery, 1000).scoreDocs;
        assertEquals(1, hits.length);
        Query rewrite = orQuery.rewrite(ir);
        assertEquals(rewrite, orQuery);

        SpanNearQuery nearQuery = new SpanNearQuery(
            new SpanQuery[] {new SpanMatchNoDocsQuery("same", ""), new SpanMatchNoDocsQuery("same", "")},
            0, true);
        assertEquals(searcher.count(nearQuery), 0);
        hits = searcher.search(nearQuery, 1000).scoreDocs;
        assertEquals(0, hits.length);
        rewrite = nearQuery.rewrite(ir);
        assertEquals(rewrite, nearQuery);

        iw.close();
        ir.close();
        dir.close();
    }

    public void testEquals() {
        Query q1 = new SpanMatchNoDocsQuery("key1", "one");
        Query q2 = new SpanMatchNoDocsQuery("key2", "two");
        assertTrue(q1.equals(q2));
        QueryUtils.check(q1);
    }

    private void addDoc(String text, IndexWriter iw) throws IOException {
        Document doc = new Document();
        Field f = newTextField("key", text, Field.Store.YES);
        doc.add(f);
        iw.addDocument(doc);
    }

}
