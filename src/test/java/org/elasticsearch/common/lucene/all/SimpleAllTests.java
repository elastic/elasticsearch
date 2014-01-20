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

package org.elasticsearch.common.lucene.all;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.payloads.PayloadHelper;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleAllTests extends ElasticsearchTestCase {

    @Test
    public void testBoostOnEagerTokenizer() throws Exception {
        AllEntries allEntries = new AllEntries();
        allEntries.addText("field1", "all", 2.0f);
        allEntries.addText("field2", "your", 1.0f);
        allEntries.addText("field1", "boosts", 0.5f);
        allEntries.reset();
        // whitespace analyzer's tokenizer reads characters eagerly on the contrary to the standard tokenizer
        final TokenStream ts = AllTokenStream.allTokenStream("any", allEntries, new WhitespaceAnalyzer(Lucene.VERSION));
        final CharTermAttribute termAtt = ts.addAttribute(CharTermAttribute.class);
        final PayloadAttribute payloadAtt = ts.addAttribute(PayloadAttribute.class);
        ts.reset();
        for (int i = 0; i < 3; ++i) {
            assertTrue(ts.incrementToken());
            final String term;
            final float boost;
            switch (i) {
            case 0:
                term = "all";
                boost = 2;
                break;
            case 1:
                term = "your";
                boost = 1;
                break;
            case 2:
                term = "boosts";
                boost = 0.5f;
                break;
            default:
                throw new AssertionError();
            }
            assertEquals(term, termAtt.toString());
            final BytesRef payload = payloadAtt.getPayload();
            if (payload == null || payload.length == 0) {
                assertEquals(boost, 1f, 0.001f);
            } else {
                assertEquals(4, payload.length);
                final float b = PayloadHelper.decodeFloat(payload.bytes, payload.offset);
                assertEquals(boost, b, 0.001f);
            }
        }
        assertFalse(ts.incrementToken());
    }

    @Test
    public void testAllEntriesRead() throws Exception {
        AllEntries allEntries = new AllEntries();
        allEntries.addText("field1", "something", 1.0f);
        allEntries.addText("field2", "else", 1.0f);

        for (int i = 1; i < 30; i++) {
            allEntries.reset();
            char[] data = new char[i];
            String value = slurpToString(allEntries, data);
            assertThat("failed for " + i, value, equalTo("something else"));
        }
    }

    private String slurpToString(AllEntries allEntries, char[] data) throws IOException {
        StringBuilder sb = new StringBuilder();
        while (true) {
            int read = allEntries.read(data, 0, data.length);
            if (read == -1) {
                break;
            }
            sb.append(data, 0, read);
        }
        return sb.toString();
    }

    private void assertExplanationScore(IndexSearcher searcher, Query query, ScoreDoc scoreDoc) throws IOException {
        final Explanation expl = searcher.explain(query, scoreDoc.doc);
        assertEquals(scoreDoc.score, expl.getValue(), 0.00001f);
    }

    @Test
    public void testSimpleAllNoBoost() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        Document doc = new Document();
        doc.add(new Field("_id", "1", StoredField.TYPE));
        AllEntries allEntries = new AllEntries();
        allEntries.addText("field1", "something", 1.0f);
        allEntries.addText("field2", "else", 1.0f);
        allEntries.reset();
        doc.add(new TextField("_all", AllTokenStream.allTokenStream("_all", allEntries, Lucene.STANDARD_ANALYZER)));

        indexWriter.addDocument(doc);

        doc = new Document();
        doc.add(new Field("_id", "2", StoredField.TYPE));
        allEntries = new AllEntries();
        allEntries.addText("field1", "else", 1.0f);
        allEntries.addText("field2", "something", 1.0f);
        allEntries.reset();
        doc.add(new TextField("_all", AllTokenStream.allTokenStream("_all", allEntries, Lucene.STANDARD_ANALYZER)));

        indexWriter.addDocument(doc);

        IndexReader reader = DirectoryReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);

        Query query = new AllTermQuery(new Term("_all", "else"));
        TopDocs docs = searcher.search(query, 10);
        assertThat(docs.totalHits, equalTo(2));
        assertThat(docs.scoreDocs[0].doc, equalTo(0));
        assertExplanationScore(searcher, query, docs.scoreDocs[0]);
        assertThat(docs.scoreDocs[1].doc, equalTo(1));
        assertExplanationScore(searcher, query, docs.scoreDocs[1]);

        query = new AllTermQuery(new Term("_all", "something"));
        docs = searcher.search(query, 10);
        assertThat(docs.totalHits, equalTo(2));
        assertThat(docs.scoreDocs[0].doc, equalTo(0));
        assertExplanationScore(searcher, query, docs.scoreDocs[0]);
        assertThat(docs.scoreDocs[1].doc, equalTo(1));
        assertExplanationScore(searcher, query, docs.scoreDocs[1]);

        indexWriter.close();
    }

    @Test
    public void testSimpleAllWithBoost() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        Document doc = new Document();
        doc.add(new Field("_id", "1", StoredField.TYPE));
        AllEntries allEntries = new AllEntries();
        allEntries.addText("field1", "something", 1.0f);
        allEntries.addText("field2", "else", 1.0f);
        allEntries.reset();
        doc.add(new TextField("_all", AllTokenStream.allTokenStream("_all", allEntries, Lucene.STANDARD_ANALYZER)));

        indexWriter.addDocument(doc);

        doc = new Document();
        doc.add(new Field("_id", "2", StoredField.TYPE));
        allEntries = new AllEntries();
        allEntries.addText("field1", "else", 2.0f);
        allEntries.addText("field2", "something", 1.0f);
        allEntries.reset();
        doc.add(new TextField("_all", AllTokenStream.allTokenStream("_all", allEntries, Lucene.STANDARD_ANALYZER)));

        indexWriter.addDocument(doc);

        IndexReader reader = DirectoryReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);

        // this one is boosted. so the second doc is more relevant
        Query query = new AllTermQuery(new Term("_all", "else"));
        TopDocs docs = searcher.search(query, 10);
        assertThat(docs.totalHits, equalTo(2));
        assertThat(docs.scoreDocs[0].doc, equalTo(1));
        assertExplanationScore(searcher, query, docs.scoreDocs[0]);
        assertThat(docs.scoreDocs[1].doc, equalTo(0));
        assertExplanationScore(searcher, query, docs.scoreDocs[1]);

        query = new AllTermQuery(new Term("_all", "something"));
        docs = searcher.search(query, 10);
        assertThat(docs.totalHits, equalTo(2));
        assertThat(docs.scoreDocs[0].doc, equalTo(0));
        assertExplanationScore(searcher, query, docs.scoreDocs[0]);
        assertThat(docs.scoreDocs[1].doc, equalTo(1));
        assertExplanationScore(searcher, query, docs.scoreDocs[1]);

        indexWriter.close();
    }

    @Test
    public void testMultipleTokensAllNoBoost() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        Document doc = new Document();
        doc.add(new Field("_id", "1", StoredField.TYPE));
        AllEntries allEntries = new AllEntries();
        allEntries.addText("field1", "something moo", 1.0f);
        allEntries.addText("field2", "else koo", 1.0f);
        allEntries.reset();
        doc.add(new TextField("_all", AllTokenStream.allTokenStream("_all", allEntries, Lucene.STANDARD_ANALYZER)));

        indexWriter.addDocument(doc);

        doc = new Document();
        doc.add(new Field("_id", "2", StoredField.TYPE));
        allEntries = new AllEntries();
        allEntries.addText("field1", "else koo", 1.0f);
        allEntries.addText("field2", "something moo", 1.0f);
        allEntries.reset();
        doc.add(new TextField("_all", AllTokenStream.allTokenStream("_all", allEntries, Lucene.STANDARD_ANALYZER)));

        indexWriter.addDocument(doc);

        IndexReader reader = DirectoryReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);

        TopDocs docs = searcher.search(new AllTermQuery(new Term("_all", "else")), 10);
        assertThat(docs.totalHits, equalTo(2));
        assertThat(docs.scoreDocs[0].doc, equalTo(0));
        assertThat(docs.scoreDocs[1].doc, equalTo(1));

        docs = searcher.search(new AllTermQuery(new Term("_all", "koo")), 10);
        assertThat(docs.totalHits, equalTo(2));
        assertThat(docs.scoreDocs[0].doc, equalTo(0));
        assertThat(docs.scoreDocs[1].doc, equalTo(1));

        docs = searcher.search(new AllTermQuery(new Term("_all", "something")), 10);
        assertThat(docs.totalHits, equalTo(2));
        assertThat(docs.scoreDocs[0].doc, equalTo(0));
        assertThat(docs.scoreDocs[1].doc, equalTo(1));

        docs = searcher.search(new AllTermQuery(new Term("_all", "moo")), 10);
        assertThat(docs.totalHits, equalTo(2));
        assertThat(docs.scoreDocs[0].doc, equalTo(0));
        assertThat(docs.scoreDocs[1].doc, equalTo(1));

        indexWriter.close();
    }

    @Test
    public void testMultipleTokensAllWithBoost() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        Document doc = new Document();
        doc.add(new Field("_id", "1", StoredField.TYPE));
        AllEntries allEntries = new AllEntries();
        allEntries.addText("field1", "something moo", 1.0f);
        allEntries.addText("field2", "else koo", 1.0f);
        allEntries.reset();
        doc.add(new TextField("_all", AllTokenStream.allTokenStream("_all", allEntries, Lucene.STANDARD_ANALYZER)));

        indexWriter.addDocument(doc);

        doc = new Document();
        doc.add(new Field("_id", "2", StoredField.TYPE));
        allEntries = new AllEntries();
        allEntries.addText("field1", "else koo", 2.0f);
        allEntries.addText("field2", "something moo", 1.0f);
        allEntries.reset();
        doc.add(new TextField("_all", AllTokenStream.allTokenStream("_all", allEntries, Lucene.STANDARD_ANALYZER)));

        indexWriter.addDocument(doc);

        IndexReader reader = DirectoryReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);

        TopDocs docs = searcher.search(new AllTermQuery(new Term("_all", "else")), 10);
        assertThat(docs.totalHits, equalTo(2));
        assertThat(docs.scoreDocs[0].doc, equalTo(1));
        assertThat(docs.scoreDocs[1].doc, equalTo(0));

        docs = searcher.search(new AllTermQuery(new Term("_all", "koo")), 10);
        assertThat(docs.totalHits, equalTo(2));
        assertThat(docs.scoreDocs[0].doc, equalTo(1));
        assertThat(docs.scoreDocs[1].doc, equalTo(0));

        docs = searcher.search(new AllTermQuery(new Term("_all", "something")), 10);
        assertThat(docs.totalHits, equalTo(2));
        assertThat(docs.scoreDocs[0].doc, equalTo(0));
        assertThat(docs.scoreDocs[1].doc, equalTo(1));

        docs = searcher.search(new AllTermQuery(new Term("_all", "moo")), 10);
        assertThat(docs.totalHits, equalTo(2));
        assertThat(docs.scoreDocs[0].doc, equalTo(0));
        assertThat(docs.scoreDocs[1].doc, equalTo(1));

        indexWriter.close();
    }

    @Test
    public void testNoTokensWithKeywordAnalyzer() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.KEYWORD_ANALYZER));

        Document doc = new Document();
        doc.add(new Field("_id", "1", StoredField.TYPE));
        AllEntries allEntries = new AllEntries();
        allEntries.reset();
        doc.add(new TextField("_all", AllTokenStream.allTokenStream("_all", allEntries, Lucene.KEYWORD_ANALYZER)));

        indexWriter.addDocument(doc);

        IndexReader reader = DirectoryReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);

        TopDocs docs = searcher.search(new MatchAllDocsQuery(), 10);
        assertThat(docs.totalHits, equalTo(1));
        assertThat(docs.scoreDocs[0].doc, equalTo(0));
    }
}
