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

import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleAllTests extends ESTestCase {
    private FieldType getAllFieldType() {
        FieldType ft = new FieldType();
        ft.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
        ft.setTokenized(true);
        ft.freeze();
        return ft;
    }

    private void assertExplanationScore(IndexSearcher searcher, Query query, ScoreDoc scoreDoc) throws IOException {
        final Explanation expl = searcher.explain(query, scoreDoc.doc);
        assertEquals(scoreDoc.score, expl.getValue(), 0.00001f);
    }

    public void testSimpleAllNoBoost() throws Exception {
        FieldType allFt = getAllFieldType();
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        Document doc = new Document();
        doc.add(new Field("_id", "1", StoredField.TYPE));
        doc.add(new AllField("_all", "something", 1.0f, allFt));
        doc.add(new AllField("_all", "else", 1.0f, allFt));

        indexWriter.addDocument(doc);

        doc = new Document();
        doc.add(new Field("_id", "2", StoredField.TYPE));
        doc.add(new AllField("_all", "else", 1.0f, allFt));
        doc.add(new AllField("_all", "something", 1.0f, allFt));
        indexWriter.addDocument(doc);

        IndexReader reader = DirectoryReader.open(indexWriter);
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

    public void testSimpleAllWithBoost() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        FieldType allFt = getAllFieldType();
        Document doc = new Document();
        doc.add(new Field("_id", "1", StoredField.TYPE));
        doc.add(new AllField("_all", "something", 1.0f, allFt));
        doc.add(new AllField("_all", "else", 1.0f, allFt));

        indexWriter.addDocument(doc);

        doc = new Document();
        doc.add(new Field("_id", "2", StoredField.TYPE));
        doc.add(new AllField("_all", "else", 2.0f, allFt));
        doc.add(new AllField("_all", "something", 1.0f, allFt));

        indexWriter.addDocument(doc);

        IndexReader reader = DirectoryReader.open(indexWriter);
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

   public void testTermMissingFromOneSegment() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        FieldType allFt = getAllFieldType();
        Document doc = new Document();
        doc.add(new Field("_id", "1", StoredField.TYPE));
        doc.add(new AllField("_all", "something", 2.0f, allFt));

        indexWriter.addDocument(doc);
        indexWriter.commit();

        doc = new Document();
        doc.add(new Field("_id", "2", StoredField.TYPE));
        doc.add(new AllField("_all", "else", 1.0f, allFt));
        indexWriter.addDocument(doc);

        IndexReader reader = DirectoryReader.open(indexWriter);
        assertEquals(2, reader.leaves().size());
        IndexSearcher searcher = new IndexSearcher(reader);

        // "something" only appears in the first segment:
        Query query = new AllTermQuery(new Term("_all", "something"));
        TopDocs docs = searcher.search(query, 10);
        assertEquals(1, docs.totalHits);

        indexWriter.close();
    }

    public void testMultipleTokensAllNoBoost() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        FieldType allFt = getAllFieldType();
        Document doc = new Document();
        doc.add(new Field("_id", "1", StoredField.TYPE));
        doc.add(new AllField("_all", "something moo", 1.0f, allFt));
        doc.add(new AllField("_all", "else koo", 1.0f, allFt));

        indexWriter.addDocument(doc);

        doc = new Document();
        doc.add(new Field("_id", "2", StoredField.TYPE));
        doc.add(new AllField("_all", "else koo", 1.0f, allFt));
        doc.add(new AllField("_all", "something moo", 1.0f, allFt));

        indexWriter.addDocument(doc);

        IndexReader reader = DirectoryReader.open(indexWriter);
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

    public void testMultipleTokensAllWithBoost() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        FieldType allFt = getAllFieldType();
        Document doc = new Document();
        doc.add(new Field("_id", "1", StoredField.TYPE));
        doc.add(new AllField("_all", "something moo", 1.0f, allFt));
        doc.add(new AllField("_all", "else koo", 1.0f, allFt));

        indexWriter.addDocument(doc);

        doc = new Document();
        doc.add(new Field("_id", "2", StoredField.TYPE));
        doc.add(new AllField("_all", "else koo", 2.0f, allFt));
        doc.add(new AllField("_all", "something moo", 1.0f, allFt));

        indexWriter.addDocument(doc);

        IndexReader reader = DirectoryReader.open(indexWriter);
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

    public void testNoTokens() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.KEYWORD_ANALYZER));

        FieldType allFt = getAllFieldType();
        Document doc = new Document();
        doc.add(new Field("_id", "1", StoredField.TYPE));
        doc.add(new AllField("_all", "", 2.0f, allFt));
        indexWriter.addDocument(doc);

        IndexReader reader = DirectoryReader.open(indexWriter);
        IndexSearcher searcher = new IndexSearcher(reader);

        TopDocs docs = searcher.search(new MatchAllDocsQuery(), 10);
        assertThat(docs.totalHits, equalTo(1));
        assertThat(docs.scoreDocs[0].doc, equalTo(0));
    }
}
