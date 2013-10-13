/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.*;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleAllTests extends ElasticsearchTestCase {

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

        TopDocs docs = searcher.search(new AllTermQuery(new Term("_all", "else")), 10);
        assertThat(docs.totalHits, equalTo(2));
        assertThat(docs.scoreDocs[0].doc, equalTo(0));
        assertThat(docs.scoreDocs[1].doc, equalTo(1));

        docs = searcher.search(new AllTermQuery(new Term("_all", "something")), 10);
        assertThat(docs.totalHits, equalTo(2));
        assertThat(docs.scoreDocs[0].doc, equalTo(0));
        assertThat(docs.scoreDocs[1].doc, equalTo(1));

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
        TopDocs docs = searcher.search(new AllTermQuery(new Term("_all", "else")), 10);
        assertThat(docs.totalHits, equalTo(2));
        assertThat(docs.scoreDocs[0].doc, equalTo(1));
        assertThat(docs.scoreDocs[1].doc, equalTo(0));

        docs = searcher.search(new AllTermQuery(new Term("_all", "something")), 10);
        assertThat(docs.totalHits, equalTo(2));
        assertThat(docs.scoreDocs[0].doc, equalTo(0));
        assertThat(docs.scoreDocs[1].doc, equalTo(1));

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
}
