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

package org.elasticsearch.deps.lucene;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFieldVisitor;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;

import static org.hamcrest.Matchers.equalTo;

public class SimpleLuceneTests extends ESTestCase {
    public void testSortValues() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        for (int i = 0; i < 10; i++) {
            Document document = new Document();
            String text = new String(new char[]{(char) (97 + i), (char) (97 + i)});
            document.add(new TextField("str", text, Field.Store.YES));
            document.add(new SortedDocValuesField("str", new BytesRef(text)));
            indexWriter.addDocument(document);
        }
        IndexReader reader = DirectoryReader.open(indexWriter);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopFieldDocs docs = searcher.search(new MatchAllDocsQuery(), 10, new Sort(new SortField("str", SortField.Type.STRING)));
        for (int i = 0; i < 10; i++) {
            FieldDoc fieldDoc = (FieldDoc) docs.scoreDocs[i];
            assertThat((BytesRef) fieldDoc.fields[0], equalTo(new BytesRef(new String(new char[]{(char) (97 + i), (char) (97 + i)}))));
        }
    }

    public void testSimpleNumericOps() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        Document document = new Document();
        document.add(new TextField("_id", "1", Field.Store.YES));
        document.add(new IntPoint("test", 2));
        document.add(new StoredField("test", 2));
        indexWriter.addDocument(document);

        IndexReader reader = DirectoryReader.open(indexWriter);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);
        Document doc = searcher.doc(topDocs.scoreDocs[0].doc);
        IndexableField f = doc.getField("test");
        assertThat(f.numericValue(), equalTo(2));

        topDocs = searcher.search(IntPoint.newExactQuery("test", 2), 1);
        doc = searcher.doc(topDocs.scoreDocs[0].doc);
        f = doc.getField("test");
        assertThat(f.stringValue(), equalTo("2"));

        indexWriter.close();
    }

    /**
     * Here, we verify that the order that we add fields to a document counts, and not the lexi order
     * of the field. This means that heavily accessed fields that use field selector should be added
     * first (with load and break).
     */
    public void testOrdering() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        Document document = new Document();
        document.add(new TextField("_id", "1", Field.Store.YES));
        document.add(new TextField("#id", "1", Field.Store.YES));
        indexWriter.addDocument(document);

        IndexReader reader = DirectoryReader.open(indexWriter);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);
        final ArrayList<String> fieldsOrder = new ArrayList<>();
        searcher.doc(topDocs.scoreDocs[0].doc, new StoredFieldVisitor() {
            @Override
            public Status needsField(FieldInfo fieldInfo) throws IOException {
                fieldsOrder.add(fieldInfo.name);
                return Status.YES;
            }
        });

        assertThat(fieldsOrder.size(), equalTo(2));
        assertThat(fieldsOrder.get(0), equalTo("_id"));
        assertThat(fieldsOrder.get(1), equalTo("#id"));

        indexWriter.close();
    }

    public void testNRTSearchOnClosedWriter() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        DirectoryReader reader = DirectoryReader.open(indexWriter);

        for (int i = 0; i < 100; i++) {
            Document document = new Document();
            TextField field = new TextField("_id", Integer.toString(i), Field.Store.YES);
            document.add(field);
            indexWriter.addDocument(document);
        }
        reader = refreshReader(reader);

        indexWriter.close();

        for (LeafReaderContext leaf : reader.leaves()) {
            leaf.reader().terms("_id").iterator().next();
        }
    }

    private DirectoryReader refreshReader(DirectoryReader reader) throws IOException {
        DirectoryReader oldReader = reader;
        reader = DirectoryReader.openIfChanged(reader);
        if (reader != oldReader) {
            oldReader.close();
        }
        return reader;
    }
}
