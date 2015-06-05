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

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleLuceneTests extends ElasticsearchTestCase {

    @Test
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
        IndexReader reader = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(indexWriter, true));
        IndexSearcher searcher = new IndexSearcher(reader);
        TopFieldDocs docs = searcher.search(new MatchAllDocsQuery(), null, 10, new Sort(new SortField("str", SortField.Type.STRING)));
        for (int i = 0; i < 10; i++) {
            FieldDoc fieldDoc = (FieldDoc) docs.scoreDocs[i];
            assertThat((BytesRef) fieldDoc.fields[0], equalTo(new BytesRef(new String(new char[]{(char) (97 + i), (char) (97 + i)}))));
        }
    }

    @Test
    public void testAddDocAfterPrepareCommit() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        Document document = new Document();
        document.add(new TextField("_id", "1", Field.Store.YES));
        indexWriter.addDocument(document);
        DirectoryReader reader = DirectoryReader.open(indexWriter, true);
        assertThat(reader.numDocs(), equalTo(1));

        indexWriter.prepareCommit();
        // Returns null b/c no changes.
        assertThat(DirectoryReader.openIfChanged(reader), equalTo(null));

        document = new Document();
        document.add(new TextField("_id", "2", Field.Store.YES));
        indexWriter.addDocument(document);
        indexWriter.commit();
        reader = DirectoryReader.openIfChanged(reader);
        assertThat(reader.numDocs(), equalTo(2));
    }

    @Test
    public void testSimpleNumericOps() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        Document document = new Document();
        document.add(new TextField("_id", "1", Field.Store.YES));
        document.add(new IntField("test", 2, IntField.TYPE_STORED));
        indexWriter.addDocument(document);

        IndexReader reader = DirectoryReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);
        Document doc = searcher.doc(topDocs.scoreDocs[0].doc);
        IndexableField f = doc.getField("test");
        assertThat(f.stringValue(), equalTo("2"));

        BytesRefBuilder bytes = new BytesRefBuilder();
        NumericUtils.intToPrefixCoded(2, 0, bytes);
        topDocs = searcher.search(new TermQuery(new Term("test", bytes.get())), 1);
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
    @Test
    public void testOrdering() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        Document document = new Document();
        document.add(new TextField("_id", "1", Field.Store.YES));
        document.add(new TextField("#id", "1", Field.Store.YES));
        indexWriter.addDocument(document);

        IndexReader reader = DirectoryReader.open(indexWriter, true);
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

    @Test
    public void testBoost() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        for (int i = 0; i < 100; i++) {
            // TODO (just setting the boost value does not seem to work...)
            StringBuilder value = new StringBuilder().append("value");
            for (int j = 0; j < i; j++) {
                value.append(" ").append("value");
            }
            Document document = new Document();
            TextField textField = new TextField("_id", Integer.toString(i), Field.Store.YES);
            textField.setBoost(i);
            document.add(textField);
            textField = new TextField("value", value.toString(), Field.Store.YES);
            textField.setBoost(i);
            document.add(textField);
            indexWriter.addDocument(document);
        }

        IndexReader reader = DirectoryReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);
        TermQuery query = new TermQuery(new Term("value", "value"));
        TopDocs topDocs = searcher.search(query, 100);
        assertThat(100, equalTo(topDocs.totalHits));
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
//            System.out.println(doc.get("id") + ": " + searcher.explain(query, topDocs.scoreDocs[i].doc));
            assertThat(doc.get("_id"), equalTo(Integer.toString(100 - i - 1)));
        }

        indexWriter.close();
    }

    @Test
    public void testNRTSearchOnClosedWriter() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));
        DirectoryReader reader = DirectoryReader.open(indexWriter, true);

        for (int i = 0; i < 100; i++) {
            Document document = new Document();
            TextField field = new TextField("_id", Integer.toString(i), Field.Store.YES);
            field.setBoost(i);
            document.add(field);
            indexWriter.addDocument(document);
        }
        reader = refreshReader(reader);

        indexWriter.close();

        TermsEnum termDocs = SlowCompositeReaderWrapper.wrap(reader).terms("_id").iterator();
        termDocs.next();
    }

    /**
     * A test just to verify that term freqs are not stored for numeric fields. <tt>int1</tt> is not storing termFreq
     * and <tt>int2</tt> does.
     */
    @Test
    public void testNumericTermDocsFreqs() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.STANDARD_ANALYZER));

        Document doc = new Document();
        FieldType type = IntField.TYPE_NOT_STORED;
        IntField field = new IntField("int1", 1, type);
        doc.add(field);

        type = new FieldType(IntField.TYPE_NOT_STORED);
        type.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
        type.freeze();

        field = new IntField("int1", 1, type);
        doc.add(field);

        field = new IntField("int2", 1, type);
        doc.add(field);

        field = new IntField("int2", 1, type);
        doc.add(field);

        indexWriter.addDocument(doc);

        IndexReader reader = DirectoryReader.open(indexWriter, true);
        LeafReader atomicReader = SlowCompositeReaderWrapper.wrap(reader);

        Terms terms = atomicReader.terms("int1");
        TermsEnum termsEnum = terms.iterator();
        termsEnum.next();

        PostingsEnum termDocs = termsEnum.postings(atomicReader.getLiveDocs(), null);
        assertThat(termDocs.nextDoc(), equalTo(0));
        assertThat(termDocs.docID(), equalTo(0));
        assertThat(termDocs.freq(), equalTo(1));

        terms = atomicReader.terms("int2");
        termsEnum = terms.iterator();
        termsEnum.next();
        termDocs =  termsEnum.postings(atomicReader.getLiveDocs(), termDocs);
        assertThat(termDocs.nextDoc(), equalTo(0));
        assertThat(termDocs.docID(), equalTo(0));
        assertThat(termDocs.freq(), equalTo(2));

        reader.close();
        indexWriter.close();
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
