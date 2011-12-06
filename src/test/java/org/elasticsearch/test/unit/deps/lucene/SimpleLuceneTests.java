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

package org.elasticsearch.test.unit.deps.lucene;

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.lucene.Lucene;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;

import static org.elasticsearch.common.lucene.DocumentBuilder.doc;
import static org.elasticsearch.common.lucene.DocumentBuilder.field;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 *
 */
public class SimpleLuceneTests {

    @Test
    public void testSortValues() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));
        for (int i = 0; i < 10; i++) {
            indexWriter.addDocument(doc().add(field("str", new String(new char[]{(char) (97 + i), (char) (97 + i)}))).build());
        }
        IndexReader reader = IndexReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopFieldDocs docs = searcher.search(new MatchAllDocsQuery(), null, 10, new Sort(new SortField("str", SortField.STRING)));
        for (int i = 0; i < 10; i++) {
            FieldDoc fieldDoc = (FieldDoc) docs.scoreDocs[i];
            assertThat(fieldDoc.fields[0].toString(), equalTo(new String(new char[]{(char) (97 + i), (char) (97 + i)})));
        }
    }

    @Test
    public void testAddDocAfterPrepareCommit() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));
        indexWriter.addDocument(doc()
                .add(field("_id", "1")).build());
        IndexReader reader = IndexReader.open(indexWriter, true);
        assertThat(reader.numDocs(), equalTo(1));

        indexWriter.prepareCommit();
        reader = reader.reopen();
        assertThat(reader.numDocs(), equalTo(1));

        indexWriter.addDocument(doc()
                .add(field("_id", "2")).build());
        indexWriter.commit();
        reader = reader.reopen();
        assertThat(reader.numDocs(), equalTo(2));
    }

    @Test
    public void testSimpleNumericOps() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        indexWriter.addDocument(doc().add(field("_id", "1")).add(new NumericField("test", Field.Store.YES, true).setIntValue(2)).build());

        IndexReader reader = IndexReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);
        Document doc = searcher.doc(topDocs.scoreDocs[0].doc);
        Fieldable f = doc.getFieldable("test");
        assertThat(f.stringValue(), equalTo("2"));

        topDocs = searcher.search(new TermQuery(new Term("test", NumericUtils.intToPrefixCoded(2))), 1);
        doc = searcher.doc(topDocs.scoreDocs[0].doc);
        f = doc.getFieldable("test");
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
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        indexWriter.addDocument(doc()
                .add(field("_id", "1"))
                .add(field("#id", "1")).build());

        IndexReader reader = IndexReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);
        TopDocs topDocs = searcher.search(new TermQuery(new Term("_id", "1")), 1);
        final ArrayList<String> fieldsOrder = new ArrayList<String>();
        Document doc = searcher.doc(topDocs.scoreDocs[0].doc, new FieldSelector() {
            @Override
            public FieldSelectorResult accept(String fieldName) {
                fieldsOrder.add(fieldName);
                return FieldSelectorResult.LOAD;
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
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        for (int i = 0; i < 100; i++) {
            // TODO (just setting the boost value does not seem to work...)
            StringBuilder value = new StringBuilder().append("value");
            for (int j = 0; j < i; j++) {
                value.append(" ").append("value");
            }
            indexWriter.addDocument(doc()
                    .add(field("id", Integer.toString(i)))
                    .add(field("value", value.toString()))
                    .boost(i).build());
        }

        IndexReader reader = IndexReader.open(indexWriter, true);
        IndexSearcher searcher = new IndexSearcher(reader);
        TermQuery query = new TermQuery(new Term("value", "value"));
        TopDocs topDocs = searcher.search(query, 100);
        assertThat(100, equalTo(topDocs.totalHits));
        for (int i = 0; i < topDocs.scoreDocs.length; i++) {
            Document doc = searcher.doc(topDocs.scoreDocs[i].doc);
//            System.out.println(doc.get("id") + ": " + searcher.explain(query, topDocs.scoreDocs[i].doc));
            assertThat(doc.get("id"), equalTo(Integer.toString(100 - i - 1)));
        }

        indexWriter.close();
    }

    @Test
    public void testNRTSearchOnClosedWriter() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));
        IndexReader reader = IndexReader.open(indexWriter, true);

        for (int i = 0; i < 100; i++) {
            indexWriter.addDocument(doc()
                    .add(field("id", Integer.toString(i)))
                    .boost(i).build());
        }
        reader = refreshReader(reader);

        indexWriter.close();

        TermDocs termDocs = reader.termDocs();
        termDocs.next();
    }

    /**
     * A test just to verify that term freqs are not stored for numeric fields. <tt>int1</tt> is not storing termFreq
     * and <tt>int2</tt> does.
     */
    @Test
    public void testNumericTermDocsFreqs() throws Exception {
        Directory dir = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(dir, new IndexWriterConfig(Lucene.VERSION, Lucene.STANDARD_ANALYZER));

        Document doc = new Document();
        NumericField field = new NumericField("int1").setIntValue(1);
        field.setOmitNorms(true);
        field.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS);
        doc.add(field);

        field = new NumericField("int1").setIntValue(1);
        doc.add(field);

        field = new NumericField("int2").setIntValue(1);
        field.setOmitNorms(true);
        field.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS);
        doc.add(field);

        field = new NumericField("int2").setIntValue(1);
        field.setOmitNorms(true);
        field.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS);
        doc.add(field);

        indexWriter.addDocument(doc);

        IndexReader reader = IndexReader.open(indexWriter, true);

        TermDocs termDocs = reader.termDocs();

        TermEnum termEnum = reader.terms(new Term("int1", ""));
        termDocs.seek(termEnum);
        assertThat(termDocs.next(), equalTo(true));
        assertThat(termDocs.doc(), equalTo(0));
        assertThat(termDocs.freq(), equalTo(1));

        termEnum = reader.terms(new Term("int2", ""));
        termDocs.seek(termEnum);
        assertThat(termDocs.next(), equalTo(true));
        assertThat(termDocs.doc(), equalTo(0));
        assertThat(termDocs.freq(), equalTo(2));

        reader.close();
        indexWriter.close();
    }

    private IndexReader refreshReader(IndexReader reader) throws IOException {
        IndexReader oldReader = reader;
        reader = reader.reopen();
        if (reader != oldReader) {
            oldReader.close();
        }
        return reader;
    }
}
