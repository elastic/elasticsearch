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

package org.elasticsearch.test.unit.index.fielddata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

/**
 */
public abstract class AbstractStringFieldDataTests extends AbstractFieldDataTests {

    protected String one() {
        return "1";
    }

    protected String two() {
        return "2";
    }

    protected String three() {
        return "3";
    }

    protected String four() {
        return "4";
    }

    private String toString(Object value) {
        if (value instanceof BytesRef) {
            return ((BytesRef) value).utf8ToString();
        }
        return value.toString();
    }

    protected void fillSingleValueAllSet() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(new StringField("value", "2", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(new StringField("value", "1", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        d.add(new StringField("value", "3", Field.Store.NO));
        writer.addDocument(d);
    }

    protected void add2SingleValuedDocumentsAndDeleteOneOfThem() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(new StringField("value", "2", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        d.add(new StringField("value", "4", Field.Store.NO));
        writer.addDocument(d);

        writer.commit();

        writer.deleteDocuments(new Term("_id", "1"));
    }

    @Test
    public void testDeletedDocs() throws Exception {
        add2SingleValuedDocumentsAndDeleteOneOfThem();
        IndexFieldData indexFieldData = getForField("value");
        AtomicReaderContext readerContext = refreshReader();
        AtomicFieldData fieldData = indexFieldData.load(readerContext);
        BytesValues values = fieldData.getBytesValues();
        assertThat(fieldData.getNumDocs(), equalTo(2));
        for (int i = 0; i < fieldData.getNumDocs(); ++i) {
            assertThat(values.hasValue(i), equalTo(true));
        }
    }

    @Test
    public void testSingleValueAllSet() throws Exception {
        fillSingleValueAllSet();
        IndexFieldData indexFieldData = getForField("value");
        AtomicReaderContext readerContext = refreshReader();
        AtomicFieldData fieldData = indexFieldData.load(readerContext);
        assertThat(fieldData.getMemorySizeInBytes(), greaterThan(0l));

        assertThat(fieldData.getNumDocs(), equalTo(3));

        BytesValues bytesValues = fieldData.getBytesValues();

        assertThat(bytesValues.isMultiValued(), equalTo(false));

        assertThat(bytesValues.hasValue(0), equalTo(true));
        assertThat(bytesValues.hasValue(1), equalTo(true));
        assertThat(bytesValues.hasValue(2), equalTo(true));

        assertThat(bytesValues.getValue(0), equalTo(new BytesRef(two())));
        assertThat(bytesValues.getValue(1), equalTo(new BytesRef(one())));
        assertThat(bytesValues.getValue(2), equalTo(new BytesRef(three())));

        BytesRef bytesRef = new BytesRef();
        assertThat(bytesValues.getValueScratch(0, bytesRef), equalTo(new BytesRef(two())));
        assertThat(bytesRef, equalTo(new BytesRef(two())));
        assertThat(bytesValues.getValueScratch(1, bytesRef), equalTo(new BytesRef(one())));
        assertThat(bytesRef, equalTo(new BytesRef(one())));
        assertThat(bytesValues.getValueScratch(2, bytesRef), equalTo(new BytesRef(three())));
        assertThat(bytesRef, equalTo(new BytesRef(three())));


        BytesValues.Iter bytesValuesIter = bytesValues.getIter(0);
        assertThat(bytesValuesIter.hasNext(), equalTo(true));
        assertThat(bytesValuesIter.next(), equalTo(new BytesRef(two())));
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        BytesValues hashedBytesValues = fieldData.getBytesValues();

        assertThat(hashedBytesValues.hasValue(0), equalTo(true));
        assertThat(hashedBytesValues.hasValue(1), equalTo(true));
        assertThat(hashedBytesValues.hasValue(2), equalTo(true));

        assertThat(convert(hashedBytesValues, 0), equalTo(new HashedBytesRef(two())));
        assertThat(convert(hashedBytesValues, 1), equalTo(new HashedBytesRef(one())));
        assertThat(convert(hashedBytesValues, 2), equalTo(new HashedBytesRef(three())));

        BytesValues.Iter hashedBytesValuesIter = hashedBytesValues.getIter(0);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(true));
        assertThat(new HashedBytesRef(hashedBytesValuesIter.next(), hashedBytesValuesIter.hash()), equalTo(new HashedBytesRef(two())));
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));

        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopFieldDocs topDocs;

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, SortMode.MIN))));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(toString(((FieldDoc) topDocs.scoreDocs[0]).fields[0]), equalTo(one()));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(toString(((FieldDoc) topDocs.scoreDocs[1]).fields[0]), equalTo(two()));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));
        assertThat(toString(((FieldDoc) topDocs.scoreDocs[2]).fields[0]), equalTo(three()));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, SortMode.MAX), true)));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(1));
    }
    
    private HashedBytesRef convert(BytesValues values, int doc) {
        BytesRef ref = new BytesRef();
        return new HashedBytesRef(ref, values.getValueHashed(doc, ref));
    }

    protected void fillSingleValueWithMissing() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(new StringField("value", "2", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        //d.add(new StringField("value", one(), Field.Store.NO)); // MISSING....
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        d.add(new StringField("value", "3", Field.Store.NO));
        writer.addDocument(d);
    }

    @Test
    public void testSingleValueWithMissing() throws Exception {
        fillSingleValueWithMissing();
        IndexFieldData indexFieldData = getForField("value");
        AtomicFieldData fieldData = indexFieldData.load(refreshReader());
        assertThat(fieldData.getMemorySizeInBytes(), greaterThan(0l));

        assertThat(fieldData.getNumDocs(), equalTo(3));

        BytesValues bytesValues = fieldData
                .getBytesValues();

        assertThat(bytesValues.isMultiValued(), equalTo(false));

        assertThat(bytesValues.hasValue(0), equalTo(true));
        assertThat(bytesValues.hasValue(1), equalTo(false));
        assertThat(bytesValues.hasValue(2), equalTo(true));

        assertThat(bytesValues.getValue(0), equalTo(new BytesRef(two())));
        assertThat(bytesValues.getValue(1), nullValue());
        assertThat(bytesValues.getValue(2), equalTo(new BytesRef(three())));

        BytesRef bytesRef = new BytesRef();
        assertThat(bytesValues.getValueScratch(0, bytesRef), equalTo(new BytesRef(two())));
        assertThat(bytesRef, equalTo(new BytesRef(two())));
        assertThat(bytesValues.getValueScratch(1, bytesRef), equalTo(new BytesRef()));
        assertThat(bytesRef, equalTo(new BytesRef()));
        assertThat(bytesValues.getValueScratch(2, bytesRef), equalTo(new BytesRef(three())));
        assertThat(bytesRef, equalTo(new BytesRef(three())));


        BytesValues.Iter bytesValuesIter = bytesValues.getIter(0);
        assertThat(bytesValuesIter.hasNext(), equalTo(true));
        assertThat(bytesValuesIter.next(), equalTo(new BytesRef(two())));
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        bytesValuesIter = bytesValues.getIter(1);
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        BytesValues hashedBytesValues = fieldData.getBytesValues();

        assertThat(hashedBytesValues.hasValue(0), equalTo(true));
        assertThat(hashedBytesValues.hasValue(1), equalTo(false));
        assertThat(hashedBytesValues.hasValue(2), equalTo(true));

        assertThat(convert(hashedBytesValues, 0), equalTo(new HashedBytesRef(two())));
        assertThat(convert(hashedBytesValues, 1), equalTo(new HashedBytesRef(new BytesRef())));
        assertThat(convert(hashedBytesValues, 2), equalTo(new HashedBytesRef(three())));

        BytesValues.Iter hashedBytesValuesIter = hashedBytesValues.getIter(0);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(true));
        assertThat(new HashedBytesRef(hashedBytesValuesIter.next(), hashedBytesValuesIter.hash()), equalTo(new HashedBytesRef(two())));
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));

        hashedBytesValuesIter = hashedBytesValues.getIter(1);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));

        // TODO properly support missing....
    }

    protected void fillMultiValueAllSet() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(new StringField("value", "2", Field.Store.NO));
        d.add(new StringField("value", "4", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        d.add(new StringField("value", "1", Field.Store.NO));
        writer.addDocument(d);
        writer.commit(); // TODO: Have tests with more docs for sorting

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        d.add(new StringField("value", "3", Field.Store.NO));
        writer.addDocument(d);
    }

    @Test
    public void testMultiValueAllSet() throws Exception {
        fillMultiValueAllSet();
        IndexFieldData indexFieldData = getForField("value");
        AtomicFieldData fieldData = indexFieldData.load(refreshReader());
        assertThat(fieldData.getMemorySizeInBytes(), greaterThan(0l));

        assertThat(fieldData.getNumDocs(), equalTo(3));

        BytesValues bytesValues = fieldData.getBytesValues();

        assertThat(bytesValues.isMultiValued(), equalTo(true));

        assertThat(bytesValues.hasValue(0), equalTo(true));
        assertThat(bytesValues.hasValue(1), equalTo(true));
        assertThat(bytesValues.hasValue(2), equalTo(true));

        assertThat(bytesValues.getValue(0), equalTo(new BytesRef(two())));
        assertThat(bytesValues.getValue(1), equalTo(new BytesRef(one())));
        assertThat(bytesValues.getValue(2), equalTo(new BytesRef(three())));

        BytesRef bytesRef = new BytesRef();
        assertThat(bytesValues.getValueScratch(0, bytesRef), equalTo(new BytesRef(two())));
        assertThat(bytesRef, equalTo(new BytesRef(two())));
        assertThat(bytesValues.getValueScratch(1, bytesRef), equalTo(new BytesRef(one())));
        assertThat(bytesRef, equalTo(new BytesRef(one())));
        assertThat(bytesValues.getValueScratch(2, bytesRef), equalTo(new BytesRef(three())));
        assertThat(bytesRef, equalTo(new BytesRef(three())));


        BytesValues.Iter bytesValuesIter = bytesValues.getIter(0);
        assertThat(bytesValuesIter.hasNext(), equalTo(true));
        assertThat(bytesValuesIter.next(), equalTo(new BytesRef(two())));
        assertThat(bytesValuesIter.hasNext(), equalTo(true));
        assertThat(bytesValuesIter.next(), equalTo(new BytesRef(four())));
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        BytesValues hashedBytesValues = fieldData.getBytesValues();

        assertThat(hashedBytesValues.hasValue(0), equalTo(true));
        assertThat(hashedBytesValues.hasValue(1), equalTo(true));
        assertThat(hashedBytesValues.hasValue(2), equalTo(true));

        assertThat(convert(hashedBytesValues, 0), equalTo(new HashedBytesRef(two())));
        assertThat(convert(hashedBytesValues, 1), equalTo(new HashedBytesRef(one())));
        assertThat(convert(hashedBytesValues, 2), equalTo(new HashedBytesRef(three())));

        BytesValues.Iter hashedBytesValuesIter = hashedBytesValues.getIter(0);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(true));
        assertThat(new HashedBytesRef(hashedBytesValuesIter.next(), hashedBytesValuesIter.hash()), equalTo(new HashedBytesRef(two())));
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(true));
        assertThat(new HashedBytesRef(hashedBytesValuesIter.next(), hashedBytesValuesIter.hash()), equalTo(new HashedBytesRef(four())));
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));

        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer, true));
        TopFieldDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10, new Sort(new SortField("value", indexFieldData.comparatorSource(null, SortMode.MIN))));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs.length, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10, new Sort(new SortField("value", indexFieldData.comparatorSource(null, SortMode.MAX), true)));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs.length, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(1));
    }

    protected void fillMultiValueWithMissing() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(new StringField("value", "2", Field.Store.NO));
        d.add(new StringField("value", "4", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        //d.add(new StringField("value", one(), Field.Store.NO)); // MISSING
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        d.add(new StringField("value", "3", Field.Store.NO));
        writer.addDocument(d);
    }

    @Test
    public void testMultiValueWithMissing() throws Exception {
        fillMultiValueWithMissing();
        IndexFieldData indexFieldData = getForField("value");
        AtomicFieldData fieldData = indexFieldData.load(refreshReader());
        assertThat(fieldData.getMemorySizeInBytes(), greaterThan(0l));

        assertThat(fieldData.getNumDocs(), equalTo(3));

        BytesValues bytesValues = fieldData.getBytesValues();

        assertThat(bytesValues.isMultiValued(), equalTo(true));

        assertThat(bytesValues.hasValue(0), equalTo(true));
        assertThat(bytesValues.hasValue(1), equalTo(false));
        assertThat(bytesValues.hasValue(2), equalTo(true));

        assertThat(bytesValues.getValue(0), equalTo(new BytesRef(two())));
        assertThat(bytesValues.getValue(1), nullValue());
        assertThat(bytesValues.getValue(2), equalTo(new BytesRef(three())));

        BytesRef bytesRef = new BytesRef();
        assertThat(bytesValues.getValueScratch(0, bytesRef), equalTo(new BytesRef(two())));
        assertThat(bytesRef, equalTo(new BytesRef(two())));
        assertThat(bytesValues.getValueScratch(1, bytesRef), equalTo(new BytesRef()));
        assertThat(bytesRef, equalTo(new BytesRef()));
        assertThat(bytesValues.getValueScratch(2, bytesRef), equalTo(new BytesRef(three())));
        assertThat(bytesRef, equalTo(new BytesRef(three())));


        BytesValues.Iter bytesValuesIter = bytesValues.getIter(0);
        assertThat(bytesValuesIter.hasNext(), equalTo(true));
        assertThat(bytesValuesIter.next(), equalTo(new BytesRef(two())));
        assertThat(bytesValuesIter.hasNext(), equalTo(true));
        assertThat(bytesValuesIter.next(), equalTo(new BytesRef(four())));
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        bytesValuesIter = bytesValues.getIter(1);
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        BytesValues hashedBytesValues = fieldData.getBytesValues();

        assertThat(hashedBytesValues.hasValue(0), equalTo(true));
        assertThat(hashedBytesValues.hasValue(1), equalTo(false));
        assertThat(hashedBytesValues.hasValue(2), equalTo(true));

        assertThat(convert(hashedBytesValues, 0), equalTo(new HashedBytesRef(two())));
        assertThat(convert(hashedBytesValues, 1), equalTo(new HashedBytesRef(new BytesRef())));
        assertThat(convert(hashedBytesValues, 2), equalTo(new HashedBytesRef(three())));

        BytesValues.Iter hashedBytesValuesIter = hashedBytesValues.getIter(0);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(true));
        assertThat(new HashedBytesRef(hashedBytesValuesIter.next(), hashedBytesValuesIter.hash()), equalTo(new HashedBytesRef(two())));
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(true));
        assertThat(new HashedBytesRef(hashedBytesValuesIter.next(), hashedBytesValuesIter.hash()), equalTo(new HashedBytesRef(four())));
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));

        hashedBytesValuesIter = hashedBytesValues.getIter(1);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));
    }

    public void testMissingValueForAll() throws Exception {
        fillAllMissing();
        IndexFieldData indexFieldData = getForField("value");
        AtomicFieldData fieldData = indexFieldData.load(refreshReader());
        // Some impls (FST) return size 0 and some (PagedBytes) do take size in the case no actual data is loaded
        assertThat(fieldData.getMemorySizeInBytes(), greaterThanOrEqualTo(0l));

        assertThat(fieldData.getNumDocs(), equalTo(3));

        BytesValues bytesValues = fieldData.getBytesValues();

        assertThat(bytesValues.isMultiValued(), equalTo(false));

        assertThat(bytesValues.hasValue(0), equalTo(false));
        assertThat(bytesValues.hasValue(1), equalTo(false));
        assertThat(bytesValues.hasValue(2), equalTo(false));

        assertThat(bytesValues.getValue(0), nullValue());
        assertThat(bytesValues.getValue(1), nullValue());
        assertThat(bytesValues.getValue(2), nullValue());

        BytesRef bytesRef = new BytesRef();
        assertThat(bytesValues.getValueScratch(0, bytesRef), equalTo(new BytesRef()));
        assertThat(bytesRef, equalTo(new BytesRef()));
        assertThat(bytesValues.getValueScratch(1, bytesRef), equalTo(new BytesRef()));
        assertThat(bytesRef, equalTo(new BytesRef()));
        assertThat(bytesValues.getValueScratch(2, bytesRef), equalTo(new BytesRef()));
        assertThat(bytesRef, equalTo(new BytesRef()));

        BytesValues.Iter bytesValuesIter = bytesValues.getIter(0);
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        bytesValuesIter = bytesValues.getIter(1);
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        bytesValuesIter = bytesValues.getIter(2);
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        BytesValues hashedBytesValues = fieldData.getBytesValues();

        assertThat(hashedBytesValues.hasValue(0), equalTo(false));
        assertThat(hashedBytesValues.hasValue(1), equalTo(false));
        assertThat(hashedBytesValues.hasValue(2), equalTo(false));

        assertThat(hashedBytesValues.getValue(0), nullValue());
        assertThat(hashedBytesValues.getValue(1), nullValue());
        assertThat(hashedBytesValues.getValue(2), nullValue());

        BytesValues.Iter hashedBytesValuesIter = hashedBytesValues.getIter(0);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));

        hashedBytesValuesIter = hashedBytesValues.getIter(1);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));

        hashedBytesValuesIter = hashedBytesValues.getIter(2);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));
    }

    protected void fillAllMissing() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        writer.addDocument(d);
    }

    @Test
    public void testSortMultiValuesFields() throws Exception {
        fillExtendedMvSet();
        IndexFieldData indexFieldData = getForField("value");

        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer, true));
        TopFieldDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, SortMode.MIN))));
        assertThat(topDocs.totalHits, equalTo(8));
        assertThat(topDocs.scoreDocs.length, equalTo(8));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(((FieldDoc) topDocs.scoreDocs[0]).fields[0], equalTo(null));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(5));
        assertThat(((FieldDoc) topDocs.scoreDocs[1]).fields[0], equalTo(null));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(7));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString(), equalTo("!08"));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(0));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString(), equalTo("02"));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(2));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString(), equalTo("03"));
        assertThat(topDocs.scoreDocs[5].doc, equalTo(3));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[5]).fields[0]).utf8ToString(), equalTo("04"));
        assertThat(topDocs.scoreDocs[6].doc, equalTo(4));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[6]).fields[0]).utf8ToString(), equalTo("06"));
        assertThat(topDocs.scoreDocs[7].doc, equalTo(6));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[7]).fields[0]).utf8ToString(), equalTo("08"));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, SortMode.MAX), true)));
        assertThat(topDocs.totalHits, equalTo(8));
        assertThat(topDocs.scoreDocs.length, equalTo(8));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(6));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString(), equalTo("10"));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(4));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString(), equalTo("08"));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(3));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString(), equalTo("06"));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(0));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString(), equalTo("04"));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(2));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString(), equalTo("03"));
        assertThat(topDocs.scoreDocs[5].doc, equalTo(7));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[5]).fields[0]).utf8ToString(), equalTo("!10"));
        assertThat(topDocs.scoreDocs[6].doc, equalTo(1));
        assertThat(((FieldDoc) topDocs.scoreDocs[6]).fields[0], equalTo(null));
        assertThat(topDocs.scoreDocs[7].doc, equalTo(5));
        assertThat(((FieldDoc) topDocs.scoreDocs[7]).fields[0], equalTo(null));
    }

    protected void fillExtendedMvSet() throws Exception {
        Document d = new Document();
        d.add(new StringField("_id", "1", Field.Store.NO));
        d.add(new StringField("value", "02", Field.Store.NO));
        d.add(new StringField("value", "04", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "2", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "3", Field.Store.NO));
        d.add(new StringField("value", "03", Field.Store.NO));
        writer.addDocument(d);
        writer.commit();

        d = new Document();
        d.add(new StringField("_id", "4", Field.Store.NO));
        d.add(new StringField("value", "04", Field.Store.NO));
        d.add(new StringField("value", "05", Field.Store.NO));
        d.add(new StringField("value", "06", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "5", Field.Store.NO));
        d.add(new StringField("value", "06", Field.Store.NO));
        d.add(new StringField("value", "07", Field.Store.NO));
        d.add(new StringField("value", "08", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "6", Field.Store.NO));
        writer.addDocument(d);

        d = new Document();
        d.add(new StringField("_id", "7", Field.Store.NO));
        d.add(new StringField("value", "08", Field.Store.NO));
        d.add(new StringField("value", "09", Field.Store.NO));
        d.add(new StringField("value", "10", Field.Store.NO));
        writer.addDocument(d);
        writer.commit();

        d = new Document();
        d.add(new StringField("_id", "8", Field.Store.NO));
        d.add(new StringField("value", "!08", Field.Store.NO));
        d.add(new StringField("value", "!09", Field.Store.NO));
        d.add(new StringField("value", "!10", Field.Store.NO));
        writer.addDocument(d);
    }

}
