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
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.util.BytesRefArrayRef;
import org.elasticsearch.index.fielddata.util.StringArrayRef;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 */
@Test
public abstract class StringFieldDataTests extends AbstractFieldDataTests {

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

    @Test
    public void testSingleValueAllSet() throws Exception {
        fillSingleValueAllSet();
        IndexFieldData indexFieldData = getForField("value");
        AtomicReaderContext readerContext = refreshReader();
        AtomicFieldData fieldData = indexFieldData.load(readerContext);

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


        BytesRefArrayRef bytesRefArrayRef = bytesValues.getValues(0);
        assertThat(bytesRefArrayRef.size(), equalTo(1));
        assertThat(bytesRefArrayRef.values[bytesRefArrayRef.start], equalTo(new BytesRef(two())));

        bytesRefArrayRef = bytesValues.getValues(1);
        assertThat(bytesRefArrayRef.size(), equalTo(1));
        assertThat(bytesRefArrayRef.values[bytesRefArrayRef.start], equalTo(new BytesRef(one())));

        bytesRefArrayRef = bytesValues.getValues(2);
        assertThat(bytesRefArrayRef.size(), equalTo(1));
        assertThat(bytesRefArrayRef.values[bytesRefArrayRef.start], equalTo(new BytesRef(three())));

        BytesValues.Iter bytesValuesIter = bytesValues.getIter(0);
        assertThat(bytesValuesIter.hasNext(), equalTo(true));
        assertThat(bytesValuesIter.next(), equalTo(new BytesRef(two())));
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        bytesValues.forEachValueInDoc(0, new BytesValuesVerifierProc(0).addExpected(two()));
        bytesValues.forEachValueInDoc(1, new BytesValuesVerifierProc(1).addExpected(one()));
        bytesValues.forEachValueInDoc(2, new BytesValuesVerifierProc(2).addExpected(three()));

        HashedBytesValues hashedBytesValues = fieldData.getHashedBytesValues();

        assertThat(hashedBytesValues.hasValue(0), equalTo(true));
        assertThat(hashedBytesValues.hasValue(1), equalTo(true));
        assertThat(hashedBytesValues.hasValue(2), equalTo(true));

        assertThat(hashedBytesValues.getValue(0), equalTo(new HashedBytesRef(two())));
        assertThat(hashedBytesValues.getValue(1), equalTo(new HashedBytesRef(one())));
        assertThat(hashedBytesValues.getValue(2), equalTo(new HashedBytesRef(three())));

        HashedBytesValues.Iter hashedBytesValuesIter = hashedBytesValues.getIter(0);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(true));
        assertThat(hashedBytesValuesIter.next(), equalTo(new HashedBytesRef(two())));
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));

        hashedBytesValues.forEachValueInDoc(0, new HashedBytesValuesVerifierProc(0).addExpected(two()));
        hashedBytesValues.forEachValueInDoc(1, new HashedBytesValuesVerifierProc(1).addExpected(one()));
        hashedBytesValues.forEachValueInDoc(2, new HashedBytesValuesVerifierProc(2).addExpected(three()));

        StringValues stringValues = fieldData.getStringValues();

        assertThat(stringValues.hasValue(0), equalTo(true));
        assertThat(stringValues.hasValue(1), equalTo(true));
        assertThat(stringValues.hasValue(2), equalTo(true));

        assertThat(stringValues.getValue(0), equalTo(two()));
        assertThat(stringValues.getValue(1), equalTo(one()));
        assertThat(stringValues.getValue(2), equalTo(three()));

        StringArrayRef stringArrayRef;
        stringArrayRef = stringValues.getValues(0);
        assertThat(stringArrayRef.size(), equalTo(1));
        assertThat(stringArrayRef.values[stringArrayRef.start], equalTo(two()));

        stringArrayRef = stringValues.getValues(1);
        assertThat(stringArrayRef.size(), equalTo(1));
        assertThat(stringArrayRef.values[stringArrayRef.start], equalTo(one()));

        stringArrayRef = stringValues.getValues(2);
        assertThat(stringArrayRef.size(), equalTo(1));
        assertThat(stringArrayRef.values[stringArrayRef.start], equalTo(three()));

        StringValues.Iter stringValuesIter = stringValues.getIter(0);
        assertThat(stringValuesIter.hasNext(), equalTo(true));
        assertThat(stringValuesIter.next(), equalTo(two()));
        assertThat(stringValuesIter.hasNext(), equalTo(false));

        stringValuesIter = stringValues.getIter(1);
        assertThat(stringValuesIter.hasNext(), equalTo(true));
        assertThat(stringValuesIter.next(), equalTo(one()));
        assertThat(stringValuesIter.hasNext(), equalTo(false));

        stringValuesIter = stringValues.getIter(2);
        assertThat(stringValuesIter.hasNext(), equalTo(true));
        assertThat(stringValuesIter.next(), equalTo(three()));
        assertThat(stringValuesIter.hasNext(), equalTo(false));

        stringValues.forEachValueInDoc(0, new StringValuesVerifierProc(0).addExpected(two()));
        stringValues.forEachValueInDoc(1, new StringValuesVerifierProc(1).addExpected(one()));
        stringValues.forEachValueInDoc(2, new StringValuesVerifierProc(2).addExpected(three()));

        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopFieldDocs topDocs;

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null))));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(toString(((FieldDoc) topDocs.scoreDocs[0]).fields[0]), equalTo(one()));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(toString(((FieldDoc) topDocs.scoreDocs[1]).fields[0]), equalTo(two()));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));
        assertThat(toString(((FieldDoc) topDocs.scoreDocs[2]).fields[0]), equalTo(three()));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null), true)));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(1));
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


        BytesRefArrayRef bytesRefArrayRef = bytesValues.getValues(0);
        assertThat(bytesRefArrayRef.size(), equalTo(1));
        assertThat(bytesRefArrayRef.values[bytesRefArrayRef.start], equalTo(new BytesRef(two())));

        bytesRefArrayRef = bytesValues.getValues(1);
        assertThat(bytesRefArrayRef.size(), equalTo(0));

        bytesRefArrayRef = bytesValues.getValues(2);
        assertThat(bytesRefArrayRef.size(), equalTo(1));
        assertThat(bytesRefArrayRef.values[bytesRefArrayRef.start], equalTo(new BytesRef(three())));

        BytesValues.Iter bytesValuesIter = bytesValues.getIter(0);
        assertThat(bytesValuesIter.hasNext(), equalTo(true));
        assertThat(bytesValuesIter.next(), equalTo(new BytesRef(two())));
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        bytesValuesIter = bytesValues.getIter(1);
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        bytesValues.forEachValueInDoc(0, new BytesValuesVerifierProc(0).addExpected(two()));
        bytesValues.forEachValueInDoc(1, new BytesValuesVerifierProc(1).addMissing());
        bytesValues.forEachValueInDoc(2, new BytesValuesVerifierProc(2).addExpected(three()));

        HashedBytesValues hashedBytesValues = fieldData.getHashedBytesValues();

        assertThat(hashedBytesValues.hasValue(0), equalTo(true));
        assertThat(hashedBytesValues.hasValue(1), equalTo(false));
        assertThat(hashedBytesValues.hasValue(2), equalTo(true));

        assertThat(hashedBytesValues.getValue(0), equalTo(new HashedBytesRef(two())));
        assertThat(hashedBytesValues.getValue(1), nullValue());
        assertThat(hashedBytesValues.getValue(2), equalTo(new HashedBytesRef(three())));

        HashedBytesValues.Iter hashedBytesValuesIter = hashedBytesValues.getIter(0);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(true));
        assertThat(hashedBytesValuesIter.next(), equalTo(new HashedBytesRef(two())));
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));

        hashedBytesValuesIter = hashedBytesValues.getIter(1);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));

        hashedBytesValues.forEachValueInDoc(0, new HashedBytesValuesVerifierProc(0).addExpected(two()));
        hashedBytesValues.forEachValueInDoc(1, new HashedBytesValuesVerifierProc(1).addMissing());
        hashedBytesValues.forEachValueInDoc(2, new HashedBytesValuesVerifierProc(2).addExpected(three()));

        StringValues stringValues = fieldData.getStringValues();

        assertThat(stringValues.hasValue(0), equalTo(true));
        assertThat(stringValues.hasValue(1), equalTo(false));
        assertThat(stringValues.hasValue(2), equalTo(true));

        assertThat(stringValues.getValue(0), equalTo(two()));
        assertThat(stringValues.getValue(1), nullValue());
        assertThat(stringValues.getValue(2), equalTo(three()));

        StringArrayRef stringArrayRef;
        stringArrayRef = stringValues.getValues(0);
        assertThat(stringArrayRef.size(), equalTo(1));
        assertThat(stringArrayRef.values[stringArrayRef.start], equalTo(two()));

        stringArrayRef = stringValues.getValues(1);
        assertThat(stringArrayRef.size(), equalTo(0));

        stringArrayRef = stringValues.getValues(2);
        assertThat(stringArrayRef.size(), equalTo(1));
        assertThat(stringArrayRef.values[stringArrayRef.start], equalTo(three()));

        StringValues.Iter stringValuesIter = stringValues.getIter(0);
        assertThat(stringValuesIter.hasNext(), equalTo(true));
        assertThat(stringValuesIter.next(), equalTo(two()));
        assertThat(stringValuesIter.hasNext(), equalTo(false));

        stringValuesIter = stringValues.getIter(1);
        assertThat(stringValuesIter.hasNext(), equalTo(false));

        stringValuesIter = stringValues.getIter(2);
        assertThat(stringValuesIter.hasNext(), equalTo(true));
        assertThat(stringValuesIter.next(), equalTo(three()));
        assertThat(stringValuesIter.hasNext(), equalTo(false));

        stringValues.forEachValueInDoc(0, new StringValuesVerifierProc(0).addExpected(two()));
        stringValues.forEachValueInDoc(1, new StringValuesVerifierProc(1).addMissing());
        stringValues.forEachValueInDoc(2, new StringValuesVerifierProc(2).addExpected(three()));

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


        BytesRefArrayRef bytesRefArrayRef = bytesValues.getValues(0);
        assertThat(bytesRefArrayRef.size(), equalTo(2));
        assertThat(bytesRefArrayRef.values[bytesRefArrayRef.start], equalTo(new BytesRef(two())));
        assertThat(bytesRefArrayRef.values[bytesRefArrayRef.start + 1], equalTo(new BytesRef(four())));

        bytesRefArrayRef = bytesValues.getValues(1);
        assertThat(bytesRefArrayRef.size(), equalTo(1));
        assertThat(bytesRefArrayRef.values[bytesRefArrayRef.start], equalTo(new BytesRef(one())));

        bytesRefArrayRef = bytesValues.getValues(2);
        assertThat(bytesRefArrayRef.size(), equalTo(1));
        assertThat(bytesRefArrayRef.values[bytesRefArrayRef.start], equalTo(new BytesRef(three())));

        BytesValues.Iter bytesValuesIter = bytesValues.getIter(0);
        assertThat(bytesValuesIter.hasNext(), equalTo(true));
        assertThat(bytesValuesIter.next(), equalTo(new BytesRef(two())));
        assertThat(bytesValuesIter.hasNext(), equalTo(true));
        assertThat(bytesValuesIter.next(), equalTo(new BytesRef(four())));
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        bytesValues.forEachValueInDoc(0, new BytesValuesVerifierProc(0).addExpected(two()).addExpected(four()));
        bytesValues.forEachValueInDoc(1, new BytesValuesVerifierProc(1).addExpected(one()));
        bytesValues.forEachValueInDoc(2, new BytesValuesVerifierProc(2).addExpected(three()));

        HashedBytesValues hashedBytesValues = fieldData.getHashedBytesValues();

        assertThat(hashedBytesValues.hasValue(0), equalTo(true));
        assertThat(hashedBytesValues.hasValue(1), equalTo(true));
        assertThat(hashedBytesValues.hasValue(2), equalTo(true));

        assertThat(hashedBytesValues.getValue(0), equalTo(new HashedBytesRef(two())));
        assertThat(hashedBytesValues.getValue(1), equalTo(new HashedBytesRef(one())));
        assertThat(hashedBytesValues.getValue(2), equalTo(new HashedBytesRef(three())));

        HashedBytesValues.Iter hashedBytesValuesIter = hashedBytesValues.getIter(0);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(true));
        assertThat(hashedBytesValuesIter.next(), equalTo(new HashedBytesRef(two())));
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(true));
        assertThat(hashedBytesValuesIter.next(), equalTo(new HashedBytesRef(four())));
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));

        hashedBytesValues.forEachValueInDoc(0, new HashedBytesValuesVerifierProc(0).addExpected(two()).addExpected(four()));
        hashedBytesValues.forEachValueInDoc(1, new HashedBytesValuesVerifierProc(1).addExpected(one()));
        hashedBytesValues.forEachValueInDoc(2, new HashedBytesValuesVerifierProc(2).addExpected(three()));

        StringValues stringValues = fieldData.getStringValues();

        assertThat(stringValues.hasValue(0), equalTo(true));
        assertThat(stringValues.hasValue(1), equalTo(true));
        assertThat(stringValues.hasValue(2), equalTo(true));

        assertThat(stringValues.getValue(0), equalTo(two()));
        assertThat(stringValues.getValue(1), equalTo(one()));
        assertThat(stringValues.getValue(2), equalTo(three()));

        StringArrayRef stringArrayRef;
        stringArrayRef = stringValues.getValues(0);
        assertThat(stringArrayRef.size(), equalTo(2));
        assertThat(stringArrayRef.values[stringArrayRef.start], equalTo(two()));
        assertThat(stringArrayRef.values[stringArrayRef.start + 1], equalTo(four()));

        stringArrayRef = stringValues.getValues(1);
        assertThat(stringArrayRef.size(), equalTo(1));
        assertThat(stringArrayRef.values[stringArrayRef.start], equalTo(one()));

        stringArrayRef = stringValues.getValues(2);
        assertThat(stringArrayRef.size(), equalTo(1));
        assertThat(stringArrayRef.values[stringArrayRef.start], equalTo(three()));

        StringValues.Iter stringValuesIter = stringValues.getIter(0);
        assertThat(stringValuesIter.hasNext(), equalTo(true));
        assertThat(stringValuesIter.next(), equalTo(two()));
        assertThat(stringValuesIter.hasNext(), equalTo(true));
        assertThat(stringValuesIter.next(), equalTo(four()));
        assertThat(stringValuesIter.hasNext(), equalTo(false));

        stringValuesIter = stringValues.getIter(1);
        assertThat(stringValuesIter.hasNext(), equalTo(true));
        assertThat(stringValuesIter.next(), equalTo(one()));
        assertThat(stringValuesIter.hasNext(), equalTo(false));

        stringValuesIter = stringValues.getIter(2);
        assertThat(stringValuesIter.hasNext(), equalTo(true));
        assertThat(stringValuesIter.next(), equalTo(three()));
        assertThat(stringValuesIter.hasNext(), equalTo(false));

        stringValues.forEachValueInDoc(0, new StringValuesVerifierProc(0).addExpected(two()).addExpected(four()));
        stringValues.forEachValueInDoc(1, new StringValuesVerifierProc(1).addExpected(one()));
        stringValues.forEachValueInDoc(2, new StringValuesVerifierProc(2).addExpected(three()));

        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer, true));
        TopFieldDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10, new Sort(new SortField("value", indexFieldData.comparatorSource(null))));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs.length, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10, new Sort(new SortField("value", indexFieldData.comparatorSource(null), true)));
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


        BytesRefArrayRef bytesRefArrayRef = bytesValues.getValues(0);
        assertThat(bytesRefArrayRef.size(), equalTo(2));
        assertThat(bytesRefArrayRef.values[bytesRefArrayRef.start], equalTo(new BytesRef(two())));
        assertThat(bytesRefArrayRef.values[bytesRefArrayRef.start + 1], equalTo(new BytesRef(four())));

        bytesRefArrayRef = bytesValues.getValues(1);
        assertThat(bytesRefArrayRef.size(), equalTo(0));

        bytesRefArrayRef = bytesValues.getValues(2);
        assertThat(bytesRefArrayRef.size(), equalTo(1));
        assertThat(bytesRefArrayRef.values[bytesRefArrayRef.start], equalTo(new BytesRef(three())));

        BytesValues.Iter bytesValuesIter = bytesValues.getIter(0);
        assertThat(bytesValuesIter.hasNext(), equalTo(true));
        assertThat(bytesValuesIter.next(), equalTo(new BytesRef(two())));
        assertThat(bytesValuesIter.hasNext(), equalTo(true));
        assertThat(bytesValuesIter.next(), equalTo(new BytesRef(four())));
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        bytesValuesIter = bytesValues.getIter(1);
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        bytesValues.forEachValueInDoc(0, new BytesValuesVerifierProc(0).addExpected(two()).addExpected(four()));
        bytesValues.forEachValueInDoc(1, new BytesValuesVerifierProc(1).addMissing());
        bytesValues.forEachValueInDoc(2, new BytesValuesVerifierProc(2).addExpected(three()));

        HashedBytesValues hashedBytesValues = fieldData.getHashedBytesValues();

        assertThat(hashedBytesValues.hasValue(0), equalTo(true));
        assertThat(hashedBytesValues.hasValue(1), equalTo(false));
        assertThat(hashedBytesValues.hasValue(2), equalTo(true));

        assertThat(hashedBytesValues.getValue(0), equalTo(new HashedBytesRef(two())));
        assertThat(hashedBytesValues.getValue(1), nullValue());
        assertThat(hashedBytesValues.getValue(2), equalTo(new HashedBytesRef(three())));

        HashedBytesValues.Iter hashedBytesValuesIter = hashedBytesValues.getIter(0);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(true));
        assertThat(hashedBytesValuesIter.next(), equalTo(new HashedBytesRef(two())));
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(true));
        assertThat(hashedBytesValuesIter.next(), equalTo(new HashedBytesRef(four())));
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));

        hashedBytesValuesIter = hashedBytesValues.getIter(1);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));

        hashedBytesValues.forEachValueInDoc(0, new HashedBytesValuesVerifierProc(0).addExpected(two()).addExpected(four()));
        hashedBytesValues.forEachValueInDoc(1, new HashedBytesValuesVerifierProc(1).addMissing());
        hashedBytesValues.forEachValueInDoc(2, new HashedBytesValuesVerifierProc(2).addExpected(three()));

        StringValues stringValues = fieldData.getStringValues();

        assertThat(stringValues.hasValue(0), equalTo(true));
        assertThat(stringValues.hasValue(1), equalTo(false));
        assertThat(stringValues.hasValue(2), equalTo(true));

        assertThat(stringValues.getValue(0), equalTo(two()));
        assertThat(stringValues.getValue(1), nullValue());
        assertThat(stringValues.getValue(2), equalTo(three()));

        StringArrayRef stringArrayRef;
        stringArrayRef = stringValues.getValues(0);
        assertThat(stringArrayRef.size(), equalTo(2));
        assertThat(stringArrayRef.values[stringArrayRef.start], equalTo(two()));
        assertThat(stringArrayRef.values[stringArrayRef.start + 1], equalTo(four()));

        stringArrayRef = stringValues.getValues(1);
        assertThat(stringArrayRef.size(), equalTo(0));

        stringArrayRef = stringValues.getValues(2);
        assertThat(stringArrayRef.size(), equalTo(1));
        assertThat(stringArrayRef.values[stringArrayRef.start], equalTo(three()));

        StringValues.Iter stringValuesIter = stringValues.getIter(0);
        assertThat(stringValuesIter.hasNext(), equalTo(true));
        assertThat(stringValuesIter.next(), equalTo(two()));
        assertThat(stringValuesIter.hasNext(), equalTo(true));
        assertThat(stringValuesIter.next(), equalTo(four()));
        assertThat(stringValuesIter.hasNext(), equalTo(false));

        stringValuesIter = stringValues.getIter(1);
        assertThat(stringValuesIter.hasNext(), equalTo(false));

        stringValuesIter = stringValues.getIter(2);
        assertThat(stringValuesIter.hasNext(), equalTo(true));
        assertThat(stringValuesIter.next(), equalTo(three()));
        assertThat(stringValuesIter.hasNext(), equalTo(false));

        stringValues.forEachValueInDoc(0, new StringValuesVerifierProc(0).addExpected(two()).addExpected(four()));
        stringValues.forEachValueInDoc(1, new StringValuesVerifierProc(1).addMissing());
        stringValues.forEachValueInDoc(2, new StringValuesVerifierProc(2).addExpected(three()));
    }

    public void testMissingValueForAll() throws Exception {
        fillAllMissing();
        IndexFieldData indexFieldData = getForField("value");
        AtomicFieldData fieldData = indexFieldData.load(refreshReader());

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


        BytesRefArrayRef bytesRefArrayRef = bytesValues.getValues(0);
        assertThat(bytesRefArrayRef.size(), equalTo(0));

        bytesRefArrayRef = bytesValues.getValues(1);
        assertThat(bytesRefArrayRef.size(), equalTo(0));

        bytesRefArrayRef = bytesValues.getValues(2);
        assertThat(bytesRefArrayRef.size(), equalTo(0));

        BytesValues.Iter bytesValuesIter = bytesValues.getIter(0);
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        bytesValuesIter = bytesValues.getIter(1);
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        bytesValuesIter = bytesValues.getIter(2);
        assertThat(bytesValuesIter.hasNext(), equalTo(false));

        bytesValues.forEachValueInDoc(0, new BytesValuesVerifierProc(0).addMissing());
        bytesValues.forEachValueInDoc(1, new BytesValuesVerifierProc(1).addMissing());
        bytesValues.forEachValueInDoc(2, new BytesValuesVerifierProc(2).addMissing());

        HashedBytesValues hashedBytesValues = fieldData.getHashedBytesValues();

        assertThat(hashedBytesValues.hasValue(0), equalTo(false));
        assertThat(hashedBytesValues.hasValue(1), equalTo(false));
        assertThat(hashedBytesValues.hasValue(2), equalTo(false));

        assertThat(hashedBytesValues.getValue(0), nullValue());
        assertThat(hashedBytesValues.getValue(1), nullValue());
        assertThat(hashedBytesValues.getValue(2), nullValue());

        HashedBytesValues.Iter hashedBytesValuesIter = hashedBytesValues.getIter(0);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));

        hashedBytesValuesIter = hashedBytesValues.getIter(1);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));

        hashedBytesValuesIter = hashedBytesValues.getIter(2);
        assertThat(hashedBytesValuesIter.hasNext(), equalTo(false));

        hashedBytesValues.forEachValueInDoc(0, new HashedBytesValuesVerifierProc(0).addMissing());
        hashedBytesValues.forEachValueInDoc(1, new HashedBytesValuesVerifierProc(1).addMissing());
        hashedBytesValues.forEachValueInDoc(2, new HashedBytesValuesVerifierProc(2).addMissing());

        StringValues stringValues = fieldData.getStringValues();

        assertThat(stringValues.hasValue(0), equalTo(false));
        assertThat(stringValues.hasValue(1), equalTo(false));
        assertThat(stringValues.hasValue(2), equalTo(false));

        assertThat(stringValues.getValue(0), nullValue());
        assertThat(stringValues.getValue(1), nullValue());
        assertThat(stringValues.getValue(2), nullValue());

        StringArrayRef stringArrayRef;
        stringArrayRef = stringValues.getValues(0);
        assertThat(stringArrayRef.size(), equalTo(0));

        stringArrayRef = stringValues.getValues(1);
        assertThat(stringArrayRef.size(), equalTo(0));

        stringArrayRef = stringValues.getValues(2);
        assertThat(stringArrayRef.size(), equalTo(0));

        StringValues.Iter stringValuesIter = stringValues.getIter(0);
        assertThat(stringValuesIter.hasNext(), equalTo(false));

        stringValuesIter = stringValues.getIter(1);
        assertThat(stringValuesIter.hasNext(), equalTo(false));

        stringValuesIter = stringValues.getIter(2);
        assertThat(stringValuesIter.hasNext(), equalTo(false));

        stringValues.forEachValueInDoc(0, new StringValuesVerifierProc(0).addMissing());
        stringValues.forEachValueInDoc(1, new StringValuesVerifierProc(1).addMissing());
        stringValues.forEachValueInDoc(2, new StringValuesVerifierProc(2).addMissing());
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
                new Sort(new SortField("value", indexFieldData.comparatorSource(null))));
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
                new Sort(new SortField("value", indexFieldData.comparatorSource(null), true)));
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
