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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.*;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.fieldcomparator.SortMode;
import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

/**
 */
@Ignore("abstract")
public abstract class NumericFieldDataTests extends AbstractStringFieldDataTests {

    protected abstract FieldDataType getFieldDataType();

    @Test
    public void testSingleValueAllSetNumber() throws Exception {
        fillSingleValueAllSet();
        IndexNumericFieldData indexFieldData = getForField("value");
        AtomicNumericFieldData fieldData = indexFieldData.load(refreshReader());

        assertThat(fieldData.getNumDocs(), equalTo(3));

        LongValues longValues = fieldData.getLongValues();

        assertThat(longValues.isMultiValued(), equalTo(false));

        assertThat(longValues.hasValue(0), equalTo(true));
        assertThat(longValues.hasValue(1), equalTo(true));
        assertThat(longValues.hasValue(2), equalTo(true));

        assertThat(longValues.getValue(0), equalTo(2l));
        assertThat(longValues.getValue(1), equalTo(1l));
        assertThat(longValues.getValue(2), equalTo(3l));

        assertThat(longValues.getValueMissing(0, -1), equalTo(2l));
        assertThat(longValues.getValueMissing(1, -1), equalTo(1l));
        assertThat(longValues.getValueMissing(2, -1), equalTo(3l));

        LongValues.Iter longValuesIter = longValues.getIter(0);
        assertThat(longValuesIter.hasNext(), equalTo(true));
        assertThat(longValuesIter.next(), equalTo(2l));
        assertThat(longValuesIter.hasNext(), equalTo(false));

        longValuesIter = longValues.getIter(1);
        assertThat(longValuesIter.hasNext(), equalTo(true));
        assertThat(longValuesIter.next(), equalTo(1l));
        assertThat(longValuesIter.hasNext(), equalTo(false));

        longValuesIter = longValues.getIter(2);
        assertThat(longValuesIter.hasNext(), equalTo(true));
        assertThat(longValuesIter.next(), equalTo(3l));
        assertThat(longValuesIter.hasNext(), equalTo(false));

        DoubleValues doubleValues = fieldData.getDoubleValues();

        assertThat(doubleValues.isMultiValued(), equalTo(false));

        assertThat(doubleValues.hasValue(0), equalTo(true));
        assertThat(doubleValues.hasValue(1), equalTo(true));
        assertThat(doubleValues.hasValue(2), equalTo(true));

        assertThat(doubleValues.getValue(0), equalTo(2d));
        assertThat(doubleValues.getValue(1), equalTo(1d));
        assertThat(doubleValues.getValue(2), equalTo(3d));

        assertThat(doubleValues.getValueMissing(0, -1), equalTo(2d));
        assertThat(doubleValues.getValueMissing(1, -1), equalTo(1d));
        assertThat(doubleValues.getValueMissing(2, -1), equalTo(3d));

        DoubleValues.Iter doubleValuesIter = doubleValues.getIter(0);
        assertThat(doubleValuesIter.hasNext(), equalTo(true));
        assertThat(doubleValuesIter.next(), equalTo(2d));
        assertThat(doubleValuesIter.hasNext(), equalTo(false));

        doubleValuesIter = doubleValues.getIter(1);
        assertThat(doubleValuesIter.hasNext(), equalTo(true));
        assertThat(doubleValuesIter.next(), equalTo(1d));
        assertThat(doubleValuesIter.hasNext(), equalTo(false));

        doubleValuesIter = doubleValues.getIter(2);
        assertThat(doubleValuesIter.hasNext(), equalTo(true));
        assertThat(doubleValuesIter.next(), equalTo(3d));
        assertThat(doubleValuesIter.hasNext(), equalTo(false));

        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopFieldDocs topDocs;

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, SortMode.MIN))));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, SortMode.MAX), true)));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(1));
    }

    @Test
    public void testSingleValueWithMissingNumber() throws Exception {
        fillSingleValueWithMissing();
        IndexNumericFieldData indexFieldData = getForField("value");
        AtomicNumericFieldData fieldData = indexFieldData.load(refreshReader());

        assertThat(fieldData.getNumDocs(), equalTo(3));

        LongValues longValues = fieldData.getLongValues();

        assertThat(longValues.isMultiValued(), equalTo(false));

        assertThat(longValues.hasValue(0), equalTo(true));
        assertThat(longValues.hasValue(1), equalTo(false));
        assertThat(longValues.hasValue(2), equalTo(true));

        assertThat(longValues.getValue(0), equalTo(2l));
        assertThat(longValues.getValue(2), equalTo(3l));

        assertThat(longValues.getValueMissing(0, -1), equalTo(2l));
        assertThat(longValues.getValueMissing(1, -1), equalTo(-1l));
        assertThat(longValues.getValueMissing(2, -1), equalTo(3l));

        LongValues.Iter longValuesIter = longValues.getIter(0);
        assertThat(longValuesIter.hasNext(), equalTo(true));
        assertThat(longValuesIter.next(), equalTo(2l));
        assertThat(longValuesIter.hasNext(), equalTo(false));

        longValuesIter = longValues.getIter(1);
        assertThat(longValuesIter.hasNext(), equalTo(false));

        longValuesIter = longValues.getIter(2);
        assertThat(longValuesIter.hasNext(), equalTo(true));
        assertThat(longValuesIter.next(), equalTo(3l));
        assertThat(longValuesIter.hasNext(), equalTo(false));

        DoubleValues doubleValues = fieldData.getDoubleValues();

        assertThat(doubleValues.isMultiValued(), equalTo(false));

        assertThat(doubleValues.hasValue(0), equalTo(true));
        assertThat(doubleValues.hasValue(1), equalTo(false));
        assertThat(doubleValues.hasValue(2), equalTo(true));

        assertThat(doubleValues.getValue(0), equalTo(2d));
        assertThat(doubleValues.getValue(2), equalTo(3d));

        assertThat(doubleValues.getValueMissing(0, -1), equalTo(2d));
        assertThat(doubleValues.getValueMissing(1, -1), equalTo(-1d));
        assertThat(doubleValues.getValueMissing(2, -1), equalTo(3d));

        DoubleValues.Iter doubleValuesIter = doubleValues.getIter(0);
        assertThat(doubleValuesIter.hasNext(), equalTo(true));
        assertThat(doubleValuesIter.next(), equalTo(2d));
        assertThat(doubleValuesIter.hasNext(), equalTo(false));

        doubleValuesIter = doubleValues.getIter(1);
        assertThat(doubleValuesIter.hasNext(), equalTo(false));

        doubleValuesIter = doubleValues.getIter(2);
        assertThat(doubleValuesIter.hasNext(), equalTo(true));
        assertThat(doubleValuesIter.next(), equalTo(3d));
        assertThat(doubleValuesIter.hasNext(), equalTo(false));

        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopFieldDocs topDocs;

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, SortMode.MIN)))); // defaults to _last
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(1));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, SortMode.MAX), true))); // defaults to _last
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(1));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource("_first", SortMode.MIN))));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource("_first", SortMode.MAX), true)));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(0));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource("1", SortMode.MIN))));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource("1", SortMode.MAX), true)));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(1));
    }

    @Test
    public void testMultiValueAllSetNumber() throws Exception {
        fillMultiValueAllSet();
        IndexNumericFieldData indexFieldData = getForField("value");
        AtomicNumericFieldData fieldData = indexFieldData.load(refreshReader());

        assertThat(fieldData.getNumDocs(), equalTo(3));

        LongValues longValues = fieldData.getLongValues();

        assertThat(longValues.isMultiValued(), equalTo(true));

        assertThat(longValues.hasValue(0), equalTo(true));
        assertThat(longValues.hasValue(1), equalTo(true));
        assertThat(longValues.hasValue(2), equalTo(true));

        assertThat(longValues.getValue(0), equalTo(2l));
        assertThat(longValues.getValue(1), equalTo(1l));
        assertThat(longValues.getValue(2), equalTo(3l));

        assertThat(longValues.getValueMissing(0, -1), equalTo(2l));
        assertThat(longValues.getValueMissing(1, -1), equalTo(1l));
        assertThat(longValues.getValueMissing(2, -1), equalTo(3l));

        LongValues.Iter longValuesIter = longValues.getIter(0);
        assertThat(longValuesIter.hasNext(), equalTo(true));
        assertThat(longValuesIter.next(), equalTo(2l));
        assertThat(longValuesIter.hasNext(), equalTo(true));
        assertThat(longValuesIter.next(), equalTo(4l));
        assertThat(longValuesIter.hasNext(), equalTo(false));

        longValuesIter = longValues.getIter(1);
        assertThat(longValuesIter.hasNext(), equalTo(true));
        assertThat(longValuesIter.next(), equalTo(1l));
        assertThat(longValuesIter.hasNext(), equalTo(false));

        longValuesIter = longValues.getIter(2);
        assertThat(longValuesIter.hasNext(), equalTo(true));
        assertThat(longValuesIter.next(), equalTo(3l));
        assertThat(longValuesIter.hasNext(), equalTo(false));

        DoubleValues doubleValues = fieldData.getDoubleValues();

        assertThat(doubleValues.isMultiValued(), equalTo(true));

        assertThat(doubleValues.hasValue(0), equalTo(true));
        assertThat(doubleValues.hasValue(1), equalTo(true));
        assertThat(doubleValues.hasValue(2), equalTo(true));

        assertThat(doubleValues.getValue(0), equalTo(2d));
        assertThat(doubleValues.getValue(1), equalTo(1d));
        assertThat(doubleValues.getValue(2), equalTo(3d));

        assertThat(doubleValues.getValueMissing(0, -1), equalTo(2d));
        assertThat(doubleValues.getValueMissing(1, -1), equalTo(1d));
        assertThat(doubleValues.getValueMissing(2, -1), equalTo(3d));

        DoubleValues.Iter doubleValuesIter = doubleValues.getIter(0);
        assertThat(doubleValuesIter.hasNext(), equalTo(true));
        assertThat(doubleValuesIter.next(), equalTo(2d));
        assertThat(doubleValuesIter.hasNext(), equalTo(true));
        assertThat(doubleValuesIter.next(), equalTo(4d));
        assertThat(doubleValuesIter.hasNext(), equalTo(false));

        doubleValuesIter = doubleValues.getIter(1);
        assertThat(doubleValuesIter.hasNext(), equalTo(true));
        assertThat(doubleValuesIter.next(), equalTo(1d));
        assertThat(doubleValuesIter.hasNext(), equalTo(false));

        doubleValuesIter = doubleValues.getIter(2);
        assertThat(doubleValuesIter.hasNext(), equalTo(true));
        assertThat(doubleValuesIter.next(), equalTo(3d));
        assertThat(doubleValuesIter.hasNext(), equalTo(false));
    }

    @Test
    public void testMultiValueWithMissingNumber() throws Exception {
        fillMultiValueWithMissing();
        IndexNumericFieldData indexFieldData = getForField("value");
        AtomicNumericFieldData fieldData = indexFieldData.load(refreshReader());

        assertThat(fieldData.getNumDocs(), equalTo(3));

        LongValues longValues = fieldData.getLongValues();

        assertThat(longValues.isMultiValued(), equalTo(true));

        assertThat(longValues.hasValue(0), equalTo(true));
        assertThat(longValues.hasValue(1), equalTo(false));
        assertThat(longValues.hasValue(2), equalTo(true));

        assertThat(longValues.getValue(0), equalTo(2l));
        assertThat(longValues.getValue(2), equalTo(3l));

        assertThat(longValues.getValueMissing(0, -1), equalTo(2l));
        assertThat(longValues.getValueMissing(1, -1), equalTo(-1l));
        assertThat(longValues.getValueMissing(2, -1), equalTo(3l));

        LongValues.Iter longValuesIter = longValues.getIter(0);
        assertThat(longValuesIter.hasNext(), equalTo(true));
        assertThat(longValuesIter.next(), equalTo(2l));
        assertThat(longValuesIter.hasNext(), equalTo(true));
        assertThat(longValuesIter.next(), equalTo(4l));
        assertThat(longValuesIter.hasNext(), equalTo(false));

        longValuesIter = longValues.getIter(1);
        assertThat(longValuesIter.hasNext(), equalTo(false));

        longValuesIter = longValues.getIter(2);
        assertThat(longValuesIter.hasNext(), equalTo(true));
        assertThat(longValuesIter.next(), equalTo(3l));
        assertThat(longValuesIter.hasNext(), equalTo(false));

        DoubleValues doubleValues = fieldData.getDoubleValues();

        assertThat(doubleValues.isMultiValued(), equalTo(true));

        assertThat(doubleValues.hasValue(0), equalTo(true));
        assertThat(doubleValues.hasValue(1), equalTo(false));
        assertThat(doubleValues.hasValue(2), equalTo(true));

        assertThat(doubleValues.getValue(0), equalTo(2d));
        assertThat(doubleValues.getValue(2), equalTo(3d));

        assertThat(doubleValues.getValueMissing(0, -1), equalTo(2d));
        assertThat(doubleValues.getValueMissing(1, -1), equalTo(-1d));
        assertThat(doubleValues.getValueMissing(2, -1), equalTo(3d));

        DoubleValues.Iter doubleValuesIter = doubleValues.getIter(0);
        assertThat(doubleValuesIter.hasNext(), equalTo(true));
        assertThat(doubleValuesIter.next(), equalTo(2d));
        assertThat(doubleValuesIter.hasNext(), equalTo(true));
        assertThat(doubleValuesIter.next(), equalTo(4d));
        assertThat(doubleValuesIter.hasNext(), equalTo(false));

        doubleValuesIter = doubleValues.getIter(1);
        assertThat(doubleValuesIter.hasNext(), equalTo(false));

        doubleValuesIter = doubleValues.getIter(2);
        assertThat(doubleValuesIter.hasNext(), equalTo(true));
        assertThat(doubleValuesIter.next(), equalTo(3d));
        assertThat(doubleValuesIter.hasNext(), equalTo(false));
    }

    @Test
    public void testMissingValueForAll() throws Exception {
        fillAllMissing();
        IndexNumericFieldData indexFieldData = getForField("value");
        AtomicNumericFieldData fieldData = indexFieldData.load(refreshReader());

        assertThat(fieldData.getNumDocs(), equalTo(0));

        // long values

        LongValues longValues = fieldData.getLongValues();

        assertThat(longValues.isMultiValued(), equalTo(false));

        assertThat(longValues.hasValue(0), equalTo(false));
        assertThat(longValues.hasValue(1), equalTo(false));
        assertThat(longValues.hasValue(2), equalTo(false));

        assertThat(longValues.getValueMissing(0, -1), equalTo(-1l));
        assertThat(longValues.getValueMissing(1, -1), equalTo(-1l));
        assertThat(longValues.getValueMissing(2, -1), equalTo(-1l));

        LongValues.Iter longValuesIter = longValues.getIter(0);
        assertThat(longValuesIter.hasNext(), equalTo(false));

        longValuesIter = longValues.getIter(1);
        assertThat(longValuesIter.hasNext(), equalTo(false));

        longValuesIter = longValues.getIter(2);
        assertThat(longValuesIter.hasNext(), equalTo(false));

        // double values

        DoubleValues doubleValues = fieldData.getDoubleValues();

        assertThat(doubleValues.isMultiValued(), equalTo(false));

        assertThat(doubleValues.hasValue(0), equalTo(false));
        assertThat(doubleValues.hasValue(1), equalTo(false));
        assertThat(doubleValues.hasValue(2), equalTo(false));

        assertThat(doubleValues.getValueMissing(0, -1), equalTo(-1d));
        assertThat(doubleValues.getValueMissing(1, -1), equalTo(-1d));
        assertThat(doubleValues.getValueMissing(2, -1), equalTo(-1d));

        DoubleValues.Iter doubleValuesIter = doubleValues.getIter(0);
        assertThat(doubleValuesIter.hasNext(), equalTo(false));

        doubleValuesIter = doubleValues.getIter(1);
        assertThat(doubleValuesIter.hasNext(), equalTo(false));

        doubleValuesIter = doubleValues.getIter(2);
        assertThat(doubleValuesIter.hasNext(), equalTo(false));
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
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, SortMode.MIN)))); // defaults to _last
        assertThat(topDocs.totalHits, equalTo(8));
        assertThat(topDocs.scoreDocs.length, equalTo(8));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(7));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).intValue(), equalTo(-10));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).intValue(), equalTo(2));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(3));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).intValue(), equalTo(4));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(4));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).intValue(), equalTo(6));
        assertThat(topDocs.scoreDocs[5].doc, equalTo(6));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[5]).fields[0]).intValue(), equalTo(8));
        assertThat(topDocs.scoreDocs[6].doc, equalTo(1));
//        assertThat(((FieldDoc) topDocs.scoreDocs[6]).fields[0], equalTo(null));
        assertThat(topDocs.scoreDocs[7].doc, equalTo(5));
//        assertThat(((FieldDoc) topDocs.scoreDocs[7]).fields[0], equalTo(null));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, SortMode.MAX), true))); // defaults to _last
        assertThat(topDocs.totalHits, equalTo(8));
        assertThat(topDocs.scoreDocs.length, equalTo(8));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(6));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).intValue(), equalTo(10));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(4));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).intValue(), equalTo(8));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(3));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).intValue(), equalTo(6));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(0));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).intValue(), equalTo(4));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(2));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[5].doc, equalTo(7));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[5]).fields[0]).intValue(), equalTo(-8));
        assertThat(topDocs.scoreDocs[6].doc, equalTo(1));
//        assertThat(((FieldDoc) topDocs.scoreDocs[6]).fields[0], equalTo(null));
        assertThat(topDocs.scoreDocs[7].doc, equalTo(5));
//        assertThat(((FieldDoc) topDocs.scoreDocs[7]).fields[0], equalTo(null));

        searcher = new IndexSearcher(DirectoryReader.open(writer, true));
        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, SortMode.SUM)))); // defaults to _last
        assertThat(topDocs.totalHits, equalTo(8));
        assertThat(topDocs.scoreDocs.length, equalTo(8));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(7));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).intValue(), equalTo(-27));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(0));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).intValue(), equalTo(6));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(3));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).intValue(), equalTo(15));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(4));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).intValue(), equalTo(21));
        assertThat(topDocs.scoreDocs[5].doc, equalTo(6));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[5]).fields[0]).intValue(), equalTo(27));
        assertThat(topDocs.scoreDocs[6].doc, equalTo(1));
//        assertThat(((FieldDoc) topDocs.scoreDocs[6]).fields[0], equalTo(null));
        assertThat(topDocs.scoreDocs[7].doc, equalTo(5));
//        assertThat(((FieldDoc) topDocs.scoreDocs[7]).fields[0], equalTo(null));

        searcher = new IndexSearcher(DirectoryReader.open(writer, true));
        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, SortMode.SUM), true))); // defaults to _last
        assertThat(topDocs.totalHits, equalTo(8));
        assertThat(topDocs.scoreDocs.length, equalTo(8));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(6));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).intValue(), equalTo(27));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(4));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).intValue(), equalTo(21));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(3));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).intValue(), equalTo(15));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(0));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).intValue(), equalTo(6));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(2));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[5].doc, equalTo(7));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[5]).fields[0]).intValue(), equalTo(-27));
        assertThat(topDocs.scoreDocs[6].doc, equalTo(1));
//        assertThat(((FieldDoc) topDocs.scoreDocs[6]).fields[0], equalTo(null));
        assertThat(topDocs.scoreDocs[7].doc, equalTo(5));
//        assertThat(((FieldDoc) topDocs.scoreDocs[7]).fields[0], equalTo(null));

        searcher = new IndexSearcher(DirectoryReader.open(writer, true));
        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, SortMode.AVG)))); // defaults to _last
        assertThat(topDocs.totalHits, equalTo(8));
        assertThat(topDocs.scoreDocs.length, equalTo(8));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(7));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).intValue(), equalTo(-9));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(3));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).intValue(), equalTo(5));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(4));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).intValue(), equalTo(7));
        assertThat(topDocs.scoreDocs[5].doc, equalTo(6));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[5]).fields[0]).intValue(), equalTo(9));
        assertThat(topDocs.scoreDocs[6].doc, equalTo(1));
//        assertThat(((FieldDoc) topDocs.scoreDocs[6]).fields[0], equalTo(null));
        assertThat(topDocs.scoreDocs[7].doc, equalTo(5));
//        assertThat(((FieldDoc) topDocs.scoreDocs[7]).fields[0], equalTo(null));

        searcher = new IndexSearcher(DirectoryReader.open(writer, true));
        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, SortMode.AVG), true))); // defaults to _last
        assertThat(topDocs.totalHits, equalTo(8));
        assertThat(topDocs.scoreDocs.length, equalTo(8));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(6));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).intValue(), equalTo(9));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(4));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).intValue(), equalTo(7));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(3));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).intValue(), equalTo(5));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(0));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(2));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).intValue(), equalTo(3));
        assertThat(topDocs.scoreDocs[5].doc, equalTo(7));
        assertThat(((Number) ((FieldDoc) topDocs.scoreDocs[5]).fields[0]).intValue(), equalTo(-9));
        assertThat(topDocs.scoreDocs[6].doc, equalTo(1));
//        assertThat(((FieldDoc) topDocs.scoreDocs[6]).fields[0], equalTo(null));
        assertThat(topDocs.scoreDocs[7].doc, equalTo(5));
//        assertThat(((FieldDoc) topDocs.scoreDocs[7]).fields[0], equalTo(null));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource("_first", SortMode.MIN))));
        assertThat(topDocs.totalHits, equalTo(8));
        assertThat(topDocs.scoreDocs.length, equalTo(8));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(5));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(7));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[5].doc, equalTo(3));
        assertThat(topDocs.scoreDocs[6].doc, equalTo(4));
        assertThat(topDocs.scoreDocs[7].doc, equalTo(6));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource("_first", SortMode.MAX), true)));
        assertThat(topDocs.totalHits, equalTo(8));
        assertThat(topDocs.scoreDocs.length, equalTo(8));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(5));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(6));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(4));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(3));
        assertThat(topDocs.scoreDocs[5].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[6].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[7].doc, equalTo(7));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource("-9", SortMode.MIN))));
        assertThat(topDocs.totalHits, equalTo(8));
        assertThat(topDocs.scoreDocs.length, equalTo(8));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(7));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(5));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[5].doc, equalTo(3));
        assertThat(topDocs.scoreDocs[6].doc, equalTo(4));
        assertThat(topDocs.scoreDocs[7].doc, equalTo(6));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource("9", SortMode.MAX), true)));
        assertThat(topDocs.totalHits, equalTo(8));
        assertThat(topDocs.scoreDocs.length, equalTo(8));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(6));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(5));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(4));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(3));
        assertThat(topDocs.scoreDocs[5].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[6].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[7].doc, equalTo(7));
    }

}
