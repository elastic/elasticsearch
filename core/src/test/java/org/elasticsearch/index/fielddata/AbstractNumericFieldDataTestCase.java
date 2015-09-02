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

package org.elasticsearch.index.fielddata;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.MultiValueMode;
import org.junit.Test;

import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public abstract class AbstractNumericFieldDataTestCase extends AbstractFieldDataImplTestCase {

    @Override
    protected abstract FieldDataType getFieldDataType();

    protected Settings.Builder getFieldDataSettings() {
        Settings.Builder builder = Settings.builder();
        IndexFieldData.CommonSettings.MemoryStorageFormat[] formats = IndexFieldData.CommonSettings.MemoryStorageFormat.values();
        int i = randomInt(formats.length);
        if (i < formats.length) {
            builder.put(IndexFieldData.CommonSettings.SETTING_MEMORY_STORAGE_HINT, formats[i].name().toLowerCase(Locale.ROOT));
        }
        return builder;
    }

    @Test
    public void testSingleValueAllSetNumber() throws Exception {
        fillSingleValueAllSet();
        IndexNumericFieldData indexFieldData = getForField("value");
        AtomicNumericFieldData fieldData = indexFieldData.load(refreshReader());

        SortedNumericDocValues longValues = fieldData.getLongValues();

        assertThat(FieldData.isMultiValued(longValues), equalTo(false));

        longValues.setDocument(0);
        assertThat(longValues.count(), equalTo(1));
        assertThat(longValues.valueAt(0), equalTo(2l));

        longValues.setDocument(1);
        assertThat(longValues.count(), equalTo(1));
        assertThat(longValues.valueAt(0), equalTo(1l));

        longValues.setDocument(2);
        assertThat(longValues.count(), equalTo(1));
        assertThat(longValues.valueAt(0), equalTo(3l));

        SortedNumericDoubleValues doubleValues = fieldData.getDoubleValues();

        assertThat(FieldData.isMultiValued(doubleValues), equalTo(false));

        doubleValues.setDocument(0);
        assertThat(1, equalTo(doubleValues.count()));
        assertThat(doubleValues.valueAt(0), equalTo(2d));

        doubleValues.setDocument(1);
        assertThat(1, equalTo(doubleValues.count()));
        assertThat(doubleValues.valueAt(0), equalTo(1d));

        doubleValues.setDocument(2);
        assertThat(1, equalTo(doubleValues.count()));
        assertThat(doubleValues.valueAt(0), equalTo(3d));

        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopFieldDocs topDocs;

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, MultiValueMode.MIN, null))));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, MultiValueMode.MAX, null), true)));
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

        SortedNumericDocValues longValues = fieldData.getLongValues();

        assertThat(FieldData.isMultiValued(longValues), equalTo(false));

        longValues.setDocument(0);
        assertThat(longValues.count(), equalTo(1));
        assertThat(longValues.valueAt(0), equalTo(2l));

        longValues.setDocument(1);
        assertThat(longValues.count(), equalTo(0));

        longValues.setDocument(2);
        assertThat(longValues.count(), equalTo(1));
        assertThat(longValues.valueAt(0), equalTo(3l));

        SortedNumericDoubleValues doubleValues = fieldData.getDoubleValues();

        assertThat(FieldData.isMultiValued(doubleValues), equalTo(false));

        doubleValues.setDocument(0);
        assertThat(1, equalTo(doubleValues.count()));
        assertThat(doubleValues.valueAt(0), equalTo(2d));

        doubleValues.setDocument(1);
        assertThat(0, equalTo(doubleValues.count()));

        doubleValues.setDocument(2);
        assertThat(1, equalTo(doubleValues.count()));
        assertThat(doubleValues.valueAt(0), equalTo(3d));

        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopFieldDocs topDocs;

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, MultiValueMode.MIN, null)))); // defaults to _last
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(1));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, MultiValueMode.MAX, null), true))); // defaults to _last
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(1));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource("_first", MultiValueMode.MIN, null))));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource("_first", MultiValueMode.MAX, null), true)));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(0));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource("1", MultiValueMode.MIN, null))));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource("1", MultiValueMode.MAX, null), true)));
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

        SortedNumericDocValues longValues = fieldData.getLongValues();

        assertThat(FieldData.isMultiValued(longValues), equalTo(true));

        longValues.setDocument(0);
        assertThat(longValues.count(), equalTo(2));
        assertThat(longValues.valueAt(0), equalTo(2l));
        assertThat(longValues.valueAt(1), equalTo(4l));

        longValues.setDocument(1);
        assertThat(longValues.count(), equalTo(1));
        assertThat(longValues.valueAt(0), equalTo(1l));

        longValues.setDocument(2);
        assertThat(longValues.count(), equalTo(1));
        assertThat(longValues.valueAt(0), equalTo(3l));

        SortedNumericDoubleValues doubleValues = fieldData.getDoubleValues();

        assertThat(FieldData.isMultiValued(doubleValues), equalTo(true));

        doubleValues.setDocument(0);
        assertThat(2, equalTo(doubleValues.count()));
        assertThat(doubleValues.valueAt(0), equalTo(2d));
        assertThat(doubleValues.valueAt(1), equalTo(4d));

        doubleValues.setDocument(1);
        assertThat(1, equalTo(doubleValues.count()));
        assertThat(doubleValues.valueAt(0), equalTo(1d));

        doubleValues.setDocument(2);
        assertThat(1, equalTo(doubleValues.count()));
        assertThat(doubleValues.valueAt(0), equalTo(3d));
    }

    @Test
    public void testMultiValueWithMissingNumber() throws Exception {
        fillMultiValueWithMissing();
        IndexNumericFieldData indexFieldData = getForField("value");
        AtomicNumericFieldData fieldData = indexFieldData.load(refreshReader());

        SortedNumericDocValues longValues = fieldData.getLongValues();

        assertThat(FieldData.isMultiValued(longValues), equalTo(true));

        longValues.setDocument(0);
        assertThat(longValues.count(), equalTo(2));
        assertThat(longValues.valueAt(0), equalTo(2l));
        assertThat(longValues.valueAt(1), equalTo(4l));

        longValues.setDocument(1);
        assertThat(longValues.count(), equalTo(0));

        longValues.setDocument(2);
        assertThat(longValues.count(), equalTo(1));
        assertThat(longValues.valueAt(0), equalTo(3l));

        SortedNumericDoubleValues doubleValues = fieldData.getDoubleValues();

        assertThat(FieldData.isMultiValued(doubleValues), equalTo(true));

        doubleValues.setDocument(0);
        assertThat(2, equalTo(doubleValues.count()));
        assertThat(doubleValues.valueAt(0), equalTo(2d));
        assertThat(doubleValues.valueAt(1), equalTo(4d));

        doubleValues.setDocument(1);
        assertThat(0, equalTo(doubleValues.count()));

        doubleValues.setDocument(2);
        assertThat(1, equalTo(doubleValues.count()));
        assertThat(doubleValues.valueAt(0), equalTo(3d));

    }

    @Override
    @Test
    public void testMissingValueForAll() throws Exception {
        fillAllMissing();
        IndexNumericFieldData indexFieldData = getForField("value");
        AtomicNumericFieldData fieldData = indexFieldData.load(refreshReader());

        // long values

        SortedNumericDocValues longValues = fieldData.getLongValues();

        assertThat(FieldData.isMultiValued(longValues), equalTo(false));

        for (int i = 0; i < 3; ++i) {
            longValues.setDocument(0);
            assertThat(longValues.count(), equalTo(0));
        }

        // double values

        SortedNumericDoubleValues doubleValues = fieldData.getDoubleValues();

        assertThat(FieldData.isMultiValued(doubleValues), equalTo(false));

        doubleValues.setDocument(0);
        assertThat(0, equalTo(doubleValues.count()));

        doubleValues.setDocument(1);
        assertThat(0, equalTo(doubleValues.count()));

        doubleValues.setDocument(2);
        assertThat(0, equalTo(doubleValues.count()));
    }


    @Override
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

    @Override
    @Test
    public void testSortMultiValuesFields() throws Exception {
        fillExtendedMvSet();
        IndexFieldData indexFieldData = getForField("value");

        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer, true));
        TopFieldDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, MultiValueMode.MIN, null)))); // defaults to _last
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
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, MultiValueMode.MAX, null), true))); // defaults to _last
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
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, MultiValueMode.SUM, null)))); // defaults to _last
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
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, MultiValueMode.SUM, null), true))); // defaults to _last
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
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, MultiValueMode.AVG, null)))); // defaults to _last
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
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, MultiValueMode.AVG, null), true))); // defaults to _last
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
                new Sort(new SortField("value", indexFieldData.comparatorSource("_first", MultiValueMode.MIN, null))));
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
                new Sort(new SortField("value", indexFieldData.comparatorSource("_first", MultiValueMode.MAX, null), true)));
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
                new Sort(new SortField("value", indexFieldData.comparatorSource("-9", MultiValueMode.MIN, null))));
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
                new Sort(new SortField("value", indexFieldData.comparatorSource("9", MultiValueMode.MAX, null), true)));
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
