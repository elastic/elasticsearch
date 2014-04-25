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

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.HashedBytesRef;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.search.MultiValueMode;
import org.junit.Test;

import static org.hamcrest.Matchers.*;

public abstract class AbstractFieldDataImplTests extends AbstractFieldDataTests {

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

    protected String toString(Object value) {
        if (value instanceof BytesRef) {
            return ((BytesRef) value).utf8ToString();
        }
        return value.toString();
    }

    protected abstract void fillSingleValueAllSet() throws Exception;

    protected abstract void add2SingleValuedDocumentsAndDeleteOneOfThem() throws Exception;

    @Test
    public void testDeletedDocs() throws Exception {
        add2SingleValuedDocumentsAndDeleteOneOfThem();
        IndexFieldData indexFieldData = getForField("value");
        AtomicReaderContext readerContext = refreshReader();
        AtomicFieldData fieldData = indexFieldData.load(readerContext);
        BytesValues values = fieldData.getBytesValues(randomBoolean());
        for (int i = 0; i < readerContext.reader().maxDoc(); ++i) {
            assertThat(values.setDocument(i), greaterThanOrEqualTo(1));
        }
    }

    @Test
    public void testSingleValueAllSet() throws Exception {
        fillSingleValueAllSet();
        IndexFieldData indexFieldData = getForField("value");
        AtomicReaderContext readerContext = refreshReader();
        AtomicFieldData fieldData = indexFieldData.load(readerContext);
        assertThat(fieldData.getMemorySizeInBytes(), greaterThan(0l));

        BytesValues bytesValues = fieldData.getBytesValues(randomBoolean());

        assertThat(bytesValues.isMultiValued(), equalTo(false));

        assertThat(bytesValues.setDocument(0), equalTo(1));
        assertThat(bytesValues.nextValue(), equalTo(new BytesRef(two())));
        assertThat(bytesValues.setDocument(1), equalTo(1));
        assertThat(bytesValues.nextValue(), equalTo(new BytesRef(one())));
        assertThat(bytesValues.setDocument(2), equalTo(1));
        assertThat(bytesValues.nextValue(), equalTo(new BytesRef(three())));

        assertValues(bytesValues, 0, two());
        assertValues(bytesValues, 1, one());
        assertValues(bytesValues, 2, three());

        BytesValues hashedBytesValues = fieldData.getBytesValues(randomBoolean());

        assertThat(convert(hashedBytesValues, 0), equalTo(new HashedBytesRef(two())));
        assertThat(convert(hashedBytesValues, 1), equalTo(new HashedBytesRef(one())));
        assertThat(convert(hashedBytesValues, 2), equalTo(new HashedBytesRef(three())));
        assertHashedValues(hashedBytesValues, 0, two());
        assertHashedValues(hashedBytesValues, 1, one());
        assertHashedValues(hashedBytesValues, 2, three());

        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopFieldDocs topDocs;

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, MultiValueMode.MIN))));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(toString(((FieldDoc) topDocs.scoreDocs[0]).fields[0]), equalTo(one()));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(toString(((FieldDoc) topDocs.scoreDocs[1]).fields[0]), equalTo(two()));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));
        assertThat(toString(((FieldDoc) topDocs.scoreDocs[2]).fields[0]), equalTo(three()));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, MultiValueMode.MAX), true)));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(1));
    }
    
    private HashedBytesRef convert(BytesValues values, int doc) {
        if (values.setDocument(doc) > 0) {
            return new HashedBytesRef(BytesRef.deepCopyOf(values.nextValue()), values.currentValueHash());
        } else {
            return new HashedBytesRef(new BytesRef());
        }
    }

    protected abstract void fillSingleValueWithMissing() throws Exception;
    
    public void assertValues(BytesValues values, int docId, BytesRef... actualValues) {
        assertThat(values.setDocument(docId), equalTo(actualValues.length));
        for (int i = 0; i < actualValues.length; i++) {
            assertThat(values.nextValue(), equalTo(actualValues[i]));
        }
    }
    
    public void assertValues(BytesValues values, int docId, String... actualValues) {
        assertThat(values.setDocument(docId), equalTo(actualValues.length));
        for (int i = 0; i < actualValues.length; i++) {
            assertThat(values.nextValue(), equalTo(new BytesRef(actualValues[i])));
        }
    }
    
    public void assertHashedValues(BytesValues values, int docId, BytesRef... actualValues) {
        assertThat(values.setDocument(docId), equalTo(actualValues.length));
        BytesRef r = new BytesRef();
        for (int i = 0; i < actualValues.length; i++) {
            assertThat(values.nextValue(), equalTo(new HashedBytesRef(actualValues[i]).bytes));
            assertThat(values.currentValueHash(), equalTo(new HashedBytesRef(actualValues[i]).hash));

        }
    }
    
    public void assertHashedValues(BytesValues values, int docId, String... actualValues) {
        assertThat(values.setDocument(docId), equalTo(actualValues.length));
        for (int i = 0; i < actualValues.length; i++) {
            assertThat(values.nextValue(), equalTo(new HashedBytesRef(actualValues[i]).bytes));
            assertThat(values.currentValueHash(), equalTo(new HashedBytesRef(actualValues[i]).hash));
        }
    }


    @Test
    public void testSingleValueWithMissing() throws Exception {
        fillSingleValueWithMissing();
        IndexFieldData indexFieldData = getForField("value");
        AtomicFieldData fieldData = indexFieldData.load(refreshReader());
        assertThat(fieldData.getMemorySizeInBytes(), greaterThan(0l));

        BytesValues bytesValues = fieldData
                .getBytesValues(randomBoolean());

        assertThat(bytesValues.isMultiValued(), equalTo(false));

        assertValues(bytesValues, 0, two());
        assertValues(bytesValues, 1, Strings.EMPTY_ARRAY);
        assertValues(bytesValues, 2, three());

        BytesValues hashedBytesValues = fieldData.getBytesValues(randomBoolean());
        assertThat(convert(hashedBytesValues, 0), equalTo(new HashedBytesRef(two())));
        assertThat(convert(hashedBytesValues, 1), equalTo(new HashedBytesRef(new BytesRef())));
        assertThat(convert(hashedBytesValues, 2), equalTo(new HashedBytesRef(three())));

        assertHashedValues(hashedBytesValues, 0, two());
        assertHashedValues(hashedBytesValues, 1, Strings.EMPTY_ARRAY);
        assertHashedValues(hashedBytesValues,  2, three());


    }

    protected abstract void fillMultiValueAllSet() throws Exception;

    @Test
    public void testMultiValueAllSet() throws Exception {
        fillMultiValueAllSet();
        IndexFieldData indexFieldData = getForField("value");
        AtomicFieldData fieldData = indexFieldData.load(refreshReader());
        assertThat(fieldData.getMemorySizeInBytes(), greaterThan(0l));

        BytesValues bytesValues = fieldData.getBytesValues(randomBoolean());

        assertThat(bytesValues.isMultiValued(), equalTo(true));

        assertValues(bytesValues, 0, two(), four());
        assertValues(bytesValues, 1, one());
        assertValues(bytesValues, 2, three());


        BytesValues hashedBytesValues = fieldData.getBytesValues(randomBoolean());

        assertThat(convert(hashedBytesValues, 0), equalTo(new HashedBytesRef(two())));
        assertThat(convert(hashedBytesValues, 1), equalTo(new HashedBytesRef(one())));
        assertThat(convert(hashedBytesValues, 2), equalTo(new HashedBytesRef(three())));

        assertHashedValues(hashedBytesValues, 0, two(), four());
        
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer, true));
        TopFieldDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10, new Sort(new SortField("value", indexFieldData.comparatorSource(null, MultiValueMode.MIN))));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs.length, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10, new Sort(new SortField("value", indexFieldData.comparatorSource(null, MultiValueMode.MAX), true)));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs.length, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(1));
    }

    protected abstract void fillMultiValueWithMissing() throws Exception;

    @Test
    public void testMultiValueWithMissing() throws Exception {
        fillMultiValueWithMissing();
        IndexFieldData indexFieldData = getForField("value");
        AtomicFieldData fieldData = indexFieldData.load(refreshReader());
        assertThat(fieldData.getMemorySizeInBytes(), greaterThan(0l));

        BytesValues bytesValues = fieldData.getBytesValues(randomBoolean());

        assertThat(bytesValues.isMultiValued(), equalTo(true));

        assertValues(bytesValues, 0, two(), four());
        assertValues(bytesValues, 1, Strings.EMPTY_ARRAY);

        BytesValues hashedBytesValues = fieldData.getBytesValues(randomBoolean());


        assertThat(convert(hashedBytesValues, 0), equalTo(new HashedBytesRef(two())));
        assertThat(convert(hashedBytesValues, 1), equalTo(new HashedBytesRef(new BytesRef())));
        assertThat(convert(hashedBytesValues, 2), equalTo(new HashedBytesRef(three())));

        assertHashedValues(bytesValues, 0, two(), four());
        assertHashedValues(bytesValues, 1, Strings.EMPTY_ARRAY);
        assertHashedValues(bytesValues, 2, three());

        assertHashedValues(hashedBytesValues, 0, two(), four());
        assertHashedValues(hashedBytesValues, 1, Strings.EMPTY_ARRAY);
        assertHashedValues(hashedBytesValues, 2, three());

    }

    public void testMissingValueForAll() throws Exception {
        fillAllMissing();
        IndexFieldData indexFieldData = getForField("value");
        AtomicFieldData fieldData = indexFieldData.load(refreshReader());
        // Some impls (FST) return size 0 and some (PagedBytes) do take size in the case no actual data is loaded
        assertThat(fieldData.getMemorySizeInBytes(), greaterThanOrEqualTo(0l));

        BytesValues bytesValues = fieldData.getBytesValues(randomBoolean());

        assertThat(bytesValues.isMultiValued(), equalTo(false));

        assertValues(bytesValues, 0, Strings.EMPTY_ARRAY);
        assertValues(bytesValues, 1, Strings.EMPTY_ARRAY);
        assertValues(bytesValues, 2, Strings.EMPTY_ARRAY);
        BytesValues hashedBytesValues = fieldData.getBytesValues(randomBoolean());

        assertValues(hashedBytesValues, 0, Strings.EMPTY_ARRAY);
        assertValues(hashedBytesValues, 1, Strings.EMPTY_ARRAY);
        assertValues(hashedBytesValues, 2, Strings.EMPTY_ARRAY);
    }

    protected abstract void fillAllMissing() throws Exception;

    @Test
    public void testSortMultiValuesFields() throws Exception {
        fillExtendedMvSet();
        IndexFieldData indexFieldData = getForField("value");

        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(writer, true));
        TopFieldDocs topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, MultiValueMode.MIN))));
        assertThat(topDocs.totalHits, equalTo(8));
        assertThat(topDocs.scoreDocs.length, equalTo(8));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(7));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[0]).fields[0]).utf8ToString(), equalTo("!08"));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[1]).fields[0]).utf8ToString(), equalTo("02"));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[2]).fields[0]).utf8ToString(), equalTo("03"));
        assertThat(topDocs.scoreDocs[3].doc, equalTo(3));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[3]).fields[0]).utf8ToString(), equalTo("04"));
        assertThat(topDocs.scoreDocs[4].doc, equalTo(4));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[4]).fields[0]).utf8ToString(), equalTo("06"));
        assertThat(topDocs.scoreDocs[5].doc, equalTo(6));
        assertThat(((BytesRef) ((FieldDoc) topDocs.scoreDocs[5]).fields[0]).utf8ToString(), equalTo("08"));
        assertThat(topDocs.scoreDocs[6].doc, equalTo(1));
        assertThat((BytesRef) ((FieldDoc) topDocs.scoreDocs[6]).fields[0], equalTo(BytesRefFieldComparatorSource.MAX_TERM));
        assertThat(topDocs.scoreDocs[7].doc, equalTo(5));
        assertThat((BytesRef) ((FieldDoc) topDocs.scoreDocs[7]).fields[0], equalTo(BytesRefFieldComparatorSource.MAX_TERM));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null, MultiValueMode.MAX), true)));
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

    protected abstract void fillExtendedMvSet() throws Exception;

}
