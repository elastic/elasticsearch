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

package org.apache.lucene.search.suggest.analyzing;

import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LuceneTestCase;
import org.apache.lucene.util.TestUtil;
import org.junit.Test;

import java.util.*;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class DefaultCollectorTest extends LuceneTestCase {


    @Test
    public void testMultiValuedFields() throws Exception {
        Directory dir = newDirectory();

        IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
        int nValues = 10;
        List<String> values = new ArrayList<>();
        Set<String> fieldNames = new HashSet<>();
        fieldNames.add("string_field");
        Document doc = new Document();
        for (int i = 0; i < nValues; i++) {
            String val = TestUtil.randomUnicodeString(random(), 10);
            values.add(val);
            doc.add(stringField(val));
        }

        iw.addDocument(doc);

        SegmentReader reader = getOnlySegmentReader(iw.getReader());

        DefaultCollector<Long> defaultCollector = new DefaultCollector<>(reader, fieldNames);

        defaultCollector.collect("", collect(0));

        List<DefaultCollector.Result<Long>> results = defaultCollector.get();

        for (DefaultCollector.Result.InnerResult<Long> longInnerResult : results.get(0).innerResults()) {
            for (DefaultCollector.FieldValue fieldValue : longInnerResult.fieldValues) {
                assertThat(fieldValue.name(), equalTo("string_field"));
                assertThat(fieldValue.isString(), equalTo(true));
                assertThat(fieldValue.stringValues().size(), equalTo(values.size()));
                Set<String> actualValueSet = new HashSet<>(values);
                for (String value : fieldValue.stringValues()) {
                    assertTrue(actualValueSet.contains(value));
                }
            }
        }

        iw.close();
        reader.close();
        dir.close();
    }

    @Test
    public void testMissingValues() throws Exception {
        Directory dir = newDirectory();

        IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
        int nDocs = 6;
        for (int i = 0; i < nDocs; i++) {
            Document doc = new Document();
            String val = TestUtil.randomUnicodeString(random(), 10);
            doc.add(stringField(val));
            if (i % 2 == 0) {
                BytesRef binaryDVValue = new BytesRef(TestUtil.randomUnicodeString(random(), 10));
                doc.add(binaryDocValuesField(binaryDVValue));
            }
            iw.addDocument(doc);
        }

        iw.forceMerge(1);
        SegmentReader reader = getOnlySegmentReader(iw.getReader());
        Set<String> fieldNames = new HashSet<>();
        fieldNames.add("string_field");
        fieldNames.add("binary_doc_values");


        DefaultCollector<Long> defaultCollector = new DefaultCollector<>(reader, fieldNames);
        defaultCollector.collect("", collect(0, 1, 2, 3, 4, 5));
        List<DefaultCollector.Result<Long>> results = defaultCollector.get();

        for (DefaultCollector.Result.InnerResult<Long> longInnerResult : results.get(0).innerResults()) {
            assertThat(longInnerResult.fieldValues.size(), equalTo(2));
            for (DefaultCollector.FieldValue fieldValue : longInnerResult.fieldValues) {
                if (fieldValue.isString()) {
                    assertThat(fieldValue.name(), equalTo("string_field"));
                } else if (fieldValue.isBinary()) {
                    assertThat(fieldValue.name(), equalTo("binary_doc_values"));
                } else {
                    fail("expected field to be either string or binary");
                }
            }
        }
        iw.close();
        reader.close();
        dir.close();
    }

    @Test
    public void testNonExistentField() throws Exception {
        Directory dir = newDirectory();

        IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
        int nValues = 10;
        List<String> values = new ArrayList<>();
        Document doc = new Document();
        for (int i = 0; i < nValues; i++) {
            String val = TestUtil.randomUnicodeString(random(), 10);
            values.add(val);
            doc.add(stringField(val));
        }

        iw.addDocument(doc);

        SegmentReader reader = getOnlySegmentReader(iw.getReader());
        Set<String> fieldNames = new HashSet<>();
        fieldNames.add("string_field");
        fieldNames.add("bogus_field");
        try {
            DefaultCollector<Long> defaultCollector = new DefaultCollector<>(reader, fieldNames);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("bogus_field"));
        }

        iw.close();
        reader.close();
        dir.close();
    }


    private Field idField(String value) {
        return newStringField("id", value, Field.Store.YES);
    }

    private Field stringField(String value) {
        return newStringField("string_field", value, Field.Store.YES);
    }

    private Field binaryDocValuesField(BytesRef value) {
        return new BinaryDocValuesField("binary_doc_values", value);
    }

    private Field numericDocValuesField(Long value) {
        return new NumericDocValuesField("numeric_doc_values", value);
    }

    private Field sortedDocValueField(BytesRef value) {
        return new SortedDocValuesField("sorted_doc_values", value);
    }

    private Field sortedSetDocValueField(BytesRef value) {
        return new SortedSetDocValuesField("sorted_set_doc_values", value);
    }

    private Field sortedNumericDocValuesField(Long value) {
        return new SortedNumericDocValuesField("sorted_num_doc_values", value);
    }

    @Test
    public void testFieldValueRetrieval() throws Exception {
        Directory dir = newDirectory();

        IndexWriterConfig iwc = newIndexWriterConfig(random(), null);
        iwc.setMergePolicy(newLogMergePolicy());
        RandomIndexWriter iw = new RandomIndexWriter(random(), dir, iwc);
        int nDocs = atLeast(100);

        Set<String> fieldNames = new HashSet<>();
        fieldNames.add("id");
        fieldNames.add("string_field");
        fieldNames.add("binary_doc_values");
        fieldNames.add("numeric_doc_values");
        fieldNames.add("sorted_doc_values");
        fieldNames.add("sorted_num_doc_values");
        fieldNames.add("sorted_set_doc_values");

        Map<String, Map<String, String>> docFieldMap = new HashMap<>();

        int[] docIds = new int[nDocs];
        for (int i = 0; i < nDocs; i++) {
            Document doc = new Document();
            String id = String.valueOf(i);
            String stringValue = TestUtil.randomUnicodeString(random(), 10);
            BytesRef binaryDVValue = new BytesRef(TestUtil.randomUnicodeString(random(), 10));
            BytesRef sortedDVValue = new BytesRef(TestUtil.randomUnicodeString(random(), 10));
            BytesRef sortedSetDVValue= new BytesRef(TestUtil.randomUnicodeString(random(), 10));
            long numericDVValue = TestUtil.nextLong(random(), 1, 1000);
            long sortedNumericDVValue = TestUtil.nextLong(random(), 1, 1000);

            Map<String, String> fieldValueMap = new HashMap<>();
            fieldValueMap.put("string_field", stringValue);
            fieldValueMap.put("binary_doc_values", binaryDVValue.utf8ToString());
            fieldValueMap.put("sorted_doc_values", sortedDVValue.utf8ToString());
            fieldValueMap.put("sorted_set_doc_values", sortedSetDVValue.utf8ToString());
            fieldValueMap.put("numeric_doc_values", String.valueOf(numericDVValue));
            fieldValueMap.put("sorted_num_doc_values", String.valueOf(sortedNumericDVValue));

            docFieldMap.put(id, fieldValueMap);

            doc.add(idField(id));
            doc.add(stringField(stringValue));
            doc.add(binaryDocValuesField(binaryDVValue));
            doc.add(sortedDocValueField(sortedDVValue));
            doc.add(sortedSetDocValueField(sortedSetDVValue));
            doc.add(numericDocValuesField(numericDVValue));
            doc.add(sortedNumericDocValuesField(sortedNumericDVValue));
            iw.addDocument(doc);
            docIds[i] = i;

            if (random().nextInt(5) == 1) {
                iw.commit();
            }
        }

        iw.forceMerge(1);

        SegmentReader reader = getOnlySegmentReader(iw.getReader());

        DefaultCollector<Long> defaultCollector = new DefaultCollector<>(reader, fieldNames);

        defaultCollector.collect("", collect(docIds));

        List<DefaultCollector.Result<Long>> results = defaultCollector.get();

        for (DefaultCollector.Result.InnerResult<Long> longInnerResult : results.get(0).innerResults()) {
            String id = null;
            for (DefaultCollector.FieldValue fieldValue : longInnerResult.fieldValues) {
                if (fieldValue.name().equals("id")) {
                    id = fieldValue.stringValues().get(0);
                    break;
                }
            }
            assertNotNull(id);
            Map<String, String> fieldValueMap = docFieldMap.get(id);
            assertNotNull(fieldValueMap);
            for (DefaultCollector.FieldValue fieldValue : longInnerResult.fieldValues) {
                if (fieldValue.name().equals("id")) {
                    continue;
                }
                String value = fieldValueMap.get(fieldValue.name());
                assertNotNull(value);
                if (fieldValue.isBinary()) {
                    assertThat(fieldValue.binaryValues().get(0).utf8ToString(), equalTo(value));
                } else if (fieldValue.isNumeric()) {
                    assertThat(String.valueOf(fieldValue.numericValues().get(0)), equalTo(value));
                } else if (fieldValue.isString()) {
                    assertThat(fieldValue.stringValues().get(0), equalTo(value));
                } else {
                    fail("fieldValue is not of binary/numeric/string type");
                }
            }
        }

        iw.close();
        reader.close();
        dir.close();
    }

    private static List<SegmentLookup.Collector.ResultMetaData<Long>> collect(int... docIDs) {
        List<SegmentLookup.Collector.ResultMetaData<Long>> res = new ArrayList<>();
        for (int docID : docIDs) {
            res.add(new SegmentLookup.Collector.ResultMetaData<>("", 1l, docID));
        }
        return res;
    }
}
