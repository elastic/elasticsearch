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

import org.apache.lucene.search.*;
import org.elasticsearch.index.fielddata.*;
import org.elasticsearch.index.fielddata.util.*;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@Test
public abstract class NumericFieldDataTests extends StringFieldDataTests {

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

        LongArrayRef longArrayRef = longValues.getValues(0);
        assertThat(longArrayRef.size(), equalTo(1));
        assertThat(longArrayRef.values[longArrayRef.start], equalTo(2l));

        longArrayRef = longValues.getValues(1);
        assertThat(longArrayRef.size(), equalTo(1));
        assertThat(longArrayRef.values[longArrayRef.start], equalTo(1l));

        longArrayRef = longValues.getValues(2);
        assertThat(longArrayRef.size(), equalTo(1));
        assertThat(longArrayRef.values[longArrayRef.start], equalTo(3l));

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

        longValues.forEachValueInDoc(0, new LongValuesVerifierProc(0).addExpected(2l));
        longValues.forEachValueInDoc(1, new LongValuesVerifierProc(1).addExpected(1l));
        longValues.forEachValueInDoc(2, new LongValuesVerifierProc(2).addExpected(3l));

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

        DoubleArrayRef doubleArrayRef = doubleValues.getValues(0);
        assertThat(doubleArrayRef.size(), equalTo(1));
        assertThat(doubleArrayRef.values[doubleArrayRef.start], equalTo(2d));

        doubleArrayRef = doubleValues.getValues(1);
        assertThat(doubleArrayRef.size(), equalTo(1));
        assertThat(doubleArrayRef.values[doubleArrayRef.start], equalTo(1d));

        doubleArrayRef = doubleValues.getValues(2);
        assertThat(doubleArrayRef.size(), equalTo(1));
        assertThat(doubleArrayRef.values[doubleArrayRef.start], equalTo(3d));

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

        doubleValues.forEachValueInDoc(0, new DoubleValuesVerifierProc(0).addExpected(2d));
        doubleValues.forEachValueInDoc(1, new DoubleValuesVerifierProc(1).addExpected(1d));
        doubleValues.forEachValueInDoc(2, new DoubleValuesVerifierProc(2).addExpected(3d));

        ByteValues byteValues = fieldData.getByteValues();

        assertThat(byteValues.isMultiValued(), equalTo(false));

        assertThat(byteValues.hasValue(0), equalTo(true));
        assertThat(byteValues.hasValue(1), equalTo(true));
        assertThat(byteValues.hasValue(2), equalTo(true));

        assertThat(byteValues.getValue(0), equalTo((byte) 2));
        assertThat(byteValues.getValue(1), equalTo((byte) 1));
        assertThat(byteValues.getValue(2), equalTo((byte) 3));

        assertThat(byteValues.getValueMissing(0, (byte) -1), equalTo((byte) 2));
        assertThat(byteValues.getValueMissing(1, (byte) -1), equalTo((byte) 1));
        assertThat(byteValues.getValueMissing(2, (byte) -1), equalTo((byte) 3));

        ByteArrayRef byteArrayRef = byteValues.getValues(0);
        assertThat(byteArrayRef.size(), equalTo(1));
        assertThat(byteArrayRef.values[byteArrayRef.start], equalTo((byte) 2));

        byteArrayRef = byteValues.getValues(1);
        assertThat(byteArrayRef.size(), equalTo(1));
        assertThat(byteArrayRef.values[byteArrayRef.start], equalTo((byte) 1));

        byteArrayRef = byteValues.getValues(2);
        assertThat(byteArrayRef.size(), equalTo(1));
        assertThat(byteArrayRef.values[byteArrayRef.start], equalTo((byte) 3));

        ByteValues.Iter byteValuesIter = byteValues.getIter(0);
        assertThat(byteValuesIter.hasNext(), equalTo(true));
        assertThat(byteValuesIter.next(), equalTo((byte) 2));
        assertThat(byteValuesIter.hasNext(), equalTo(false));

        byteValuesIter = byteValues.getIter(1);
        assertThat(byteValuesIter.hasNext(), equalTo(true));
        assertThat(byteValuesIter.next(), equalTo((byte) 1));
        assertThat(byteValuesIter.hasNext(), equalTo(false));

        byteValuesIter = byteValues.getIter(2);
        assertThat(byteValuesIter.hasNext(), equalTo(true));
        assertThat(byteValuesIter.next(), equalTo((byte) 3));
        assertThat(byteValuesIter.hasNext(), equalTo(false));

        byteValues.forEachValueInDoc(0, new ByteValuesVerifierProc(0).addExpected((byte) 2));
        byteValues.forEachValueInDoc(1, new ByteValuesVerifierProc(1).addExpected((byte) 1));
        byteValues.forEachValueInDoc(2, new ByteValuesVerifierProc(2).addExpected((byte) 3));

        ShortValues shortValues = fieldData.getShortValues();

        assertThat(shortValues.isMultiValued(), equalTo(false));

        assertThat(shortValues.hasValue(0), equalTo(true));
        assertThat(shortValues.hasValue(1), equalTo(true));
        assertThat(shortValues.hasValue(2), equalTo(true));

        assertThat(shortValues.getValue(0), equalTo((short) 2));
        assertThat(shortValues.getValue(1), equalTo((short) 1));
        assertThat(shortValues.getValue(2), equalTo((short) 3));

        assertThat(shortValues.getValueMissing(0, (short) -1), equalTo((short) 2));
        assertThat(shortValues.getValueMissing(1, (short) -1), equalTo((short) 1));
        assertThat(shortValues.getValueMissing(2, (short) -1), equalTo((short) 3));

        ShortArrayRef shortArrayRef = shortValues.getValues(0);
        assertThat(shortArrayRef.size(), equalTo(1));
        assertThat(shortArrayRef.values[shortArrayRef.start], equalTo((short) 2));

        shortArrayRef = shortValues.getValues(1);
        assertThat(shortArrayRef.size(), equalTo(1));
        assertThat(shortArrayRef.values[shortArrayRef.start], equalTo((short) 1));

        shortArrayRef = shortValues.getValues(2);
        assertThat(shortArrayRef.size(), equalTo(1));
        assertThat(shortArrayRef.values[shortArrayRef.start], equalTo((short) 3));

        ShortValues.Iter shortValuesIter = shortValues.getIter(0);
        assertThat(shortValuesIter.hasNext(), equalTo(true));
        assertThat(shortValuesIter.next(), equalTo((short) 2));
        assertThat(shortValuesIter.hasNext(), equalTo(false));

        shortValuesIter = shortValues.getIter(1);
        assertThat(shortValuesIter.hasNext(), equalTo(true));
        assertThat(shortValuesIter.next(), equalTo((short) 1));
        assertThat(shortValuesIter.hasNext(), equalTo(false));

        shortValuesIter = shortValues.getIter(2);
        assertThat(shortValuesIter.hasNext(), equalTo(true));
        assertThat(shortValuesIter.next(), equalTo((short) 3));
        assertThat(shortValuesIter.hasNext(), equalTo(false));

        shortValues.forEachValueInDoc(0, new ShortValuesVerifierProc(0).addExpected((short) 2));
        shortValues.forEachValueInDoc(1, new ShortValuesVerifierProc(1).addExpected((short) 1));
        shortValues.forEachValueInDoc(2, new ShortValuesVerifierProc(2).addExpected((short) 3));

        IntValues intValues = fieldData.getIntValues();

        assertThat(intValues.isMultiValued(), equalTo(false));

        assertThat(intValues.hasValue(0), equalTo(true));
        assertThat(intValues.hasValue(1), equalTo(true));
        assertThat(intValues.hasValue(2), equalTo(true));

        assertThat(intValues.getValue(0), equalTo(2));
        assertThat(intValues.getValue(1), equalTo(1));
        assertThat(intValues.getValue(2), equalTo(3));

        assertThat(intValues.getValueMissing(0, -1), equalTo(2));
        assertThat(intValues.getValueMissing(1, -1), equalTo(1));
        assertThat(intValues.getValueMissing(2, -1), equalTo(3));

        IntArrayRef intArrayRef = intValues.getValues(0);
        assertThat(intArrayRef.size(), equalTo(1));
        assertThat(intArrayRef.values[intArrayRef.start], equalTo(2));

        intArrayRef = intValues.getValues(1);
        assertThat(intArrayRef.size(), equalTo(1));
        assertThat(intArrayRef.values[intArrayRef.start], equalTo(1));

        intArrayRef = intValues.getValues(2);
        assertThat(intArrayRef.size(), equalTo(1));
        assertThat(intArrayRef.values[intArrayRef.start], equalTo(3));

        IntValues.Iter intValuesIter = intValues.getIter(0);
        assertThat(intValuesIter.hasNext(), equalTo(true));
        assertThat(intValuesIter.next(), equalTo(2));
        assertThat(intValuesIter.hasNext(), equalTo(false));

        intValuesIter = intValues.getIter(1);
        assertThat(intValuesIter.hasNext(), equalTo(true));
        assertThat(intValuesIter.next(), equalTo(1));
        assertThat(intValuesIter.hasNext(), equalTo(false));

        intValuesIter = intValues.getIter(2);
        assertThat(intValuesIter.hasNext(), equalTo(true));
        assertThat(intValuesIter.next(), equalTo(3));
        assertThat(intValuesIter.hasNext(), equalTo(false));

        intValues.forEachValueInDoc(0, new IntValuesVerifierProc(0).addExpected(2));
        intValues.forEachValueInDoc(1, new IntValuesVerifierProc(1).addExpected(1));
        intValues.forEachValueInDoc(2, new IntValuesVerifierProc(2).addExpected(3));

        FloatValues floatValues = fieldData.getFloatValues();

        assertThat(floatValues.isMultiValued(), equalTo(false));

        assertThat(floatValues.hasValue(0), equalTo(true));
        assertThat(floatValues.hasValue(1), equalTo(true));
        assertThat(floatValues.hasValue(2), equalTo(true));

        assertThat(floatValues.getValue(0), equalTo(2f));
        assertThat(floatValues.getValue(1), equalTo(1f));
        assertThat(floatValues.getValue(2), equalTo(3f));

        assertThat(floatValues.getValueMissing(0, -1), equalTo(2f));
        assertThat(floatValues.getValueMissing(1, -1), equalTo(1f));
        assertThat(floatValues.getValueMissing(2, -1), equalTo(3f));

        FloatArrayRef floatArrayRef = floatValues.getValues(0);
        assertThat(floatArrayRef.size(), equalTo(1));
        assertThat(floatArrayRef.values[floatArrayRef.start], equalTo(2f));

        floatArrayRef = floatValues.getValues(1);
        assertThat(floatArrayRef.size(), equalTo(1));
        assertThat(floatArrayRef.values[floatArrayRef.start], equalTo(1f));

        floatArrayRef = floatValues.getValues(2);
        assertThat(floatArrayRef.size(), equalTo(1));
        assertThat(floatArrayRef.values[floatArrayRef.start], equalTo(3f));

        FloatValues.Iter floatValuesIter = floatValues.getIter(0);
        assertThat(floatValuesIter.hasNext(), equalTo(true));
        assertThat(floatValuesIter.next(), equalTo(2f));
        assertThat(floatValuesIter.hasNext(), equalTo(false));

        floatValuesIter = floatValues.getIter(1);
        assertThat(floatValuesIter.hasNext(), equalTo(true));
        assertThat(floatValuesIter.next(), equalTo(1f));
        assertThat(floatValuesIter.hasNext(), equalTo(false));

        floatValuesIter = floatValues.getIter(2);
        assertThat(floatValuesIter.hasNext(), equalTo(true));
        assertThat(floatValuesIter.next(), equalTo(3f));
        assertThat(floatValuesIter.hasNext(), equalTo(false));

        floatValues.forEachValueInDoc(0, new FloatValuesVerifierProc(0).addExpected(2f));
        floatValues.forEachValueInDoc(1, new FloatValuesVerifierProc(1).addExpected(1f));
        floatValues.forEachValueInDoc(2, new FloatValuesVerifierProc(2).addExpected(3f));

        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopFieldDocs topDocs;

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null))));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null), true)));
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

        LongArrayRef longArrayRef = longValues.getValues(0);
        assertThat(longArrayRef.size(), equalTo(1));
        assertThat(longArrayRef.values[longArrayRef.start], equalTo(2l));

        longArrayRef = longValues.getValues(1);
        assertThat(longArrayRef.size(), equalTo(0));

        longArrayRef = longValues.getValues(2);
        assertThat(longArrayRef.size(), equalTo(1));
        assertThat(longArrayRef.values[longArrayRef.start], equalTo(3l));

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

        longValues.forEachValueInDoc(0, new LongValuesVerifierProc(0).addExpected(2l));
        longValues.forEachValueInDoc(1, new LongValuesVerifierProc(1).addMissing());
        longValues.forEachValueInDoc(2, new LongValuesVerifierProc(2).addExpected(3l));

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

        DoubleArrayRef doubleArrayRef = doubleValues.getValues(0);
        assertThat(doubleArrayRef.size(), equalTo(1));
        assertThat(doubleArrayRef.values[doubleArrayRef.start], equalTo(2d));

        doubleArrayRef = doubleValues.getValues(1);
        assertThat(doubleArrayRef.size(), equalTo(0));

        doubleArrayRef = doubleValues.getValues(2);
        assertThat(doubleArrayRef.size(), equalTo(1));
        assertThat(doubleArrayRef.values[doubleArrayRef.start], equalTo(3d));

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

        doubleValues.forEachValueInDoc(0, new DoubleValuesVerifierProc(0).addExpected(2d));
        doubleValues.forEachValueInDoc(1, new DoubleValuesVerifierProc(1).addMissing());
        doubleValues.forEachValueInDoc(2, new DoubleValuesVerifierProc(2).addExpected(3d));

        ByteValues byteValues = fieldData.getByteValues();

        assertThat(byteValues.isMultiValued(), equalTo(false));

        assertThat(byteValues.hasValue(0), equalTo(true));
        assertThat(byteValues.hasValue(1), equalTo(false));
        assertThat(byteValues.hasValue(2), equalTo(true));

        assertThat(byteValues.getValue(0), equalTo((byte) 2));
        assertThat(byteValues.getValue(2), equalTo((byte) 3));

        assertThat(byteValues.getValueMissing(0, (byte) -1), equalTo((byte) 2));
        assertThat(byteValues.getValueMissing(1, (byte) -1), equalTo((byte) -1));
        assertThat(byteValues.getValueMissing(2, (byte) -1), equalTo((byte) 3));

        ByteArrayRef byteArrayRef = byteValues.getValues(0);
        assertThat(byteArrayRef.size(), equalTo(1));
        assertThat(byteArrayRef.values[byteArrayRef.start], equalTo((byte) 2));

        byteArrayRef = byteValues.getValues(1);
        assertThat(byteArrayRef.size(), equalTo(0));

        byteArrayRef = byteValues.getValues(2);
        assertThat(byteArrayRef.size(), equalTo(1));
        assertThat(byteArrayRef.values[byteArrayRef.start], equalTo((byte) 3));

        ByteValues.Iter byteValuesIter = byteValues.getIter(0);
        assertThat(byteValuesIter.hasNext(), equalTo(true));
        assertThat(byteValuesIter.next(), equalTo((byte) 2));
        assertThat(byteValuesIter.hasNext(), equalTo(false));

        byteValuesIter = byteValues.getIter(1);
        assertThat(byteValuesIter.hasNext(), equalTo(false));

        byteValuesIter = byteValues.getIter(2);
        assertThat(byteValuesIter.hasNext(), equalTo(true));
        assertThat(byteValuesIter.next(), equalTo((byte) 3));
        assertThat(byteValuesIter.hasNext(), equalTo(false));

        byteValues.forEachValueInDoc(0, new ByteValuesVerifierProc(0).addExpected((byte) 2));
        byteValues.forEachValueInDoc(1, new ByteValuesVerifierProc(1).addMissing());
        byteValues.forEachValueInDoc(2, new ByteValuesVerifierProc(2).addExpected((byte) 3));

        ShortValues shortValues = fieldData.getShortValues();

        assertThat(shortValues.isMultiValued(), equalTo(false));

        assertThat(shortValues.hasValue(0), equalTo(true));
        assertThat(shortValues.hasValue(1), equalTo(false));
        assertThat(shortValues.hasValue(2), equalTo(true));

        assertThat(shortValues.getValue(0), equalTo((short) 2));
        assertThat(shortValues.getValue(2), equalTo((short) 3));

        assertThat(shortValues.getValueMissing(0, (short) -1), equalTo((short) 2));
        assertThat(shortValues.getValueMissing(1, (short) -1), equalTo((short) -1));
        assertThat(shortValues.getValueMissing(2, (short) -1), equalTo((short) 3));

        ShortArrayRef shortArrayRef = shortValues.getValues(0);
        assertThat(shortArrayRef.size(), equalTo(1));
        assertThat(shortArrayRef.values[shortArrayRef.start], equalTo((short) 2));

        shortArrayRef = shortValues.getValues(1);
        assertThat(shortArrayRef.size(), equalTo(0));

        shortArrayRef = shortValues.getValues(2);
        assertThat(shortArrayRef.size(), equalTo(1));
        assertThat(shortArrayRef.values[shortArrayRef.start], equalTo((short) 3));

        ShortValues.Iter shortValuesIter = shortValues.getIter(0);
        assertThat(shortValuesIter.hasNext(), equalTo(true));
        assertThat(shortValuesIter.next(), equalTo((short) 2));
        assertThat(shortValuesIter.hasNext(), equalTo(false));

        shortValuesIter = shortValues.getIter(1);
        assertThat(shortValuesIter.hasNext(), equalTo(false));

        shortValuesIter = shortValues.getIter(2);
        assertThat(shortValuesIter.hasNext(), equalTo(true));
        assertThat(shortValuesIter.next(), equalTo((short) 3));
        assertThat(shortValuesIter.hasNext(), equalTo(false));

        shortValues.forEachValueInDoc(0, new ShortValuesVerifierProc(0).addExpected((short) 2));
        shortValues.forEachValueInDoc(1, new ShortValuesVerifierProc(1).addMissing());
        shortValues.forEachValueInDoc(2, new ShortValuesVerifierProc(2).addExpected((short) 3));

        IntValues intValues = fieldData.getIntValues();

        assertThat(intValues.isMultiValued(), equalTo(false));

        assertThat(intValues.hasValue(0), equalTo(true));
        assertThat(intValues.hasValue(1), equalTo(false));
        assertThat(intValues.hasValue(2), equalTo(true));

        assertThat(intValues.getValue(0), equalTo(2));
        assertThat(intValues.getValue(2), equalTo(3));

        assertThat(intValues.getValueMissing(0, -1), equalTo(2));
        assertThat(intValues.getValueMissing(1, -1), equalTo(-1));
        assertThat(intValues.getValueMissing(2, -1), equalTo(3));

        IntArrayRef intArrayRef = intValues.getValues(0);
        assertThat(intArrayRef.size(), equalTo(1));
        assertThat(intArrayRef.values[intArrayRef.start], equalTo(2));

        intArrayRef = intValues.getValues(1);
        assertThat(intArrayRef.size(), equalTo(0));

        intArrayRef = intValues.getValues(2);
        assertThat(intArrayRef.size(), equalTo(1));
        assertThat(intArrayRef.values[intArrayRef.start], equalTo(3));

        IntValues.Iter intValuesIter = intValues.getIter(0);
        assertThat(intValuesIter.hasNext(), equalTo(true));
        assertThat(intValuesIter.next(), equalTo(2));
        assertThat(intValuesIter.hasNext(), equalTo(false));

        intValuesIter = intValues.getIter(1);
        assertThat(intValuesIter.hasNext(), equalTo(false));

        intValuesIter = intValues.getIter(2);
        assertThat(intValuesIter.hasNext(), equalTo(true));
        assertThat(intValuesIter.next(), equalTo(3));
        assertThat(intValuesIter.hasNext(), equalTo(false));

        intValues.forEachValueInDoc(0, new IntValuesVerifierProc(0).addExpected(2));
        intValues.forEachValueInDoc(1, new IntValuesVerifierProc(1).addMissing());
        intValues.forEachValueInDoc(2, new IntValuesVerifierProc(2).addExpected(3));

        FloatValues floatValues = fieldData.getFloatValues();

        assertThat(floatValues.isMultiValued(), equalTo(false));

        assertThat(floatValues.hasValue(0), equalTo(true));
        assertThat(floatValues.hasValue(1), equalTo(false));
        assertThat(floatValues.hasValue(2), equalTo(true));

        assertThat(floatValues.getValue(0), equalTo(2f));
        assertThat(floatValues.getValue(2), equalTo(3f));

        assertThat(floatValues.getValueMissing(0, -1), equalTo(2f));
        assertThat(floatValues.getValueMissing(1, -1), equalTo(-1f));
        assertThat(floatValues.getValueMissing(2, -1), equalTo(3f));

        FloatArrayRef floatArrayRef = floatValues.getValues(0);
        assertThat(floatArrayRef.size(), equalTo(1));
        assertThat(floatArrayRef.values[floatArrayRef.start], equalTo(2f));

        floatArrayRef = floatValues.getValues(1);
        assertThat(floatArrayRef.size(), equalTo(0));

        floatArrayRef = floatValues.getValues(2);
        assertThat(floatArrayRef.size(), equalTo(1));
        assertThat(floatArrayRef.values[floatArrayRef.start], equalTo(3f));

        FloatValues.Iter floatValuesIter = floatValues.getIter(0);
        assertThat(floatValuesIter.hasNext(), equalTo(true));
        assertThat(floatValuesIter.next(), equalTo(2f));
        assertThat(floatValuesIter.hasNext(), equalTo(false));

        floatValuesIter = floatValues.getIter(1);
        assertThat(floatValuesIter.hasNext(), equalTo(false));

        floatValuesIter = floatValues.getIter(2);
        assertThat(floatValuesIter.hasNext(), equalTo(true));
        assertThat(floatValuesIter.next(), equalTo(3f));
        assertThat(floatValuesIter.hasNext(), equalTo(false));

        floatValues.forEachValueInDoc(0, new FloatValuesVerifierProc(0).addExpected(2f));
        floatValues.forEachValueInDoc(1, new FloatValuesVerifierProc(1).addMissing());
        floatValues.forEachValueInDoc(2, new FloatValuesVerifierProc(2).addExpected(3f));

        IndexSearcher searcher = new IndexSearcher(readerContext.reader());
        TopFieldDocs topDocs;

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null)))); // defaults to _last
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(1));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource(null), true))); // defaults to _last
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(1));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource("_first"))));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource("_first"), true)));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(2));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(0));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource("1"))));
        assertThat(topDocs.totalHits, equalTo(3));
        assertThat(topDocs.scoreDocs[0].doc, equalTo(1));
        assertThat(topDocs.scoreDocs[1].doc, equalTo(0));
        assertThat(topDocs.scoreDocs[2].doc, equalTo(2));

        topDocs = searcher.search(new MatchAllDocsQuery(), 10,
                new Sort(new SortField("value", indexFieldData.comparatorSource("1"), true)));
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

        LongArrayRef longArrayRef = longValues.getValues(0);
        assertThat(longArrayRef.size(), equalTo(2));
        assertThat(longArrayRef.values[longArrayRef.start], equalTo(2l));
        assertThat(longArrayRef.values[longArrayRef.start + 1], equalTo(4l));

        longArrayRef = longValues.getValues(1);
        assertThat(longArrayRef.size(), equalTo(1));
        assertThat(longArrayRef.values[longArrayRef.start], equalTo(1l));

        longArrayRef = longValues.getValues(2);
        assertThat(longArrayRef.size(), equalTo(1));
        assertThat(longArrayRef.values[longArrayRef.start], equalTo(3l));

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

        longValues.forEachValueInDoc(0, new LongValuesVerifierProc(0).addExpected(2l).addExpected(4l));
        longValues.forEachValueInDoc(1, new LongValuesVerifierProc(1).addExpected(1l));
        longValues.forEachValueInDoc(2, new LongValuesVerifierProc(2).addExpected(3l));

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

        DoubleArrayRef doubleArrayRef = doubleValues.getValues(0);
        assertThat(doubleArrayRef.size(), equalTo(2));
        assertThat(doubleArrayRef.values[doubleArrayRef.start], equalTo(2d));
        assertThat(doubleArrayRef.values[doubleArrayRef.start + 1], equalTo(4d));

        doubleArrayRef = doubleValues.getValues(1);
        assertThat(doubleArrayRef.size(), equalTo(1));
        assertThat(doubleArrayRef.values[doubleArrayRef.start], equalTo(1d));

        doubleArrayRef = doubleValues.getValues(2);
        assertThat(doubleArrayRef.size(), equalTo(1));
        assertThat(doubleArrayRef.values[doubleArrayRef.start], equalTo(3d));

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

        doubleValues.forEachValueInDoc(0, new DoubleValuesVerifierProc(0).addExpected(2d).addExpected(4d));
        doubleValues.forEachValueInDoc(1, new DoubleValuesVerifierProc(1).addExpected(1d));
        doubleValues.forEachValueInDoc(2, new DoubleValuesVerifierProc(2).addExpected(3d));

        ByteValues byteValues = fieldData.getByteValues();

        assertThat(byteValues.isMultiValued(), equalTo(true));

        assertThat(byteValues.hasValue(0), equalTo(true));
        assertThat(byteValues.hasValue(1), equalTo(true));
        assertThat(byteValues.hasValue(2), equalTo(true));

        assertThat(byteValues.getValue(0), equalTo((byte) 2));
        assertThat(byteValues.getValue(1), equalTo((byte) 1));
        assertThat(byteValues.getValue(2), equalTo((byte) 3));

        assertThat(byteValues.getValueMissing(0, (byte) -1), equalTo((byte) 2));
        assertThat(byteValues.getValueMissing(1, (byte) -1), equalTo((byte) 1));
        assertThat(byteValues.getValueMissing(2, (byte) -1), equalTo((byte) 3));

        ByteArrayRef byteArrayRef = byteValues.getValues(0);
        assertThat(byteArrayRef.size(), equalTo(2));
        assertThat(byteArrayRef.values[byteArrayRef.start], equalTo((byte) 2));
        assertThat(byteArrayRef.values[byteArrayRef.start + 1], equalTo((byte) 4));

        byteArrayRef = byteValues.getValues(1);
        assertThat(byteArrayRef.size(), equalTo(1));
        assertThat(byteArrayRef.values[byteArrayRef.start], equalTo((byte) 1));

        byteArrayRef = byteValues.getValues(2);
        assertThat(byteArrayRef.size(), equalTo(1));
        assertThat(byteArrayRef.values[byteArrayRef.start], equalTo((byte) 3));

        ByteValues.Iter byteValuesIter = byteValues.getIter(0);
        assertThat(byteValuesIter.hasNext(), equalTo(true));
        assertThat(byteValuesIter.next(), equalTo((byte) 2));
        assertThat(byteValuesIter.hasNext(), equalTo(true));
        assertThat(byteValuesIter.next(), equalTo((byte) 4));
        assertThat(byteValuesIter.hasNext(), equalTo(false));

        byteValuesIter = byteValues.getIter(1);
        assertThat(byteValuesIter.hasNext(), equalTo(true));
        assertThat(byteValuesIter.next(), equalTo((byte) 1));
        assertThat(byteValuesIter.hasNext(), equalTo(false));

        byteValuesIter = byteValues.getIter(2);
        assertThat(byteValuesIter.hasNext(), equalTo(true));
        assertThat(byteValuesIter.next(), equalTo((byte) 3));
        assertThat(byteValuesIter.hasNext(), equalTo(false));

        byteValues.forEachValueInDoc(0, new ByteValuesVerifierProc(0).addExpected((byte) 2).addExpected((byte) 4));
        byteValues.forEachValueInDoc(1, new ByteValuesVerifierProc(1).addExpected((byte) 1));
        byteValues.forEachValueInDoc(2, new ByteValuesVerifierProc(2).addExpected((byte) 3));

        ShortValues shortValues = fieldData.getShortValues();

        assertThat(shortValues.isMultiValued(), equalTo(true));

        assertThat(shortValues.hasValue(0), equalTo(true));
        assertThat(shortValues.hasValue(1), equalTo(true));
        assertThat(shortValues.hasValue(2), equalTo(true));

        assertThat(shortValues.getValue(0), equalTo((short) 2));
        assertThat(shortValues.getValue(1), equalTo((short) 1));
        assertThat(shortValues.getValue(2), equalTo((short) 3));

        assertThat(shortValues.getValueMissing(0, (short) -1), equalTo((short) 2));
        assertThat(shortValues.getValueMissing(1, (short) -1), equalTo((short) 1));
        assertThat(shortValues.getValueMissing(2, (short) -1), equalTo((short) 3));

        ShortArrayRef shortArrayRef = shortValues.getValues(0);
        assertThat(shortArrayRef.size(), equalTo(2));
        assertThat(shortArrayRef.values[shortArrayRef.start], equalTo((short) 2));
        assertThat(shortArrayRef.values[shortArrayRef.start + 1], equalTo((short) 4));

        shortArrayRef = shortValues.getValues(1);
        assertThat(shortArrayRef.size(), equalTo(1));
        assertThat(shortArrayRef.values[shortArrayRef.start], equalTo((short) 1));

        shortArrayRef = shortValues.getValues(2);
        assertThat(shortArrayRef.size(), equalTo(1));
        assertThat(shortArrayRef.values[shortArrayRef.start], equalTo((short) 3));

        ShortValues.Iter shortValuesIter = shortValues.getIter(0);
        assertThat(shortValuesIter.hasNext(), equalTo(true));
        assertThat(shortValuesIter.next(), equalTo((short) 2));
        assertThat(shortValuesIter.hasNext(), equalTo(true));
        assertThat(shortValuesIter.next(), equalTo((short) 4));
        assertThat(shortValuesIter.hasNext(), equalTo(false));

        shortValuesIter = shortValues.getIter(1);
        assertThat(shortValuesIter.hasNext(), equalTo(true));
        assertThat(shortValuesIter.next(), equalTo((short) 1));
        assertThat(shortValuesIter.hasNext(), equalTo(false));

        shortValuesIter = shortValues.getIter(2);
        assertThat(shortValuesIter.hasNext(), equalTo(true));
        assertThat(shortValuesIter.next(), equalTo((short) 3));
        assertThat(shortValuesIter.hasNext(), equalTo(false));

        shortValues.forEachValueInDoc(0, new ShortValuesVerifierProc(0).addExpected((short) 2).addExpected((short) 4));
        shortValues.forEachValueInDoc(1, new ShortValuesVerifierProc(1).addExpected((short) 1));
        shortValues.forEachValueInDoc(2, new ShortValuesVerifierProc(2).addExpected((short) 3));

        IntValues intValues = fieldData.getIntValues();

        assertThat(intValues.isMultiValued(), equalTo(true));

        assertThat(intValues.hasValue(0), equalTo(true));
        assertThat(intValues.hasValue(1), equalTo(true));
        assertThat(intValues.hasValue(2), equalTo(true));

        assertThat(intValues.getValue(0), equalTo(2));
        assertThat(intValues.getValue(1), equalTo(1));
        assertThat(intValues.getValue(2), equalTo(3));

        assertThat(intValues.getValueMissing(0, -1), equalTo(2));
        assertThat(intValues.getValueMissing(1, -1), equalTo(1));
        assertThat(intValues.getValueMissing(2, -1), equalTo(3));

        IntArrayRef intArrayRef = intValues.getValues(0);
        assertThat(intArrayRef.size(), equalTo(2));
        assertThat(intArrayRef.values[intArrayRef.start], equalTo(2));
        assertThat(intArrayRef.values[intArrayRef.start + 1], equalTo(4));

        intArrayRef = intValues.getValues(1);
        assertThat(intArrayRef.size(), equalTo(1));
        assertThat(intArrayRef.values[intArrayRef.start], equalTo(1));

        intArrayRef = intValues.getValues(2);
        assertThat(intArrayRef.size(), equalTo(1));
        assertThat(intArrayRef.values[intArrayRef.start], equalTo(3));

        IntValues.Iter intValuesIter = intValues.getIter(0);
        assertThat(intValuesIter.hasNext(), equalTo(true));
        assertThat(intValuesIter.next(), equalTo(2));
        assertThat(intValuesIter.hasNext(), equalTo(true));
        assertThat(intValuesIter.next(), equalTo(4));
        assertThat(intValuesIter.hasNext(), equalTo(false));

        intValuesIter = intValues.getIter(1);
        assertThat(intValuesIter.hasNext(), equalTo(true));
        assertThat(intValuesIter.next(), equalTo(1));
        assertThat(intValuesIter.hasNext(), equalTo(false));

        intValuesIter = intValues.getIter(2);
        assertThat(intValuesIter.hasNext(), equalTo(true));
        assertThat(intValuesIter.next(), equalTo(3));
        assertThat(intValuesIter.hasNext(), equalTo(false));

        intValues.forEachValueInDoc(0, new IntValuesVerifierProc(0).addExpected(2).addExpected(4));
        intValues.forEachValueInDoc(1, new IntValuesVerifierProc(1).addExpected(1));
        intValues.forEachValueInDoc(2, new IntValuesVerifierProc(2).addExpected(3));

        FloatValues floatValues = fieldData.getFloatValues();

        assertThat(floatValues.isMultiValued(), equalTo(true));

        assertThat(floatValues.hasValue(0), equalTo(true));
        assertThat(floatValues.hasValue(1), equalTo(true));
        assertThat(floatValues.hasValue(2), equalTo(true));

        assertThat(floatValues.getValue(0), equalTo(2f));
        assertThat(floatValues.getValue(1), equalTo(1f));
        assertThat(floatValues.getValue(2), equalTo(3f));

        assertThat(floatValues.getValueMissing(0, -1), equalTo(2f));
        assertThat(floatValues.getValueMissing(1, -1), equalTo(1f));
        assertThat(floatValues.getValueMissing(2, -1), equalTo(3f));

        FloatArrayRef floatArrayRef = floatValues.getValues(0);
        assertThat(floatArrayRef.size(), equalTo(2));
        assertThat(floatArrayRef.values[floatArrayRef.start], equalTo(2f));
        assertThat(floatArrayRef.values[floatArrayRef.start + 1], equalTo(4f));

        floatArrayRef = floatValues.getValues(1);
        assertThat(floatArrayRef.size(), equalTo(1));
        assertThat(floatArrayRef.values[floatArrayRef.start], equalTo(1f));

        floatArrayRef = floatValues.getValues(2);
        assertThat(floatArrayRef.size(), equalTo(1));
        assertThat(floatArrayRef.values[floatArrayRef.start], equalTo(3f));

        FloatValues.Iter floatValuesIter = floatValues.getIter(0);
        assertThat(floatValuesIter.hasNext(), equalTo(true));
        assertThat(floatValuesIter.next(), equalTo(2f));
        assertThat(floatValuesIter.hasNext(), equalTo(true));
        assertThat(floatValuesIter.next(), equalTo(4f));
        assertThat(floatValuesIter.hasNext(), equalTo(false));

        floatValuesIter = floatValues.getIter(1);
        assertThat(floatValuesIter.hasNext(), equalTo(true));
        assertThat(floatValuesIter.next(), equalTo(1f));
        assertThat(floatValuesIter.hasNext(), equalTo(false));

        floatValuesIter = floatValues.getIter(2);
        assertThat(floatValuesIter.hasNext(), equalTo(true));
        assertThat(floatValuesIter.next(), equalTo(3f));
        assertThat(floatValuesIter.hasNext(), equalTo(false));

        floatValues.forEachValueInDoc(0, new FloatValuesVerifierProc(0).addExpected(2f).addExpected(4f));
        floatValues.forEachValueInDoc(1, new FloatValuesVerifierProc(1).addExpected(1f));
        floatValues.forEachValueInDoc(2, new FloatValuesVerifierProc(2).addExpected(3f));
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

        LongArrayRef longArrayRef = longValues.getValues(0);
        assertThat(longArrayRef.size(), equalTo(2));
        assertThat(longArrayRef.values[longArrayRef.start], equalTo(2l));
        assertThat(longArrayRef.values[longArrayRef.start + 1], equalTo(4l));

        longArrayRef = longValues.getValues(1);
        assertThat(longArrayRef.size(), equalTo(0));

        longArrayRef = longValues.getValues(2);
        assertThat(longArrayRef.size(), equalTo(1));
        assertThat(longArrayRef.values[longArrayRef.start], equalTo(3l));

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

        longValues.forEachValueInDoc(0, new LongValuesVerifierProc(0).addExpected(2l).addExpected(4l));
        longValues.forEachValueInDoc(1, new LongValuesVerifierProc(1).addMissing());
        longValues.forEachValueInDoc(2, new LongValuesVerifierProc(2).addExpected(3l));

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

        DoubleArrayRef doubleArrayRef = doubleValues.getValues(0);
        assertThat(doubleArrayRef.size(), equalTo(2));
        assertThat(doubleArrayRef.values[doubleArrayRef.start], equalTo(2d));
        assertThat(doubleArrayRef.values[doubleArrayRef.start + 1], equalTo(4d));

        doubleArrayRef = doubleValues.getValues(1);
        assertThat(doubleArrayRef.size(), equalTo(0));

        doubleArrayRef = doubleValues.getValues(2);
        assertThat(doubleArrayRef.size(), equalTo(1));
        assertThat(doubleArrayRef.values[doubleArrayRef.start], equalTo(3d));

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

        doubleValues.forEachValueInDoc(0, new DoubleValuesVerifierProc(0).addExpected(2d).addExpected(4d));
        doubleValues.forEachValueInDoc(1, new DoubleValuesVerifierProc(1).addMissing());
        doubleValues.forEachValueInDoc(2, new DoubleValuesVerifierProc(2).addExpected(3d));

        ByteValues byteValues = fieldData.getByteValues();

        assertThat(byteValues.isMultiValued(), equalTo(true));

        assertThat(byteValues.hasValue(0), equalTo(true));
        assertThat(byteValues.hasValue(1), equalTo(false));
        assertThat(byteValues.hasValue(2), equalTo(true));

        assertThat(byteValues.getValue(0), equalTo((byte) 2));
        assertThat(byteValues.getValue(2), equalTo((byte) 3));

        assertThat(byteValues.getValueMissing(0, (byte) -1), equalTo((byte) 2));
        assertThat(byteValues.getValueMissing(1, (byte) -1), equalTo((byte) -1));
        assertThat(byteValues.getValueMissing(2, (byte) -1), equalTo((byte) 3));

        ByteArrayRef byteArrayRef = byteValues.getValues(0);
        assertThat(byteArrayRef.size(), equalTo(2));
        assertThat(byteArrayRef.values[byteArrayRef.start], equalTo((byte) 2));
        assertThat(byteArrayRef.values[byteArrayRef.start + 1], equalTo((byte) 4));

        byteArrayRef = byteValues.getValues(1);
        assertThat(byteArrayRef.size(), equalTo(0));

        byteArrayRef = byteValues.getValues(2);
        assertThat(byteArrayRef.size(), equalTo(1));
        assertThat(byteArrayRef.values[byteArrayRef.start], equalTo((byte) 3));

        ByteValues.Iter byteValuesIter = byteValues.getIter(0);
        assertThat(byteValuesIter.hasNext(), equalTo(true));
        assertThat(byteValuesIter.next(), equalTo((byte) 2));
        assertThat(byteValuesIter.hasNext(), equalTo(true));
        assertThat(byteValuesIter.next(), equalTo((byte) 4));
        assertThat(byteValuesIter.hasNext(), equalTo(false));

        byteValuesIter = byteValues.getIter(1);
        assertThat(byteValuesIter.hasNext(), equalTo(false));

        byteValuesIter = byteValues.getIter(2);
        assertThat(byteValuesIter.hasNext(), equalTo(true));
        assertThat(byteValuesIter.next(), equalTo((byte) 3));
        assertThat(byteValuesIter.hasNext(), equalTo(false));

        byteValues.forEachValueInDoc(0, new ByteValuesVerifierProc(0).addExpected((byte) 2).addExpected((byte) 4));
        byteValues.forEachValueInDoc(1, new ByteValuesVerifierProc(1).addMissing());
        byteValues.forEachValueInDoc(2, new ByteValuesVerifierProc(2).addExpected((byte) 3));

        ShortValues shortValues = fieldData.getShortValues();

        assertThat(shortValues.isMultiValued(), equalTo(true));

        assertThat(shortValues.hasValue(0), equalTo(true));
        assertThat(shortValues.hasValue(1), equalTo(false));
        assertThat(shortValues.hasValue(2), equalTo(true));

        assertThat(shortValues.getValue(0), equalTo((short) 2));
        assertThat(shortValues.getValue(2), equalTo((short) 3));

        assertThat(shortValues.getValueMissing(0, (short) -1), equalTo((short) 2));
        assertThat(shortValues.getValueMissing(1, (short) -1), equalTo((short) -1));
        assertThat(shortValues.getValueMissing(2, (short) -1), equalTo((short) 3));

        ShortArrayRef shortArrayRef = shortValues.getValues(0);
        assertThat(shortArrayRef.size(), equalTo(2));
        assertThat(shortArrayRef.values[shortArrayRef.start], equalTo((short) 2));
        assertThat(shortArrayRef.values[shortArrayRef.start + 1], equalTo((short) 4));

        shortArrayRef = shortValues.getValues(1);
        assertThat(shortArrayRef.size(), equalTo(0));

        shortArrayRef = shortValues.getValues(2);
        assertThat(shortArrayRef.size(), equalTo(1));
        assertThat(shortArrayRef.values[shortArrayRef.start], equalTo((short) 3));

        ShortValues.Iter shortValuesIter = shortValues.getIter(0);
        assertThat(shortValuesIter.hasNext(), equalTo(true));
        assertThat(shortValuesIter.next(), equalTo((short) 2));
        assertThat(shortValuesIter.hasNext(), equalTo(true));
        assertThat(shortValuesIter.next(), equalTo((short) 4));
        assertThat(shortValuesIter.hasNext(), equalTo(false));

        shortValuesIter = shortValues.getIter(1);
        assertThat(shortValuesIter.hasNext(), equalTo(false));

        shortValuesIter = shortValues.getIter(2);
        assertThat(shortValuesIter.hasNext(), equalTo(true));
        assertThat(shortValuesIter.next(), equalTo((short) 3));
        assertThat(shortValuesIter.hasNext(), equalTo(false));

        shortValues.forEachValueInDoc(0, new ShortValuesVerifierProc(0).addExpected((short) 2).addExpected((short) 4));
        shortValues.forEachValueInDoc(1, new ShortValuesVerifierProc(1).addMissing());
        shortValues.forEachValueInDoc(2, new ShortValuesVerifierProc(2).addExpected((short) 3));

        IntValues intValues = fieldData.getIntValues();

        assertThat(intValues.isMultiValued(), equalTo(true));

        assertThat(intValues.hasValue(0), equalTo(true));
        assertThat(intValues.hasValue(1), equalTo(false));
        assertThat(intValues.hasValue(2), equalTo(true));

        assertThat(intValues.getValue(0), equalTo(2));
        assertThat(intValues.getValue(2), equalTo(3));

        assertThat(intValues.getValueMissing(0, -1), equalTo(2));
        assertThat(intValues.getValueMissing(1, -1), equalTo(-1));
        assertThat(intValues.getValueMissing(2, -1), equalTo(3));

        IntArrayRef intArrayRef = intValues.getValues(0);
        assertThat(intArrayRef.size(), equalTo(2));
        assertThat(intArrayRef.values[intArrayRef.start], equalTo(2));
        assertThat(intArrayRef.values[intArrayRef.start + 1], equalTo(4));

        intArrayRef = intValues.getValues(1);
        assertThat(intArrayRef.size(), equalTo(0));

        intArrayRef = intValues.getValues(2);
        assertThat(intArrayRef.size(), equalTo(1));
        assertThat(intArrayRef.values[intArrayRef.start], equalTo(3));

        IntValues.Iter intValuesIter = intValues.getIter(0);
        assertThat(intValuesIter.hasNext(), equalTo(true));
        assertThat(intValuesIter.next(), equalTo(2));
        assertThat(intValuesIter.hasNext(), equalTo(true));
        assertThat(intValuesIter.next(), equalTo(4));
        assertThat(intValuesIter.hasNext(), equalTo(false));

        intValuesIter = intValues.getIter(1);
        assertThat(intValuesIter.hasNext(), equalTo(false));

        intValuesIter = intValues.getIter(2);
        assertThat(intValuesIter.hasNext(), equalTo(true));
        assertThat(intValuesIter.next(), equalTo(3));
        assertThat(intValuesIter.hasNext(), equalTo(false));

        intValues.forEachValueInDoc(0, new IntValuesVerifierProc(0).addExpected(2).addExpected(4));
        intValues.forEachValueInDoc(1, new IntValuesVerifierProc(1).addMissing());
        intValues.forEachValueInDoc(2, new IntValuesVerifierProc(2).addExpected(3));

        FloatValues floatValues = fieldData.getFloatValues();

        assertThat(floatValues.isMultiValued(), equalTo(true));

        assertThat(floatValues.hasValue(0), equalTo(true));
        assertThat(floatValues.hasValue(1), equalTo(false));
        assertThat(floatValues.hasValue(2), equalTo(true));

        assertThat(floatValues.getValue(0), equalTo(2f));
        assertThat(floatValues.getValue(2), equalTo(3f));

        assertThat(floatValues.getValueMissing(0, -1), equalTo(2f));
        assertThat(floatValues.getValueMissing(1, -1), equalTo(-1f));
        assertThat(floatValues.getValueMissing(2, -1), equalTo(3f));

        FloatArrayRef floatArrayRef = floatValues.getValues(0);
        assertThat(floatArrayRef.size(), equalTo(2));
        assertThat(floatArrayRef.values[floatArrayRef.start], equalTo(2f));
        assertThat(floatArrayRef.values[floatArrayRef.start + 1], equalTo(4f));

        floatArrayRef = floatValues.getValues(1);
        assertThat(floatArrayRef.size(), equalTo(0));

        floatArrayRef = floatValues.getValues(2);
        assertThat(floatArrayRef.size(), equalTo(1));
        assertThat(floatArrayRef.values[floatArrayRef.start], equalTo(3f));

        FloatValues.Iter floatValuesIter = floatValues.getIter(0);
        assertThat(floatValuesIter.hasNext(), equalTo(true));
        assertThat(floatValuesIter.next(), equalTo(2f));
        assertThat(floatValuesIter.hasNext(), equalTo(true));
        assertThat(floatValuesIter.next(), equalTo(4f));
        assertThat(floatValuesIter.hasNext(), equalTo(false));

        floatValuesIter = floatValues.getIter(1);
        assertThat(floatValuesIter.hasNext(), equalTo(false));

        floatValuesIter = floatValues.getIter(2);
        assertThat(floatValuesIter.hasNext(), equalTo(true));
        assertThat(floatValuesIter.next(), equalTo(3f));
        assertThat(floatValuesIter.hasNext(), equalTo(false));

        floatValues.forEachValueInDoc(0, new FloatValuesVerifierProc(0).addExpected(2f).addExpected(4f));
        floatValues.forEachValueInDoc(1, new FloatValuesVerifierProc(1).addMissing());
        floatValues.forEachValueInDoc(2, new FloatValuesVerifierProc(2).addExpected(3f));
    }
}
