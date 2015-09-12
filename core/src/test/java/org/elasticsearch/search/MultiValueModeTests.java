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

package org.elasticsearch.search;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.lucene.index.*;
import org.apache.lucene.util.BitDocIdSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public class MultiValueModeTests extends ESTestCase {

    private static FixedBitSet randomRootDocs(int maxDoc) {
        FixedBitSet set = new FixedBitSet(maxDoc);
        for (int i = 0; i < maxDoc; ++i) {
            if (randomBoolean()) {
                set.set(i);
            }
        }
        // the last doc must be a root doc
        set.set(maxDoc - 1);
        return set;
    }

    private static FixedBitSet randomInnerDocs(FixedBitSet rootDocs) {
        FixedBitSet innerDocs = new FixedBitSet(rootDocs.length());
        for (int i = 0; i < innerDocs.length(); ++i) {
            if (!rootDocs.get(i) && randomBoolean()) {
                innerDocs.set(i);
            }
        }
        return innerDocs;
    }

    public void testSingleValuedLongs() throws Exception {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final long[] array = new long[numDocs];
        final FixedBitSet docsWithValue = randomBoolean() ? null : new FixedBitSet(numDocs);
        for (int i = 0; i < array.length; ++i) {
            if (randomBoolean()) {
                array[i] = randomLong();
                if (docsWithValue != null) {
                    docsWithValue.set(i);
                }
            } else if (docsWithValue != null && randomBoolean()) {
                docsWithValue.set(i);
            }
        }
        final NumericDocValues singleValues = new NumericDocValues() {
            @Override
            public long get(int docID) {
                return array[docID];
            }
        };
        final SortedNumericDocValues multiValues = DocValues.singleton(singleValues, docsWithValue);
        verify(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verify(multiValues, numDocs, rootDocs, innerDocs);
    }

    public void testMultiValuedLongs() throws Exception  {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final long[][] array = new long[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            final long[] values = new long[randomInt(4)];
            for (int j = 0; j < values.length; ++j) {
                values[j] = randomLong();
            }
            Arrays.sort(values);
            array[i] = values;
        }
        final SortedNumericDocValues multiValues = new SortedNumericDocValues() {
            int doc;

            @Override
            public long valueAt(int index) {
                return array[doc][index];
            }

            @Override
            public void setDocument(int doc) {
                this.doc = doc;
            }

            @Override
            public int count() {
                return array[doc].length;
            }
        };
        verify(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verify(multiValues, numDocs, rootDocs, innerDocs);
    }

    private void verify(SortedNumericDocValues values, int maxDoc) {
        for (long missingValue : new long[] { 0, randomLong() }) {
            for (MultiValueMode mode : MultiValueMode.values()) {
                final NumericDocValues selected = mode.select(values, missingValue);
                for (int i = 0; i < maxDoc; ++i) {
                    final long actual = selected.get(i);
                    long expected = 0;
                    values.setDocument(i);
                    int numValues = values.count();
                    if (numValues == 0) {
                        expected = missingValue;
                    } else {
                        if (mode == MultiValueMode.MAX) {
                            expected = Long.MIN_VALUE;
                        } else if (mode == MultiValueMode.MIN) {
                            expected = Long.MAX_VALUE;
                        }
                        for (int j = 0; j < numValues; ++j) {
                            if (mode == MultiValueMode.SUM || mode == MultiValueMode.AVG) {
                                expected += values.valueAt(j);
                            } else if (mode == MultiValueMode.MIN) {
                                expected = Math.min(expected, values.valueAt(j));
                            } else if (mode == MultiValueMode.MAX) {
                                expected = Math.max(expected, values.valueAt(j));
                            }
                        }
                        if (mode == MultiValueMode.AVG) {
                            expected = numValues > 1 ? Math.round((double)expected/(double)numValues) : expected;
                        } else if (mode == MultiValueMode.MEDIAN) {
                            int value = numValues/2;
                            if (numValues % 2 == 0) {
                                expected = Math.round((values.valueAt(value - 1) + values.valueAt(value))/2.0);
                            } else {
                                expected = values.valueAt(value);
                            }
                        }
                    }

                    assertEquals(mode.toString() + " docId=" + i, expected, actual);
                }
            }
        }
    }

    private void verify(SortedNumericDocValues values, int maxDoc, FixedBitSet rootDocs, FixedBitSet innerDocs) throws IOException {
        for (long missingValue : new long[] { 0, randomLong() }) {
            for (MultiValueMode mode : new MultiValueMode[] {MultiValueMode.MIN, MultiValueMode.MAX, MultiValueMode.SUM, MultiValueMode.AVG}) {
                final NumericDocValues selected = mode.select(values, missingValue, rootDocs, new BitDocIdSet(innerDocs), maxDoc);
                int prevRoot = -1;
                for (int root = rootDocs.nextSetBit(0); root != -1; root = root + 1 < maxDoc ? rootDocs.nextSetBit(root + 1) : -1) {
                    final long actual = selected.get(root);
                    long expected = 0;
                    if (mode == MultiValueMode.MAX) {
                        expected = Long.MIN_VALUE;
                    } else if (mode == MultiValueMode.MIN) {
                        expected = Long.MAX_VALUE;
                    }
                    int numValues = 0;
                    for (int child = innerDocs.nextSetBit(prevRoot + 1); child != -1 && child < root; child = innerDocs.nextSetBit(child + 1)) {
                        values.setDocument(child);
                        for (int j = 0; j < values.count(); ++j) {
                            if (mode == MultiValueMode.SUM || mode == MultiValueMode.AVG) {
                                expected += values.valueAt(j);
                            } else if (mode == MultiValueMode.MIN) {
                                expected = Math.min(expected, values.valueAt(j));
                            } else if (mode == MultiValueMode.MAX) {
                                expected = Math.max(expected, values.valueAt(j));
                            }
                            ++numValues;
                        }
                    }
                    if (numValues == 0) {
                        expected = missingValue;
                    } else if (mode == MultiValueMode.AVG) {
                        expected = numValues > 1 ? Math.round((double) expected / (double) numValues) : expected;
                    }

                    assertEquals(mode.toString() + " docId=" + root, expected, actual);

                    prevRoot = root;
                }
            }
        }
    }

    public void testSingleValuedDoubles() throws Exception  {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final double[] array = new double[numDocs];
        final FixedBitSet docsWithValue = randomBoolean() ? null : new FixedBitSet(numDocs);
        for (int i = 0; i < array.length; ++i) {
            if (randomBoolean()) {
                array[i] = randomDouble();
                if (docsWithValue != null) {
                    docsWithValue.set(i);
                }
            } else if (docsWithValue != null && randomBoolean()) {
                docsWithValue.set(i);
            }
        }
        final NumericDoubleValues singleValues = new NumericDoubleValues() {
            @Override
            public double get(int docID) {
                return array[docID];
            }
        };
        final SortedNumericDoubleValues multiValues = FieldData.singleton(singleValues, docsWithValue);
        verify(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verify(multiValues, numDocs, rootDocs, innerDocs);
    }

    public void testMultiValuedDoubles() throws Exception  {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final double[][] array = new double[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            final double[] values = new double[randomInt(4)];
            for (int j = 0; j < values.length; ++j) {
                values[j] = randomDouble();
            }
            Arrays.sort(values);
            array[i] = values;
        }
        final SortedNumericDoubleValues multiValues = new SortedNumericDoubleValues() {
            int doc;

            @Override
            public double valueAt(int index) {
                return array[doc][index];
            }

            @Override
            public void setDocument(int doc) {
                this.doc = doc;
            }

            @Override
            public int count() {
                return array[doc].length;
            }
        };
        verify(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verify(multiValues, numDocs, rootDocs, innerDocs);
    }

    private void verify(SortedNumericDoubleValues values, int maxDoc) {
        for (long missingValue : new long[] { 0, randomLong() }) {
            for (MultiValueMode mode : MultiValueMode.values()) {
                if (MultiValueMode.MEDIAN.equals(mode)) {
                    continue;
                }
                final NumericDoubleValues selected = mode.select(values, missingValue);
                for (int i = 0; i < maxDoc; ++i) {
                    final double actual = selected.get(i);
                    double expected = 0.0;
                    values.setDocument(i);
                    int numValues = values.count();
                    if (numValues == 0) {
                        expected = missingValue;
                    } else {
                        if (mode == MultiValueMode.MAX) {
                            expected = Long.MIN_VALUE;
                        } else if (mode == MultiValueMode.MIN) {
                            expected = Long.MAX_VALUE;
                        }
                        for (int j = 0; j < numValues; ++j) {
                            if (mode == MultiValueMode.SUM || mode == MultiValueMode.AVG) {
                                expected += values.valueAt(j);
                            } else if (mode == MultiValueMode.MIN) {
                                expected = Math.min(expected, values.valueAt(j));
                            } else if (mode == MultiValueMode.MAX) {
                                expected = Math.max(expected, values.valueAt(j));
                            }
                        }
                        if (mode == MultiValueMode.AVG) {
                            expected = expected/numValues;
                        } else if (mode == MultiValueMode.MEDIAN) {
                            int value = numValues/2;
                            if (numValues % 2 == 0) {
                                expected = (values.valueAt(value - 1) + values.valueAt(value))/2.0;
                            } else {
                                expected = values.valueAt(value);
                            }
                        }
                    }

                    assertEquals(mode.toString() + " docId=" + i, expected, actual, 0.1);
                }
            }
        }
    }

    private void verify(SortedNumericDoubleValues values, int maxDoc, FixedBitSet rootDocs, FixedBitSet innerDocs) throws IOException {
        for (long missingValue : new long[] { 0, randomLong() }) {
            for (MultiValueMode mode : new MultiValueMode[] {MultiValueMode.MIN, MultiValueMode.MAX, MultiValueMode.SUM, MultiValueMode.AVG}) {
                final NumericDoubleValues selected = mode.select(values, missingValue, rootDocs, new BitDocIdSet(innerDocs), maxDoc);
                int prevRoot = -1;
                for (int root = rootDocs.nextSetBit(0); root != -1; root = root + 1 < maxDoc ? rootDocs.nextSetBit(root + 1) : -1) {
                    final double actual = selected.get(root);
                    double expected = 0.0;
                    if (mode == MultiValueMode.MAX) {
                        expected = Long.MIN_VALUE;
                    } else if (mode == MultiValueMode.MIN) {
                        expected = Long.MAX_VALUE;
                    }
                    int numValues = 0;
                    for (int child = innerDocs.nextSetBit(prevRoot + 1); child != -1 && child < root; child = innerDocs.nextSetBit(child + 1)) {
                        values.setDocument(child);
                        for (int j = 0; j < values.count(); ++j) {
                            if (mode == MultiValueMode.SUM || mode == MultiValueMode.AVG) {
                                expected += values.valueAt(j);
                            } else if (mode == MultiValueMode.MIN) {
                                expected = Math.min(expected, values.valueAt(j));
                            } else if (mode == MultiValueMode.MAX) {
                                expected = Math.max(expected, values.valueAt(j));
                            }
                            ++numValues;
                        }
                    }
                    if (numValues == 0) {
                        expected = missingValue;
                    } else if (mode == MultiValueMode.AVG) {
                        expected = expected/numValues;
                    }

                    assertEquals(mode.toString() + " docId=" + root, expected, actual, 0.1);

                    prevRoot = root;
                }
            }
        }
    }

    public void testSingleValuedStrings() throws Exception  {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final BytesRef[] array = new BytesRef[numDocs];
        final FixedBitSet docsWithValue = randomBoolean() ? null : new FixedBitSet(numDocs);
        for (int i = 0; i < array.length; ++i) {
            if (randomBoolean()) {
                array[i] = new BytesRef(RandomStrings.randomAsciiOfLength(getRandom(), 8));
                if (docsWithValue != null) {
                    docsWithValue.set(i);
                }
            } else {
                array[i] = new BytesRef();
                if (docsWithValue != null && randomBoolean()) {
                    docsWithValue.set(i);
                }
            }
        }
        final BinaryDocValues singleValues = new BinaryDocValues() {
            @Override
            public BytesRef get(int docID) {
                return BytesRef.deepCopyOf(array[docID]);
            }
        };
        final SortedBinaryDocValues multiValues = FieldData.singleton(singleValues, docsWithValue);
        verify(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verify(multiValues, numDocs, rootDocs, innerDocs);
    }

    public void testMultiValuedStrings() throws Exception  {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final BytesRef[][] array = new BytesRef[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            final BytesRef[] values = new BytesRef[randomInt(4)];
            for (int j = 0; j < values.length; ++j) {
                values[j] = new BytesRef(RandomStrings.randomAsciiOfLength(getRandom(), 8));
            }
            Arrays.sort(values);
            array[i] = values;
        }
        final SortedBinaryDocValues multiValues = new SortedBinaryDocValues() {
            int doc;

            @Override
            public BytesRef valueAt(int index) {
                return BytesRef.deepCopyOf(array[doc][index]);
            }

            @Override
            public void setDocument(int doc) {
                this.doc = doc;
            }

            @Override
            public int count() {
                return array[doc].length;
            }
        };
        verify(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verify(multiValues, numDocs, rootDocs, innerDocs);
    }

    private void verify(SortedBinaryDocValues values, int maxDoc) {
        for (BytesRef missingValue : new BytesRef[] { new BytesRef(), new BytesRef(RandomStrings.randomAsciiOfLength(getRandom(), 8)) }) {
            for (MultiValueMode mode : new MultiValueMode[] {MultiValueMode.MIN, MultiValueMode.MAX}) {
                final BinaryDocValues selected = mode.select(values, missingValue);
                for (int i = 0; i < maxDoc; ++i) {
                    final BytesRef actual = selected.get(i);
                    BytesRef expected = null;
                    values.setDocument(i);
                    int numValues = values.count();
                    if (numValues == 0) {
                        expected = missingValue;
                    } else {
                        for (int j = 0; j < numValues; ++j) {
                            if (expected == null) {
                                expected = BytesRef.deepCopyOf(values.valueAt(j));
                            } else {
                                if (mode == MultiValueMode.MIN) {
                                    expected = expected.compareTo(values.valueAt(j)) <= 0 ? expected : BytesRef.deepCopyOf(values.valueAt(j));
                                } else if (mode == MultiValueMode.MAX) {
                                    expected = expected.compareTo(values.valueAt(j)) > 0 ? expected : BytesRef.deepCopyOf(values.valueAt(j));
                                }
                            }
                        }
                        if (expected == null) {
                            expected = missingValue;
                        }
                    }

                    assertEquals(mode.toString() + " docId=" + i, expected, actual);
                }
            }
        }
    }

    private void verify(SortedBinaryDocValues values, int maxDoc, FixedBitSet rootDocs, FixedBitSet innerDocs) throws IOException {
        for (BytesRef missingValue : new BytesRef[] { new BytesRef(), new BytesRef(RandomStrings.randomAsciiOfLength(getRandom(), 8)) }) {
            for (MultiValueMode mode : new MultiValueMode[] {MultiValueMode.MIN, MultiValueMode.MAX}) {
                final BinaryDocValues selected = mode.select(values, missingValue, rootDocs, new BitDocIdSet(innerDocs), maxDoc);
                int prevRoot = -1;
                for (int root = rootDocs.nextSetBit(0); root != -1; root = root + 1 < maxDoc ? rootDocs.nextSetBit(root + 1) : -1) {
                    final BytesRef actual = selected.get(root);
                    BytesRef expected = null;
                    for (int child = innerDocs.nextSetBit(prevRoot + 1); child != -1 && child < root; child = innerDocs.nextSetBit(child + 1)) {
                        values.setDocument(child);
                        for (int j = 0; j < values.count(); ++j) {
                            if (expected == null) {
                                expected = BytesRef.deepCopyOf(values.valueAt(j));
                            } else {
                                if (mode == MultiValueMode.MIN) {
                                    expected = expected.compareTo(values.valueAt(j)) <= 0 ? expected : BytesRef.deepCopyOf(values.valueAt(j));
                                } else if (mode == MultiValueMode.MAX) {
                                    expected = expected.compareTo(values.valueAt(j)) > 0 ? expected : BytesRef.deepCopyOf(values.valueAt(j));
                                }
                            }
                        }
                    }
                    if (expected == null) {
                        expected = missingValue;
                    }

                    assertEquals(mode.toString() + " docId=" + root, expected, actual);

                    prevRoot = root;
                }
            }
        }
    }


    public void testSingleValuedOrds() throws Exception  {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final int[] array = new int[numDocs];
        for (int i = 0; i < array.length; ++i) {
            if (randomBoolean()) {
                array[i] = randomInt(1000);
            } else {
                array[i] = -1;
            }
        }
        final SortedDocValues singleValues = new SortedDocValues() {
            @Override
            public int getOrd(int docID) {
                return array[docID];
            }

            @Override
            public BytesRef lookupOrd(int ord) {
                throw new UnsupportedOperationException();
            }

            @Override
            public int getValueCount() {
                return 1 << 20;
            }
        };
        final RandomAccessOrds multiValues = (RandomAccessOrds) DocValues.singleton(singleValues);
        verify(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verify(multiValues, numDocs, rootDocs, innerDocs);
    }

    public void testMultiValuedOrds() throws Exception  {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final long[][] array = new long[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            final long[] values = new long[randomInt(4)];
            for (int j = 0; j < values.length; ++j) {
                values[j] = j == 0 ? randomInt(1000) : values[j - 1] + 1 + randomInt(1000);
            }
            array[i] = values;
        }
        final RandomAccessOrds multiValues = new RandomAccessOrds() {
            int doc;

            @Override
            public long ordAt(int index) {
                return array[doc][index];
            }

            @Override
            public int cardinality() {
                return array[doc].length;
            }

            @Override
            public long nextOrd() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void setDocument(int docID) {
                this.doc = docID;
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getValueCount() {
                return 1 << 20;
            }
        };
        verify(multiValues, numDocs);
        final FixedBitSet rootDocs = randomRootDocs(numDocs);
        final FixedBitSet innerDocs = randomInnerDocs(rootDocs);
        verify(multiValues, numDocs, rootDocs, innerDocs);
    }

    private void verify(RandomAccessOrds values, int maxDoc) {
        for (MultiValueMode mode : new MultiValueMode[] {MultiValueMode.MIN, MultiValueMode.MAX}) {
            final SortedDocValues selected = mode.select(values);
            for (int i = 0; i < maxDoc; ++i) {
                final long actual = selected.getOrd(i);
                int expected = -1;
                values.setDocument(i);
                for (int j = 0; j < values.cardinality(); ++j) {
                    if (expected == -1) {
                        expected = (int) values.ordAt(j);
                    } else {
                        if (mode == MultiValueMode.MIN) {
                            expected = Math.min(expected, (int)values.ordAt(j));
                        } else if (mode == MultiValueMode.MAX) {
                            expected = Math.max(expected, (int)values.ordAt(j));
                        }
                    }
                }

                assertEquals(mode.toString() + " docId=" + i, expected, actual);
            }
        }
    }

    private void verify(RandomAccessOrds values, int maxDoc, FixedBitSet rootDocs, FixedBitSet innerDocs) throws IOException {
        for (MultiValueMode mode : new MultiValueMode[] {MultiValueMode.MIN, MultiValueMode.MAX}) {
            final SortedDocValues selected = mode.select(values, rootDocs, new BitDocIdSet(innerDocs));
            int prevRoot = -1;
            for (int root = rootDocs.nextSetBit(0); root != -1; root = root + 1 < maxDoc ? rootDocs.nextSetBit(root + 1) : -1) {
                final int actual = selected.getOrd(root);
                int expected = -1;
                for (int child = innerDocs.nextSetBit(prevRoot + 1); child != -1 && child < root; child = innerDocs.nextSetBit(child + 1)) {
                    values.setDocument(child);
                    for (int j = 0; j < values.cardinality(); ++j) {
                        if (expected == -1) {
                            expected = (int) values.ordAt(j);
                        } else {
                            if (mode == MultiValueMode.MIN) {
                                expected = Math.min(expected, (int)values.ordAt(j));
                            } else if (mode == MultiValueMode.MAX) {
                                expected = Math.max(expected, (int)values.ordAt(j));
                            }
                        }
                    }
                }

                assertEquals(mode.toString() + " docId=" + root, expected, actual);

                prevRoot = root;
            }
        }
    }

    public void testUnsortedSingleValuedDoubles() throws Exception  {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final double[] array = new double[numDocs];
        final FixedBitSet docsWithValue = randomBoolean() ? null : new FixedBitSet(numDocs);
        for (int i = 0; i < array.length; ++i) {
            if (randomBoolean()) {
                array[i] = randomDouble();
                if (docsWithValue != null) {
                    docsWithValue.set(i);
                }
            } else if (docsWithValue != null && randomBoolean()) {
                docsWithValue.set(i);
            }
        }
        final NumericDoubleValues singleValues = new NumericDoubleValues() {
            @Override
            public double get(int docID) {
                return array[docID];
            }
        };
        final SortedNumericDoubleValues singletonValues = FieldData.singleton(singleValues, docsWithValue);
        final MultiValueMode.UnsortedNumericDoubleValues multiValues = new MultiValueMode.UnsortedNumericDoubleValues() {
            int doc;

            @Override
            public int count() {
                return singletonValues.count();
            }

            @Override
            public void setDocument(int doc) {
                singletonValues.setDocument(doc);
            }

            @Override
            public double valueAt(int index) {
                return Math.cos(singletonValues.valueAt(index));
            }
        };
        verify(multiValues, numDocs);
    }

    public void testUnsortedMultiValuedDoubles() throws Exception  {
        final int numDocs = scaledRandomIntBetween(1, 100);
        final double[][] array = new double[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            final double[] values = new double[randomInt(4)];
            for (int j = 0; j < values.length; ++j) {
                values[j] = randomDouble();
            }
            Arrays.sort(values);
            array[i] = values;
        }
        final MultiValueMode.UnsortedNumericDoubleValues multiValues = new MultiValueMode.UnsortedNumericDoubleValues() {
            int doc;

            @Override
            public int count() {
                return array[doc].length;
            }

            @Override
            public void setDocument(int doc) {
                this.doc = doc;
            }

            @Override
            public double valueAt(int index) {
                return Math.sin(array[doc][index]);
            }
        };
        verify(multiValues, numDocs);
    }

    private void verify(MultiValueMode.UnsortedNumericDoubleValues values, int maxDoc) {
        for (double missingValue : new double[] { 0, randomDouble() }) {
            for (MultiValueMode mode : new MultiValueMode[] {MultiValueMode.MIN, MultiValueMode.MAX, MultiValueMode.SUM, MultiValueMode.AVG}) {
                final NumericDoubleValues selected = mode.select(values, missingValue);
                for (int i = 0; i < maxDoc; ++i) {
                    final double actual = selected.get(i);
                    double expected = 0.0;
                    values.setDocument(i);
                    int numValues = values.count();
                    if (numValues == 0) {
                        expected = missingValue;
                    } else {
                        if (mode == MultiValueMode.MAX) {
                            expected = Long.MIN_VALUE;
                        } else if (mode == MultiValueMode.MIN) {
                            expected = Long.MAX_VALUE;
                        }
                        for (int j = 0; j < numValues; ++j) {
                            if (mode == MultiValueMode.SUM || mode == MultiValueMode.AVG) {
                                expected += values.valueAt(j);
                            } else if (mode == MultiValueMode.MIN) {
                                expected = Math.min(expected, values.valueAt(j));
                            } else if (mode == MultiValueMode.MAX) {
                                expected = Math.max(expected, values.valueAt(j));
                            }
                        }
                        if (mode == MultiValueMode.AVG) {
                            expected = expected/numValues;
                        }
                    }

                    assertEquals(mode.toString() + " docId=" + i, expected, actual, 0.1);
                }
            }
        }
    }
}
