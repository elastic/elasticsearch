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

package org.elasticsearch.search.aggregations.support;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.AbstractSortedNumericDocValues;
import org.elasticsearch.index.fielddata.AbstractSortedSetDocValues;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.LongUnaryOperator;

public class MissingValuesTests extends ESTestCase {

    public void testMissingBytes() throws IOException {
        final int numDocs = TestUtil.nextInt(random(), 1, 100);
        final BytesRef[][] values = new BytesRef[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            values[i] = new BytesRef[random().nextInt(4)];
            for (int j = 0; j < values[i].length; ++j) {
                values[i][j] = new BytesRef(RandomStrings.randomAsciiOfLength(random(), 2));
            }
            Arrays.sort(values[i]);
        }
        SortedBinaryDocValues asBinaryValues = new SortedBinaryDocValues() {

            int doc = -1;
            int i;

            @Override
            public BytesRef nextValue() {
                return values[doc][i++];
            }

            @Override
            public boolean advanceExact(int docId) {
                doc = docId;
                i = 0;
                return values[doc].length > 0;
            }

            @Override
            public int docValueCount() {
                return values[doc].length;
            }
        };
        final BytesRef missing = new BytesRef(RandomStrings.randomAsciiOfLength(random(), 2));
        SortedBinaryDocValues withMissingReplaced = MissingValues.replaceMissing(asBinaryValues, missing);
        for (int i = 0; i < numDocs; ++i) {
            assertTrue(withMissingReplaced.advanceExact(i));
            if (values[i].length > 0) {
                assertEquals(values[i].length, withMissingReplaced.docValueCount());
                for (int j = 0; j < values[i].length; ++j) {
                    assertEquals(values[i][j], withMissingReplaced.nextValue());
                }
            } else {
                assertEquals(1, withMissingReplaced.docValueCount());
                assertEquals(missing, withMissingReplaced.nextValue());
            }
        }
    }

    public void testMissingOrds() throws IOException {
        final int numDocs = TestUtil.nextInt(random(), 1, 100);
        final int numOrds = TestUtil.nextInt(random(), 1, 10);

        final Set<BytesRef> valueSet = new HashSet<>();
        while (valueSet.size() < numOrds) {
            valueSet.add(new BytesRef(RandomStrings.randomAsciiOfLength(random(), 5)));
        }
        final BytesRef[] values = valueSet.toArray(new BytesRef[numOrds]);
        Arrays.sort(values);

        final int[][] ords = new int[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            ords[i] = new int[random().nextInt(numOrds)];
            for (int j = 0; j < ords[i].length; ++j) {
                ords[i][j] = j;
            }
            for (int j = ords[i].length - 1; j >= 0; --j) {
                final int maxOrd = j == ords[i].length - 1 ? numOrds : ords[i][j+1];
                ords[i][j] = TestUtil.nextInt(random(), ords[i][j], maxOrd - 1);
            }
        }
        SortedSetDocValues asSortedSet = new AbstractSortedSetDocValues() {

            int doc = -1;
            int i;

            @Override
            public boolean advanceExact(int docID) {
                doc = docID;
                i = 0;
                return ords[doc].length > 0;
            }

            @Override
            public BytesRef lookupOrd(long ord) {
                return values[(int) ord];
            }

            @Override
            public long getValueCount() {
                return values.length;
            }

            @Override
            public long nextOrd() {
                if (i < ords[doc].length) {
                    return ords[doc][i++];
                } else {
                    return NO_MORE_ORDS;
                }
            }
        };

        final BytesRef existingMissing = RandomPicks.randomFrom(random(), values);
        final BytesRef missingMissing = new BytesRef(RandomStrings.randomAsciiOfLength(random(), 5));

        for (BytesRef missing : Arrays.asList(existingMissing, missingMissing)) {
            SortedSetDocValues withMissingReplaced = MissingValues.replaceMissing(asSortedSet, missing);
            if (valueSet.contains(missing)) {
                assertEquals(values.length, withMissingReplaced.getValueCount());
            } else {
                assertEquals(values.length + 1, withMissingReplaced.getValueCount());
            }
            for (int i = 0; i < numDocs; ++i) {
                assertTrue(withMissingReplaced.advanceExact(i));
                if (ords[i].length > 0) {
                    for (int ord : ords[i]) {
                        assertEquals(values[ord],
                                withMissingReplaced.lookupOrd(withMissingReplaced.nextOrd()));
                    }
                    assertEquals(SortedSetDocValues.NO_MORE_ORDS, withMissingReplaced.nextOrd());
                } else {
                    assertEquals(missing, withMissingReplaced.lookupOrd(withMissingReplaced.nextOrd()));
                    assertEquals(SortedSetDocValues.NO_MORE_ORDS, withMissingReplaced.nextOrd());
                }
            }
        }
    }

    public void testGlobalMapping() throws IOException {
        final int numOrds = TestUtil.nextInt(random(), 1, 10);
        final int numGlobalOrds = TestUtil.nextInt(random(), numOrds, numOrds + 3);

        final Set<BytesRef> valueSet = new HashSet<>();
        while (valueSet.size() < numOrds) {
            valueSet.add(new BytesRef(RandomStrings.randomAsciiLettersOfLength(random(), 5)));
        }
        final BytesRef[] values = valueSet.toArray(new BytesRef[0]);
        Arrays.sort(values);

        final Set<BytesRef> globalValueSet = new HashSet<>(valueSet);
        while (globalValueSet.size() < numGlobalOrds) {
            globalValueSet.add(new BytesRef(RandomStrings.randomAsciiLettersOfLength(random(), 5)));
        }
        final BytesRef[] globalValues = globalValueSet.toArray(new BytesRef[0]);
        Arrays.sort(globalValues);

        // exists in the current segment
        BytesRef missing = RandomPicks.randomFrom(random(), values);
        doTestGlobalMapping(values, globalValues, missing);

        // missing in all segments
        do {
            missing = new BytesRef(RandomStrings.randomAsciiLettersOfLength(random(), 5));
        } while (globalValueSet.contains(missing));
        doTestGlobalMapping(values, globalValues, missing);

        if (globalValueSet.size() > valueSet.size()) {
            // exists in other segments only
            Set<BytesRef> other = new HashSet<>(globalValueSet);
            other.removeAll(valueSet);
            missing = RandomPicks.randomFrom(random(), other.toArray(new BytesRef[0]));
            doTestGlobalMapping(values, globalValues, missing);
        }
    }

    private void doTestGlobalMapping(BytesRef[] values, BytesRef[] globalValues, BytesRef missing) throws IOException {
        LongUnaryOperator segmentToGlobalOrd = segmentOrd -> Arrays.binarySearch(globalValues, values[Math.toIntExact(segmentOrd)]);
        SortedSetDocValues sortedValues = asOrds(values);
        SortedSetDocValues sortedGlobalValues = asOrds(globalValues);

        LongUnaryOperator withMissingSegmentToGlobalOrd = MissingValues.getGlobalMapping(
                sortedValues, sortedGlobalValues, segmentToGlobalOrd, missing);
        SortedSetDocValues withMissingValues = MissingValues.replaceMissing(sortedValues, missing);
        SortedSetDocValues withMissingGlobalValues = MissingValues.replaceMissing(sortedGlobalValues, missing);

        for (long segmentOrd = 0; segmentOrd < withMissingValues.getValueCount(); ++segmentOrd) {
            long expectedGlobalOrd = withMissingSegmentToGlobalOrd.applyAsLong(segmentOrd);
            assertEquals(withMissingValues.lookupOrd(segmentOrd), withMissingGlobalValues.lookupOrd(expectedGlobalOrd));
        }
    }

    private static SortedSetDocValues asOrds(BytesRef[] values) {
        return new AbstractSortedSetDocValues() {

            @Override
            public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public long nextOrd() throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public BytesRef lookupOrd(long ord) throws IOException {
                return values[Math.toIntExact(ord)];
            }

            @Override
            public long getValueCount() {
                return values.length;
            }
        };
    }

    public void testMissingLongs() throws IOException {
        final int numDocs = TestUtil.nextInt(random(), 1, 100);
        final int[][] values = new int[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            values[i] = new int[random().nextInt(4)];
            for (int j = 0; j < values[i].length; ++j) {
                values[i][j] = randomInt();
            }
            Arrays.sort(values[i]);
        }
        SortedNumericDocValues asNumericValues = new AbstractSortedNumericDocValues() {

            int doc = -1;
            int i;

            @Override
            public long nextValue() {
                return values[doc][i++];
            }

            @Override
            public boolean advanceExact(int docId) {
                doc = docId;
                i = 0;
                return values[doc].length > 0;
            }

            @Override
            public int docValueCount() {
                return values[doc].length;
            }
        };
        final long missing = randomInt();
        SortedNumericDocValues withMissingReplaced = MissingValues.replaceMissing(asNumericValues, missing);
        for (int i = 0; i < numDocs; ++i) {
            assertTrue(withMissingReplaced.advanceExact(i));
            if (values[i].length > 0) {
                assertEquals(values[i].length, withMissingReplaced.docValueCount());
                for (int j = 0; j < values[i].length; ++j) {
                    assertEquals(values[i][j], withMissingReplaced.nextValue());
                }
            } else {
                assertEquals(1, withMissingReplaced.docValueCount());
                assertEquals(missing, withMissingReplaced.nextValue());
            }
        }
    }

    public void testMissingDoubles() throws IOException {
        final int numDocs = TestUtil.nextInt(random(), 1, 100);
        final double[][] values = new double[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            values[i] = new double[random().nextInt(4)];
            for (int j = 0; j < values[i].length; ++j) {
                values[i][j] = randomDouble();
            }
            Arrays.sort(values[i]);
        }
        SortedNumericDoubleValues asNumericValues = new SortedNumericDoubleValues() {

            int doc = -1;
            int i;

            @Override
            public double nextValue() {
                return values[doc][i++];
            }

            @Override
            public boolean advanceExact(int docId) {
                doc = docId;
                i = 0;
                return true;
            }

            @Override
            public int docValueCount() {
                return values[doc].length;
            }
        };
        final long missing = randomInt();
        SortedNumericDoubleValues withMissingReplaced = MissingValues.replaceMissing(asNumericValues, missing);
        for (int i = 0; i < numDocs; ++i) {
            assertTrue(withMissingReplaced.advanceExact(i));
            if (values[i].length > 0) {
                assertEquals(values[i].length, withMissingReplaced.docValueCount());
                for (int j = 0; j < values[i].length; ++j) {
                    assertEquals(values[i][j], withMissingReplaced.nextValue(), 0);
                }
            } else {
                assertEquals(1, withMissingReplaced.docValueCount());
                assertEquals(missing, withMissingReplaced.nextValue(), 0);
            }
        }
    }

    public void testMissingGeoPoints() throws IOException {
        final int numDocs = TestUtil.nextInt(random(), 1, 100);
        final GeoPoint[][] values = new GeoPoint[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            values[i] = new GeoPoint[random().nextInt(4)];
            for (int j = 0; j < values[i].length; ++j) {
                values[i][j] = new GeoPoint(randomDouble() * 90, randomDouble() * 180);
            }
        }
        MultiGeoPointValues asGeoValues = new MultiGeoPointValues() {

            int doc = -1;
            int i;

            @Override
            public GeoPoint nextValue() {
                return values[doc][i++];
            }

            @Override
            public boolean advanceExact(int docId) {
                doc = docId;
                i = 0;
                return values[doc].length > 0;
            }

            @Override
            public int docValueCount() {
                return values[doc].length;
            }
        };
        final GeoPoint missing = new GeoPoint(randomDouble() * 90, randomDouble() * 180);
        MultiGeoPointValues withMissingReplaced = MissingValues.replaceMissing(asGeoValues, missing);
        for (int i = 0; i < numDocs; ++i) {
            assertTrue(withMissingReplaced.advanceExact(i));
            if (values[i].length > 0) {
                assertEquals(values[i].length, withMissingReplaced.docValueCount());
                for (int j = 0; j < values[i].length; ++j) {
                    assertEquals(values[i][j], withMissingReplaced.nextValue());
                }
            } else {
                assertEquals(1, withMissingReplaced.docValueCount());
                assertEquals(missing, withMissingReplaced.nextValue());
            }
        }
    }

    public void testFloatingPointDetection() {
        assertFalse(MissingValues.replaceMissing(ValuesSource.Numeric.EMPTY, 3).isFloatingPoint());
        assertTrue(MissingValues.replaceMissing(ValuesSource.Numeric.EMPTY, 3.5).isFloatingPoint());
    }
}
