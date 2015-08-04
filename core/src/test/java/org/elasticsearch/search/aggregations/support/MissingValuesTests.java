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

import org.apache.lucene.index.RandomAccessOrds;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.index.fielddata.AbstractRandomAccessOrds;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class MissingValuesTests extends ESTestCase {

    public void testMissingBytes() {
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

            int i = -1;

            @Override
            public BytesRef valueAt(int index) {
                return values[i][index];
            }

            @Override
            public void setDocument(int docId) {
                i = docId;
            }

            @Override
            public int count() {
                return values[i].length;
            }
        };
        final BytesRef missing = new BytesRef(RandomStrings.randomAsciiOfLength(random(), 2));
        SortedBinaryDocValues withMissingReplaced = MissingValues.replaceMissing(asBinaryValues, missing);
        for (int i = 0; i < numDocs; ++i) {
            withMissingReplaced.setDocument(i);
            if (values[i].length > 0) {
                assertEquals(values[i].length, withMissingReplaced.count());
                for (int j = 0; j < values[i].length; ++j) {
                    assertEquals(values[i][j], withMissingReplaced.valueAt(j));
                }
            } else {
                assertEquals(1, withMissingReplaced.count());
                assertEquals(missing, withMissingReplaced.valueAt(0));
            }
        }
    }

    public void testMissingOrds() {
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
        RandomAccessOrds asRandomAccessOrds = new AbstractRandomAccessOrds() {

            int i = -1;

            @Override
            public void doSetDocument(int docID) {
                i = docID;
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
            public long ordAt(int index) {
                return ords[i][index];
            }

            @Override
            public int cardinality() {
                return ords[i].length;
            }
        };

        final BytesRef existingMissing = RandomPicks.randomFrom(random(), values);
        final BytesRef missingMissing = new BytesRef(RandomStrings.randomAsciiOfLength(random(), 5));

        for (BytesRef missing : Arrays.asList(existingMissing, missingMissing)) {
            RandomAccessOrds withMissingReplaced = MissingValues.replaceMissing(asRandomAccessOrds, missing);
            if (valueSet.contains(missing)) {
                assertEquals(values.length, withMissingReplaced.getValueCount());
            } else {
                assertEquals(values.length + 1, withMissingReplaced.getValueCount());
            }
            for (int i = 0; i < numDocs; ++i) {
                withMissingReplaced.setDocument(i);
                if (ords[i].length > 0) {
                    assertEquals(ords[i].length, withMissingReplaced.cardinality());
                    for (int j = 0; j < ords[i].length; ++j) {
                        assertEquals(values[ords[i][j]], withMissingReplaced.lookupOrd(withMissingReplaced.ordAt(j)));
                    }
                } else {
                    assertEquals(1, withMissingReplaced.cardinality());
                    assertEquals(missing, withMissingReplaced.lookupOrd(withMissingReplaced.ordAt(0)));
                }
            }
        }
    }

    public void testMissingLongs() {
        final int numDocs = TestUtil.nextInt(random(), 1, 100);
        final int[][] values = new int[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            values[i] = new int[random().nextInt(4)];
            for (int j = 0; j < values[i].length; ++j) {
                values[i][j] = randomInt();
            }
            Arrays.sort(values[i]);
        }
        SortedNumericDocValues asNumericValues = new SortedNumericDocValues() {

            int i = -1;

            @Override
            public long valueAt(int index) {
                return values[i][index];
            }

            @Override
            public void setDocument(int docId) {
                i = docId;
            }

            @Override
            public int count() {
                return values[i].length;
            }
        };
        final long missing = randomInt();
        SortedNumericDocValues withMissingReplaced = MissingValues.replaceMissing(asNumericValues, missing);
        for (int i = 0; i < numDocs; ++i) {
            withMissingReplaced.setDocument(i);
            if (values[i].length > 0) {
                assertEquals(values[i].length, withMissingReplaced.count());
                for (int j = 0; j < values[i].length; ++j) {
                    assertEquals(values[i][j], withMissingReplaced.valueAt(j));
                }
            } else {
                assertEquals(1, withMissingReplaced.count());
                assertEquals(missing, withMissingReplaced.valueAt(0));
            }
        }
    }

    public void testMissingDoubles() {
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

            int i = -1;

            @Override
            public double valueAt(int index) {
                return values[i][index];
            }

            @Override
            public void setDocument(int docId) {
                i = docId;
            }

            @Override
            public int count() {
                return values[i].length;
            }
        };
        final long missing = randomInt();
        SortedNumericDoubleValues withMissingReplaced = MissingValues.replaceMissing(asNumericValues, missing);
        for (int i = 0; i < numDocs; ++i) {
            withMissingReplaced.setDocument(i);
            if (values[i].length > 0) {
                assertEquals(values[i].length, withMissingReplaced.count());
                for (int j = 0; j < values[i].length; ++j) {
                    assertEquals(values[i][j], withMissingReplaced.valueAt(j), 0);
                }
            } else {
                assertEquals(1, withMissingReplaced.count());
                assertEquals(missing, withMissingReplaced.valueAt(0), 0);
            }
        }
    }

    public void testMissingGeoPoints() {
        final int numDocs = TestUtil.nextInt(random(), 1, 100);
        final GeoPoint[][] values = new GeoPoint[numDocs][];
        for (int i = 0; i < numDocs; ++i) {
            values[i] = new GeoPoint[random().nextInt(4)];
            for (int j = 0; j < values[i].length; ++j) {
                values[i][j] = new GeoPoint(randomDouble() * 90, randomDouble() * 180);
            }
        }
        MultiGeoPointValues asGeoValues = new MultiGeoPointValues() {

            int i = -1;

            @Override
            public GeoPoint valueAt(int index) {
                return values[i][index];
            }

            @Override
            public void setDocument(int docId) {
                i = docId;
            }

            @Override
            public int count() {
                return values[i].length;
            }
        };
        final GeoPoint missing = new GeoPoint(randomDouble() * 90, randomDouble() * 180);
        MultiGeoPointValues withMissingReplaced = MissingValues.replaceMissing(asGeoValues, missing);
        for (int i = 0; i < numDocs; ++i) {
            withMissingReplaced.setDocument(i);
            if (values[i].length > 0) {
                assertEquals(values[i].length, withMissingReplaced.count());
                for (int j = 0; j < values[i].length; ++j) {
                    assertEquals(values[i][j], withMissingReplaced.valueAt(j));
                }
            } else {
                assertEquals(1, withMissingReplaced.count());
                assertEquals(missing, withMissingReplaced.valueAt(0));
            }
        }
    }
}
