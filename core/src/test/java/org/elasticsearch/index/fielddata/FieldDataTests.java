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

import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.test.ESTestCase;

public class FieldDataTests extends ESTestCase {

    public void testSortableLongBitsToDoubles() {
        final double value = randomDouble();
        final long valueBits = NumericUtils.doubleToSortableLong(value);

        NumericDocValues values = new NumericDocValues() {
            @Override
            public long get(int docID) {
                return valueBits;
            }
        };

        SortedNumericDoubleValues asMultiDoubles = FieldData.sortableLongBitsToDoubles(DocValues.singleton(values, null));
        NumericDoubleValues asDoubles = FieldData.unwrapSingleton(asMultiDoubles);
        assertNotNull(asDoubles);
        assertEquals(value, asDoubles.get(0), 0);

        NumericDocValues backToLongs = DocValues.unwrapSingleton(FieldData.toSortableLongBits(asMultiDoubles));
        assertSame(values, backToLongs);

        SortedNumericDocValues multiValues = new SortedNumericDocValues() {

            @Override
            public long valueAt(int index) {
                return valueBits;
            }

            @Override
            public void setDocument(int doc) {
            }

            @Override
            public int count() {
                return 1;
            }
        };

        asMultiDoubles = FieldData.sortableLongBitsToDoubles(multiValues);
        assertEquals(value, asMultiDoubles.valueAt(0), 0);
        assertSame(multiValues, FieldData.toSortableLongBits(asMultiDoubles));
    }

    public void testDoublesToSortableLongBits() {
        final double value = randomDouble();
        final long valueBits = NumericUtils.doubleToSortableLong(value);

        NumericDoubleValues values = new NumericDoubleValues() {
            @Override
            public double get(int docID) {
                return value;
            }
        };

        SortedNumericDocValues asMultiLongs = FieldData.toSortableLongBits(FieldData.singleton(values, null));
        NumericDocValues asLongs = DocValues.unwrapSingleton(asMultiLongs);
        assertNotNull(asLongs);
        assertEquals(valueBits, asLongs.get(0));

        SortedNumericDoubleValues multiValues = new SortedNumericDoubleValues() {
            @Override
            public double valueAt(int index) {
                return value;
            }

            @Override
            public void setDocument(int doc) {
            }

            @Override
            public int count() {
                return 1;
            }
        };

        asMultiLongs = FieldData.toSortableLongBits(multiValues);
        assertEquals(valueBits, asMultiLongs.valueAt(0));
        assertSame(multiValues, FieldData.sortableLongBitsToDoubles(asMultiLongs));
    }
}
