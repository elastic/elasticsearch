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

import java.io.IOException;

public class FieldDataTests extends ESTestCase {

    private static class DummyValues extends AbstractNumericDocValues {

        private final long value;
        private int docID = -1;

        DummyValues(long value) {
            this.value = value;
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            docID = target;
            return true;
        }

        @Override
        public int docID() {
            return docID;
        }

        @Override
        public long longValue() throws IOException {
            return value;
        }
    }

    public void testSortableLongBitsToDoubles() throws IOException {
        final double value = randomDouble();
        final long valueBits = NumericUtils.doubleToSortableLong(value);

        NumericDocValues values = new DummyValues(valueBits);

        SortedNumericDoubleValues asMultiDoubles = FieldData.sortableLongBitsToDoubles(DocValues.singleton(values));
        NumericDoubleValues asDoubles = FieldData.unwrapSingleton(asMultiDoubles);
        assertNotNull(asDoubles);
        assertTrue(asDoubles.advanceExact(0));
        assertEquals(value, asDoubles.doubleValue(), 0);

        values = new DummyValues(valueBits);
        asMultiDoubles = FieldData.sortableLongBitsToDoubles(DocValues.singleton(values));
        NumericDocValues backToLongs = DocValues.unwrapSingleton(FieldData.toSortableLongBits(asMultiDoubles));
        assertSame(values, backToLongs);

        SortedNumericDocValues multiValues = new AbstractSortedNumericDocValues() {

            @Override
            public boolean advanceExact(int target) throws IOException {
                return true;
            }

            @Override
            public long nextValue() {
                return valueBits;
            }

            @Override
            public int docValueCount() {
                return 1;
            }
        };

        asMultiDoubles = FieldData.sortableLongBitsToDoubles(multiValues);
        assertEquals(value, asMultiDoubles.nextValue(), 0);
        assertSame(multiValues, FieldData.toSortableLongBits(asMultiDoubles));
    }

    public void testDoublesToSortableLongBits() throws IOException {
        final double value = randomDouble();
        final long valueBits = NumericUtils.doubleToSortableLong(value);

        NumericDoubleValues values = new NumericDoubleValues() {
            @Override
            public boolean advanceExact(int doc) throws IOException {
                return true;
            }
            @Override
            public double doubleValue() {
                return value;
            }
        };

        SortedNumericDocValues asMultiLongs = FieldData.toSortableLongBits(FieldData.singleton(values));
        NumericDocValues asLongs = DocValues.unwrapSingleton(asMultiLongs);
        assertNotNull(asLongs);
        assertTrue(asLongs.advanceExact(0));
        assertEquals(valueBits, asLongs.longValue());

        SortedNumericDoubleValues multiValues = new SortedNumericDoubleValues() {
            @Override
            public double nextValue() {
                return value;
            }

            @Override
            public boolean advanceExact(int target) throws IOException {
                return true;
            }

            @Override
            public int docValueCount() {
                return 1;
            }
        };

        asMultiLongs = FieldData.toSortableLongBits(multiValues);
        assertEquals(valueBits, asMultiLongs.nextValue());
        assertSame(multiValues, FieldData.sortableLongBitsToDoubles(asMultiLongs));
    }
}
