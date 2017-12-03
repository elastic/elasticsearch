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

import com.carrotsearch.randomizedtesting.generators.RandomStrings;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.aggregations.support.values.ScriptBytesValues;
import org.elasticsearch.search.aggregations.support.values.ScriptDoubleValues;
import org.elasticsearch.search.aggregations.support.values.ScriptLongValues;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public class ScriptValuesTests extends ESTestCase {

    private static class FakeSearchScript extends SearchScript {

        private final Object[][] values;
        int index;

        FakeSearchScript(Object[][] values) {
            super(null, null, null);
            this.values = values;
            index = -1;
        }

        @Override
        public void setNextVar(String name, Object value) {
        }

        @Override
        public Object run() {
            // Script values are supposed to support null, single values, arrays and collections
            final Object[] values = this.values[index];
            if (values.length <= 1 && randomBoolean()) {
                return values.length == 0 ? null : values[0];
            }
            return randomBoolean() ? values : Arrays.asList(values);
        }

        @Override
        public void setScorer(Scorer scorer) {
        }

        @Override
        public void setDocument(int doc) {
            index = doc;
        }

        @Override
        public long runAsLong() {
            throw new UnsupportedOperationException();
        }

        @Override
        public double runAsDouble() {
            throw new UnsupportedOperationException();
        }

    }

    public void testLongs() throws IOException {
        final Object[][] values = new Long[randomInt(10)][];
        for (int i = 0; i < values.length; ++i) {
            Long[] longs = new Long[randomInt(8)];
            for (int j = 0; j < longs.length; ++j) {
                longs[j] = randomLong();
            }
            Arrays.sort(longs);
            values[i] = longs;
        }
        FakeSearchScript script = new FakeSearchScript(values);
        ScriptLongValues scriptValues = new ScriptLongValues(script);
        for (int i = 0; i < values.length; ++i) {
            assertEquals(values[i].length > 0, scriptValues.advanceExact(i));
            if (values[i].length > 0) {
                assertEquals(values[i].length, scriptValues.docValueCount());
                for (int j = 0; j < values[i].length; ++j) {
                    assertEquals(values[i][j], scriptValues.nextValue());
                }
            }
        }
    }

    public void testBooleans() throws IOException {
        final Object[][] values = new Boolean[randomInt(10)][];
        for (int i = 0; i < values.length; ++i) {
            Boolean[] booleans = new Boolean[randomInt(8)];
            for (int j = 0; j < booleans.length; ++j) {
                booleans[j] = randomBoolean();
            }
            Arrays.sort(booleans);
            values[i] = booleans;
        }
        FakeSearchScript script = new FakeSearchScript(values);
        ScriptLongValues scriptValues = new ScriptLongValues(script);
        for (int i = 0; i < values.length; ++i) {
            assertEquals(values[i].length > 0, scriptValues.advanceExact(i));
            if (values[i].length > 0) {
                assertEquals(values[i].length, scriptValues.docValueCount());
                for (int j = 0; j < values[i].length; ++j) {
                    assertEquals(values[i][j], scriptValues.nextValue() == 1L);
                }
            }
        }
    }

    public void testDoubles() throws IOException {
        final Object[][] values = new Double[randomInt(10)][];
        for (int i = 0; i < values.length; ++i) {
            Double[] doubles = new Double[randomInt(8)];
            for (int j = 0; j < doubles.length; ++j) {
                doubles[j] = randomDouble();
            }
            Arrays.sort(doubles);
            values[i] = doubles;
        }
        FakeSearchScript script = new FakeSearchScript(values);
        ScriptDoubleValues scriptValues = new ScriptDoubleValues(script);
        for (int i = 0; i < values.length; ++i) {
            assertEquals(values[i].length > 0, scriptValues.advanceExact(i));
            if (values[i].length > 0) {
                assertEquals(values[i].length, scriptValues.docValueCount());
                for (int j = 0; j < values[i].length; ++j) {
                    assertEquals(values[i][j], scriptValues.nextValue());
                }
            }
        }
    }

    public void testBytes() throws IOException {
        final String[][] values = new String[randomInt(10)][];
        for (int i = 0; i < values.length; ++i) {
            String[] strings = new String[randomInt(8)];
            for (int j = 0; j < strings.length; ++j) {
                strings[j] = RandomStrings.randomAsciiOfLength(random(), 5);
            }
            Arrays.sort(strings);
            values[i] = strings;
        }
        FakeSearchScript script = new FakeSearchScript(values);
        ScriptBytesValues scriptValues = new ScriptBytesValues(script);
        for (int i = 0; i < values.length; ++i) {
            assertEquals(values[i].length > 0, scriptValues.advanceExact(i));
            if (values[i].length > 0) {
                assertEquals(values[i].length, scriptValues.docValueCount());
                for (int j = 0; j < values[i].length; ++j) {
                    assertEquals(new BytesRef(values[i][j]), scriptValues.nextValue());
                }
            }
        }
    }

}
