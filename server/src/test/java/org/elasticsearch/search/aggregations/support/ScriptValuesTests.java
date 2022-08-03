/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.support;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.search.aggregations.support.values.ScriptBytesValues;
import org.elasticsearch.search.aggregations.support.values.ScriptDoubleValues;
import org.elasticsearch.search.aggregations.support.values.ScriptLongValues;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScriptValuesTests extends ESTestCase {

    private static class FakeAggregationScript extends AggregationScript {

        private final Object[][] values;
        int index;

        FakeAggregationScript(Object[][] values) {
            super(Collections.emptyMap(), new SearchLookup(null, null, new SourceLookup.ReaderSourceProvider()) {

                @Override
                public LeafSearchLookup getLeafSearchLookup(LeafReaderContext context) {
                    LeafSearchLookup leafSearchLookup = mock(LeafSearchLookup.class);
                    when(leafSearchLookup.asMap()).thenReturn(Collections.emptyMap());
                    return leafSearchLookup;
                }
            }, null);
            this.values = values;
            index = -1;
        }

        @Override
        public Object execute() {
            // Script values are supposed to support null, single values, arrays and collections
            final Object[] values = this.values[index];
            if (values.length <= 1 && randomBoolean()) {
                return values.length == 0 ? null : values[0];
            }
            return randomBoolean() ? values : Arrays.asList(values);
        }

        @Override
        public void setScorer(Scorable scorer) {}

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
        FakeAggregationScript script = new FakeAggregationScript(values);
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
        FakeAggregationScript script = new FakeAggregationScript(values);
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
        FakeAggregationScript script = new FakeAggregationScript(values);
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
        FakeAggregationScript script = new FakeAggregationScript(values);
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
