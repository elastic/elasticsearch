/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.support;

import com.carrotsearch.randomizedtesting.generators.RandomStrings;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.script.AggregationScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.support.values.ScriptBytesValues;
import org.elasticsearch.search.aggregations.support.values.ScriptDoubleValues;
import org.elasticsearch.search.aggregations.support.values.ScriptLongValues;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.lookup.LeafSearchLookup;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ScriptValuesTests extends ESTestCase {

    private static class FakeAggregationScript extends AggregationScript {

        private final Object[][] values;
        int index;

        FakeAggregationScript(Object[][] values) {
            super(Collections.emptyMap(), new SearchLookup(null, null, (ctx, doc) -> null) {

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

    public void testCancellationCheckWiredForAggregationScript() throws IOException {
        AtomicReference<AggregationScript> capturedScript = new AtomicReference<>();
        AggregationScript.LeafFactory capturingLeafFactory = new AggregationScript.LeafFactory() {
            @Override
            public AggregationScript newInstance(LeafReaderContext ctx) throws IOException {
                AggregationScript script = new AggregationScript() {
                    @Override
                    public Object execute() {
                        return null;
                    }
                };
                capturedScript.set(script);
                return script;
            }

            @Override
            public boolean needs_score() {
                return false;
            }
        };
        AggregationScript.Factory capturingFactory = (params, lookup) -> capturingLeafFactory;

        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                w.addDocument(new Document());
                w.commit();
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                ContextIndexSearcher contextSearcher = new ContextIndexSearcher(
                    reader,
                    IndexSearcher.getDefaultSimilarity(),
                    IndexSearcher.getDefaultQueryCache(),
                    IndexSearcher.getDefaultQueryCachingPolicy(),
                    true
                );
                AggregationContext mockContext = mock(AggregationContext.class);
                doReturn(capturingFactory).when(mockContext).compile(any(Script.class), same(AggregationScript.CONTEXT));
                when(mockContext.lookup()).thenReturn(new SearchLookup(null, null, (ctx, doc) -> null));
                when(mockContext.searcher()).thenReturn(contextSearcher);
                when(mockContext.nowInMillis()).thenReturn(0L);

                ValuesSourceConfig config = ValuesSourceConfig.resolveUnregistered(
                    mockContext,
                    ValueType.NUMERIC,
                    null,
                    new Script("test"),
                    null,
                    null,
                    null,
                    CoreValuesSourceType.NUMERIC
                );
                config.script().newInstance(null);
                assertNotNull(capturedScript.get()._getCancellationCheck());

                capturedScript.set(null);
                AggregationContext plainMockContext = mock(AggregationContext.class);
                doReturn(capturingFactory).when(plainMockContext).compile(any(Script.class), same(AggregationScript.CONTEXT));
                when(plainMockContext.lookup()).thenReturn(new SearchLookup(null, null, (ctx, doc) -> null));
                when(plainMockContext.searcher()).thenReturn(newSearcher(reader));
                when(plainMockContext.nowInMillis()).thenReturn(0L);

                ValuesSourceConfig plainConfig = ValuesSourceConfig.resolveUnregistered(
                    plainMockContext,
                    ValueType.NUMERIC,
                    null,
                    new Script("test"),
                    null,
                    null,
                    null,
                    CoreValuesSourceType.NUMERIC
                );
                plainConfig.script().newInstance(null);
                assertNull(capturedScript.get()._getCancellationCheck());
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
