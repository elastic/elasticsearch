/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.DocLookup;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xpack.runtimefields.LongScriptFieldScript.Factory;

import java.io.IOException;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class LongScriptFieldScriptTests extends ScriptFieldScriptTestCase<
    LongScriptFieldScript.Factory,
    LongRuntimeValues,
    SortedNumericDocValues,
    Long> {

    private final Set<Integer> visited = new LinkedHashSet<>();

    public void testConstant() throws IOException {
        assertThat(randomLongs().collect("value(10)"), equalTo(List.of(10L, 10L)));
    }

    public void testTwoConstants() throws IOException {
        assertThat(randomLongs().collect("value(10); value(20)"), equalTo(List.of(10L, 20L, 10L, 20L)));
    }

    public void testSource() throws IOException {
        assertThat(singleValueInSource().collect("value(source['foo'] * 10)"), equalTo(List.of(10L, 100L)));
    }

    public void testMultileSourceValues() throws IOException {
        assertThat(multiValueInSource().collect("for (long l : source['foo']){value(l * 10)}"), equalTo(List.of(10L, 20L, 100L, 200L)));
    }

    public void testDocValues() throws IOException {
        assertThat(singleValueInDocValues().collect("value(doc['foo'].value * 10)"), equalTo(List.of(10L, 100L)));
    }

    public void testMultipleDocValuesValues() throws IOException {
        TestCase c = multipleValuesInDocValues();
        assertThat(multipleValuesInDocValues().collect(c.testScript("times_ten")), equalTo(List.of(10L, 20L, 100L, 200L)));
    }

    public void testTermQuery() throws IOException {
        TestCase c = multipleValuesInDocValues();
        LongRuntimeValues timesTen = c.testScript("times_ten");
        assertThat(c.collect(timesTen.termQuery("foo", 1), timesTen), equalTo(List.of()));
        visited.clear();
        assertThat(c.collect(timesTen.termQuery("foo", 10), timesTen), equalTo(List.of(10L, 20L)));
        visited.clear();
        assertThat(c.collect(timesTen.termQuery("foo", 20), timesTen), equalTo(List.of(10L, 20L)));
        visited.clear();
        assertThat(c.collect(timesTen.termQuery("foo", 100), timesTen), equalTo(List.of(100L, 200L)));
    }

    public void testTermsQuery() throws IOException {
        TestCase c = multipleValuesInDocValues();
        LongRuntimeValues timesTen = c.testScript("times_ten");
        assertThat(c.collect(timesTen.termsQuery("foo", 1, 2), timesTen), equalTo(List.of()));
        visited.clear();
        assertThat(c.collect(timesTen.termsQuery("foo", 10, 11), timesTen), equalTo(List.of(10L, 20L)));
        visited.clear();
        assertThat(c.collect(timesTen.termsQuery("foo", 20, 21), timesTen), equalTo(List.of(10L, 20L)));
        visited.clear();
        assertThat(c.collect(timesTen.termsQuery("foo", 19, 20), timesTen), equalTo(List.of(10L, 20L)));
        visited.clear();
        assertThat(c.collect(timesTen.termsQuery("foo", 100, 11), timesTen), equalTo(List.of(100L, 200L)));
    }

    public void testRangeQuery() throws IOException {
        TestCase c = multipleValuesInDocValues();
        LongRuntimeValues timesTen = c.testScript("times_ten");
        assertThat(c.collect(timesTen.rangeQuery("foo", 1, 2), timesTen), equalTo(List.of()));
        visited.clear();
        assertThat(c.collect(timesTen.rangeQuery("foo", 9, 11), timesTen), equalTo(List.of(10L, 20L)));
        visited.clear();
        assertThat(c.collect(timesTen.rangeQuery("foo", 10, 11), timesTen), equalTo(List.of(10L, 20L)));
        visited.clear();
        assertThat(c.collect(timesTen.rangeQuery("foo", 19, 21), timesTen), equalTo(List.of(10L, 20L)));
        visited.clear();
        assertThat(c.collect(timesTen.rangeQuery("foo", 99, 101), timesTen), equalTo(List.of(100L, 200L)));
    }

    private TestCase randomLongs() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", randomLong())));
        });
    }

    private TestCase singleValueInSource() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 1}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 10}"))));
        });
    }

    private TestCase multiValueInSource() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1, 2]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [10, 20]}"))));
        });
    }

    private TestCase singleValueInDocValues() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 1)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 10)));
        });
    }

    private TestCase multipleValuesInDocValues() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 1), new SortedNumericDocValuesField("foo", 2)));
            iw.addDocument(List.of(new SortedNumericDocValuesField("foo", 10), new SortedNumericDocValuesField("foo", 20)));
        });
    }

    @Override
    protected MappedFieldType[] fieldTypes() {
        return new MappedFieldType[] { new NumberFieldType("foo", NumberType.LONG) };
    }

    @Override
    protected ScriptContext<LongScriptFieldScript.Factory> scriptContext() {
        return LongScriptFieldScript.CONTEXT;
    }

    @Override
    protected LongRuntimeValues newValues(Factory factory, Map<String, Object> params, SourceLookup source, DocLookup fieldData)
        throws IOException {
        return factory.newFactory(params, source, fieldData).runtimeValues();
    }

    @Override
    protected CheckedFunction<LeafReaderContext, SortedNumericDocValues, IOException> docValuesBuilder(LongRuntimeValues values) {
        return values.docValues();
    }

    @Override
    protected void readAllDocValues(SortedNumericDocValues docValues, int docId, Consumer<Long> sync) throws IOException {
        assertTrue(docValues.advanceExact(docId));
        int count = docValues.docValueCount();
        for (int i = 0; i < count; i++) {
            sync.accept(docValues.nextValue());
        }
    }

    @Override
    protected List<ScriptPlugin> extraScriptPlugins() {
        return List.of(new ScriptPlugin() {
            @Override
            public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
                return new ScriptEngine() {
                    @Override
                    public String getType() {
                        return "test";
                    }

                    @Override
                    public Set<ScriptContext<?>> getSupportedContexts() {
                        return Set.of(LongScriptFieldScript.CONTEXT);
                    }

                    @Override
                    public <FactoryType> FactoryType compile(
                        String name,
                        String code,
                        ScriptContext<FactoryType> context,
                        Map<String, String> params
                    ) {
                        assert context == LongScriptFieldScript.CONTEXT;
                        @SuppressWarnings("unchecked")
                        FactoryType result = (FactoryType) compile(name);
                        return result;
                    }

                    private LongScriptFieldScript.Factory compile(String name) {
                        if (name.equals("times_ten")) {
                            return (params, source, fieldData) -> {
                                LongScriptFieldScript.LeafFactory leafFactory = (ctx, sync) -> {
                                    return new LongScriptFieldScript(params, source, fieldData, ctx, sync) {
                                        @Override
                                        protected void onSetDocument(int docId) {
                                            int rebased = ctx.docBase + docId;
                                            if (false == visited.add(rebased)) {
                                                throw new AssertionError("Visited [" + rebased + "] twice. Order before was " + visited);
                                            }
                                        }

                                        @Override
                                        public void execute() {
                                            for (Object v : getDoc().get("foo")) {
                                                sync.accept(((long) v) * 10);
                                            }
                                        }
                                    };
                                };
                                return leafFactory;
                            };
                        }
                        throw new IllegalArgumentException();
                    }
                };
            }
        });
    }
}
