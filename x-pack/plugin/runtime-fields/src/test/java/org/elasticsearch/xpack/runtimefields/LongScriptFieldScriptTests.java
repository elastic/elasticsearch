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
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberFieldType;
import org.elasticsearch.index.mapper.NumberFieldMapper.NumberType;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.xpack.runtimefields.LongScriptFieldScript.Factory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static org.hamcrest.Matchers.equalTo;

public class LongScriptFieldScriptTests extends ScriptFieldScriptTestCase<
    LongScriptFieldScript.Factory,
    LongRuntimeFieldHelper,
    SortedNumericDocValues,
    Long> {

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

    public void testExistsQuery() throws IOException {
        TestCase c = multipleValuesInDocValues();
        LongRuntimeFieldHelper isOne = c.testScript("is_one");
        assertThat(c.collect(isOne.existsQuery("foo"), isOne), equalTo(List.of(1L)));
    }

    public void testTermQuery() throws IOException {
        TestCase c = multipleValuesInDocValues();
        LongRuntimeFieldHelper timesTen = c.testScript("times_ten");
        assertThat(c.collect(timesTen.termQuery("foo", 1), timesTen), equalTo(List.of()));
        assertThat(c.collect(timesTen.termQuery("foo", 10), timesTen), equalTo(List.of(10L, 20L)));
        assertThat(c.collect(timesTen.termQuery("foo", 20), timesTen), equalTo(List.of(10L, 20L)));
        assertThat(c.collect(timesTen.termQuery("foo", 100), timesTen), equalTo(List.of(100L, 200L)));
    }

    public void testTermsQuery() throws IOException {
        TestCase c = multipleValuesInDocValues();
        LongRuntimeFieldHelper timesTen = c.testScript("times_ten");
        assertThat(c.collect(timesTen.termsQuery("foo", 1, 2), timesTen), equalTo(List.of()));
        assertThat(c.collect(timesTen.termsQuery("foo", 10, 11), timesTen), equalTo(List.of(10L, 20L)));
        assertThat(c.collect(timesTen.termsQuery("foo", 20, 21), timesTen), equalTo(List.of(10L, 20L)));
        assertThat(c.collect(timesTen.termsQuery("foo", 19, 20), timesTen), equalTo(List.of(10L, 20L)));
        assertThat(c.collect(timesTen.termsQuery("foo", 100, 11), timesTen), equalTo(List.of(100L, 200L)));
    }

    public void testRangeQuery() throws IOException {
        TestCase c = multipleValuesInDocValues();
        LongRuntimeFieldHelper timesTen = c.testScript("times_ten");
        assertThat(c.collect(timesTen.rangeQuery("foo", 1, 2), timesTen), equalTo(List.of()));
        assertThat(c.collect(timesTen.rangeQuery("foo", 9, 11), timesTen), equalTo(List.of(10L, 20L)));
        assertThat(c.collect(timesTen.rangeQuery("foo", 10, 11), timesTen), equalTo(List.of(10L, 20L)));
        assertThat(c.collect(timesTen.rangeQuery("foo", 19, 21), timesTen), equalTo(List.of(10L, 20L)));
        assertThat(c.collect(timesTen.rangeQuery("foo", 99, 101), timesTen), equalTo(List.of(100L, 200L)));
    }

    public void testInsideBoolTermQuery() throws IOException {
        /*
         * Its required that bool queries that contain more our runtime fields queries
         * be wrapped in ForceNoBulkScoringQuery. Exactly what queries in the tree need
         * to be wrapped and when isn't super clear but it is safest to wrap the whole
         * query tree when there are *any* of these queries in it. We might be able to
         * skip some of them eventually, when we're more comfortable with this.
         */
        TestCase c = multipleValuesInDocValues();
        LongRuntimeFieldHelper timesTen = c.testScript("times_ten");
        assertThat(
            c.collect(
                new ForceNoBulkScoringQuery(
                    new BooleanQuery.Builder().add(timesTen.termQuery("foo", 1), Occur.SHOULD)
                        .add(timesTen.termQuery("foo", 10), Occur.SHOULD)
                        .add(timesTen.termQuery("foo", 100), Occur.SHOULD)
                        .build()
                ),
                timesTen
            ),
            equalTo(List.of(10L, 20L, 100L, 200L))
        );
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
    protected LongRuntimeFieldHelper newHelper(Factory factory, Map<String, Object> params, SearchLookup searchLookup) throws IOException {
        return factory.newFactory(params, searchLookup).runtimeFieldHelper();
    }

    @Override
    protected CheckedFunction<LeafReaderContext, SortedNumericDocValues, IOException> docValuesBuilder(LongRuntimeFieldHelper values) {
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
                            return assertingScript((fieldData, sync) -> {
                                for (Object v : fieldData.get("foo")) {
                                    sync.accept(((long) v) * 10);
                                }
                            });
                        }
                        if (name.equals("is_one")) {
                            return assertingScript((fieldData, sync) -> {
                                for (Object v : fieldData.get("foo")) {
                                    long l = (long) v;
                                    if (l == 1) {
                                        sync.accept(1);
                                    }
                                }
                            });
                        }
                        throw new IllegalArgumentException();
                    }
                };
            }
        });
    }

    private LongScriptFieldScript.Factory assertingScript(BiConsumer<Map<String, ScriptDocValues<?>>, LongConsumer> impl) {
        return (params, searchLookup) -> {
            LongScriptFieldScript.LeafFactory leafFactory = (ctx, sync) -> {
                return new LongScriptFieldScript(params, searchLookup, ctx, sync) {
                    @Override
                    public void execute() {
                        impl.accept(getDoc(), sync);
                    }
                };
            };
            return leafFactory;
        };
    }
}
