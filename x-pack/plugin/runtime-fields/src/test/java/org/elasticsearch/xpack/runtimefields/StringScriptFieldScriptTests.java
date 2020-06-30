/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields;

import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.mapper.KeywordFieldMapper.KeywordFieldType;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.search.lookup.DocLookup;
import org.elasticsearch.search.lookup.SourceLookup;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript.Factory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;

public class StringScriptFieldScriptTests extends ScriptFieldScriptTestCase<
    StringScriptFieldScript.Factory,
    StringRuntimeValues,
    SortedBinaryDocValues,
    String> {

    private final Set<Integer> visited = new LinkedHashSet<>();

    public void testConstant() throws IOException {
        assertThat(randomStrings().collect("value('cat')"), equalTo(List.of("cat", "cat")));
    }

    public void testTwoConstants() throws IOException {
        assertThat(randomStrings().collect("value('cat'); value('dog')"), equalTo(List.of("cat", "dog", "cat", "dog")));
    }

    public void testSource() throws IOException {
        assertThat(singleValueInSource().collect("value(source['foo'] + 'o')"), equalTo(List.of("cato", "dogo")));
    }

    public void testMultipleSourceValues() throws IOException {
        assertThat(
            multiValueInSource().collect("for (String v : source['foo']) {value(v + 'o')}"),
            equalTo(List.of("cato", "chickeno", "dogo", "pigo"))
        );
    }

    public void testDocValues() throws IOException {
        assertThat(singleValueInDocValues().collect(ADD_O), equalTo(List.of("cato", "dogo")));
    }

    public void testMultipleDocValuesValues() throws IOException {
        assertThat(multipleValuesInDocValues().collect(ADD_O), equalTo(List.of("cato", "pigo", "chickeno", "dogo")));
    }

    public void testTermQuery() throws IOException {
        TestCase c = multipleValuesInDocValues();
        StringRuntimeValues addO = c.testScript("add_o");
        assertThat(c.collect(addO.termQuery("foo", "cat"), addO), equalTo(List.of()));
        visited.clear();
        assertThat(c.collect(addO.termQuery("foo", "cato"), addO), equalTo(List.of("cato", "pigo")));
        visited.clear();
        assertThat(c.collect(addO.termQuery("foo", "pigo"), addO), equalTo(List.of("cato", "pigo")));
        visited.clear();
        assertThat(c.collect(addO.termQuery("foo", "dogo"), addO), equalTo(List.of("chickeno", "dogo")));
    }

    public void testPrefixQuery() throws IOException {
        TestCase c = multipleValuesInDocValues();
        StringRuntimeValues addO = c.testScript("add_o");
        assertThat(c.collect(addO.prefixQuery("foo", "catdog"), addO), equalTo(List.of()));
        visited.clear();
        assertThat(c.collect(addO.prefixQuery("foo", "cat"), addO), equalTo(List.of("cato", "pigo")));
        visited.clear();
        assertThat(c.collect(addO.prefixQuery("foo", "pig"), addO), equalTo(List.of("cato", "pigo")));
        visited.clear();
        assertThat(c.collect(addO.prefixQuery("foo", "dogo"), addO), equalTo(List.of("chickeno", "dogo")));
        visited.clear();
        assertThat(c.collect(addO.prefixQuery("foo", "d"), addO), equalTo(List.of("chickeno", "dogo")));
    }

    public void testRangeQuery() throws IOException {
        TestCase c = multipleValuesInDocValues();
        StringRuntimeValues addO = c.testScript("add_o");
        assertThat(c.collect(addO.rangeQuery("foo", "catz", "cbat"), addO), equalTo(List.of()));
        visited.clear();
        assertThat(c.collect(addO.rangeQuery("foo", "c", "cb"), addO), equalTo(List.of("cato", "pigo")));
        visited.clear();
        assertThat(c.collect(addO.rangeQuery("foo", "p", "q"), addO), equalTo(List.of("cato", "pigo")));
        visited.clear();
        assertThat(c.collect(addO.rangeQuery("foo", "doggie", "dogs"), addO), equalTo(List.of("chickeno", "dogo")));
    }

    private TestCase randomStrings() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef(randomAlphaOfLength(2)))));
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef(randomAlphaOfLength(2)))));
        });
    }

    private TestCase singleValueInSource() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": \"cat\"}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": \"dog\"}"))));
        });
    }

    private TestCase multiValueInSource() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cat\", \"chicken\"]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"dog\", \"pig\"]}"))));
        });
    }

    private TestCase singleValueInDocValues() throws IOException {
        return testCase(iw -> {
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef("cat"))));
            iw.addDocument(List.of(new SortedSetDocValuesField("foo", new BytesRef("dog"))));
        });
    }

    private TestCase multipleValuesInDocValues() throws IOException {
        return testCase(iw -> {
            List<IndexableField> doc = new ArrayList<>();
            doc.add(new SortedSetDocValuesField("foo", new BytesRef("cat")));
            doc.add(new SortedSetDocValuesField("foo", new BytesRef("pig")));
            iw.addDocument(doc);
            doc.clear();
            doc.add(new SortedSetDocValuesField("foo", new BytesRef("chicken")));
            doc.add(new SortedSetDocValuesField("foo", new BytesRef("dog")));
            iw.addDocument(doc);
        });
    }

    private static final String ADD_O = "for (String s: doc['foo']) {value(s + 'o')}";

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
                        return Set.of(StringScriptFieldScript.CONTEXT);
                    }

                    @Override
                    public <FactoryType> FactoryType compile(
                        String name,
                        String code,
                        ScriptContext<FactoryType> context,
                        Map<String, String> params
                    ) {
                        assert context == StringScriptFieldScript.CONTEXT;
                        @SuppressWarnings("unchecked")
                        FactoryType result = (FactoryType) compile(name);
                        return result;
                    }

                    private StringScriptFieldScript.Factory compile(String name) {
                        if (name.equals("add_o")) {
                            return (params, source, fieldData) -> {
                                StringScriptFieldScript.LeafFactory leafFactory = (ctx, sync) -> {
                                    return new StringScriptFieldScript(params, source, fieldData, ctx, sync) {
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
                                                sync.accept(v + "o");
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

    @Override
    protected MappedFieldType[] fieldTypes() {
        return new MappedFieldType[] { new KeywordFieldType("foo") };
    }

    @Override
    protected ScriptContext<StringScriptFieldScript.Factory> scriptContext() {
        return StringScriptFieldScript.CONTEXT;
    }

    @Override
    protected StringRuntimeValues newValues(Factory factory, Map<String, Object> params, SourceLookup source, DocLookup fieldData)
        throws IOException {
        return factory.newFactory(params, source, fieldData).runtimeValues();
    }

    @Override
    protected CheckedFunction<LeafReaderContext, SortedBinaryDocValues, IOException> docValuesBuilder(StringRuntimeValues values) {
        return values.docValues();
    }

    @Override
    protected void readAllDocValues(SortedBinaryDocValues docValues, int docId, Consumer<String> sync) throws IOException {
        assertTrue(docValues.advanceExact(docId));
        int count = docValues.docValueCount();
        for (int i = 0; i < count; i++) {
            sync.accept(docValues.nextValue().utf8ToString());
        }
    }
}
