/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.plugins.ScriptPlugin;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.xpack.runtimefields.DoubleScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.RuntimeFields;
import org.elasticsearch.xpack.runtimefields.fielddata.ScriptDoubleFieldData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;

public class ScriptDoubleMappedFieldTypeTests extends AbstractNonTextScriptMappedFieldTypeTestCase {
    public void testFormat() throws IOException {
        assertThat(simpleMappedFieldType().docValueFormat("#.0", null).format(1), equalTo("1.0"));
        assertThat(simpleMappedFieldType().docValueFormat("#.0", null).format(1.2), equalTo("1.2"));
        assertThat(simpleMappedFieldType().docValueFormat("#,##0.##", null).format(11), equalTo("11"));
        assertThat(simpleMappedFieldType().docValueFormat("#,##0.##", null).format(1123), equalTo("1,123"));
        assertThat(simpleMappedFieldType().docValueFormat("#,##0.00", null).format(1123), equalTo("1,123.00"));
        assertThat(simpleMappedFieldType().docValueFormat("#,##0.00", null).format(1123.1), equalTo("1,123.10"));
    }

    @Override
    public void testDocValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1.0]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [3.14, 1.4]}"))));
            List<Double> results = new ArrayList<>();
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                ScriptDoubleMappedFieldType ft = build("add_param", Map.of("param", 1));
                ScriptDoubleFieldData ifd = ft.fielddataBuilder("test", mockContext()::lookup).build(null, null, null);
                searcher.search(new MatchAllDocsQuery(), new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafCollector getLeafCollector(LeafReaderContext context) {
                        SortedNumericDoubleValues dv = ifd.load(context).getDoubleValues();
                        return new LeafCollector() {
                            @Override
                            public void setScorer(Scorable scorer) {}

                            @Override
                            public void collect(int doc) throws IOException {
                                if (dv.advanceExact(doc)) {
                                    for (int i = 0; i < dv.docValueCount(); i++) {
                                        results.add(dv.nextValue());
                                    }
                                }
                            }
                        };
                    }
                });
                assertThat(results, equalTo(List.of(2.0, 2.4, 4.140000000000001)));
            }
        }
    }

    @Override
    public void testSort() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1.1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [4.2]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2.1]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                ScriptDoubleFieldData ifd = simpleMappedFieldType().fielddataBuilder("test", mockContext()::lookup).build(null, null, null);
                SortField sf = ifd.sortField(null, MultiValueMode.MIN, null, false);
                TopFieldDocs docs = searcher.search(new MatchAllDocsQuery(), 3, new Sort(sf));
                assertThat(reader.document(docs.scoreDocs[0].doc).getBinaryValue("_source").utf8ToString(), equalTo("{\"foo\": [1.1]}"));
                assertThat(reader.document(docs.scoreDocs[1].doc).getBinaryValue("_source").utf8ToString(), equalTo("{\"foo\": [2.1]}"));
                assertThat(reader.document(docs.scoreDocs[2].doc).getBinaryValue("_source").utf8ToString(), equalTo("{\"foo\": [4.2]}"));
            }
        }
    }

    @Override
    public void testUsedInScript() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1.1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [4.2]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2.1]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                QueryShardContext qsc = mockContext(true, simpleMappedFieldType());
                assertThat(searcher.count(new ScriptScoreQuery(new MatchAllDocsQuery(), new Script("test"), new ScoreScript.LeafFactory() {
                    @Override
                    public boolean needs_score() {
                        return false;
                    }

                    @Override
                    public ScoreScript newInstance(LeafReaderContext ctx) {
                        return new ScoreScript(Map.of(), qsc.lookup(), ctx) {
                            @Override
                            public double execute(ExplanationHolder explanation) {
                                ScriptDocValues.Doubles doubles = (ScriptDocValues.Doubles) getDoc().get("test");
                                return doubles.get(0);
                            }
                        };
                    }
                }, 2.5f, "test", 0, Version.CURRENT)), equalTo(1));
            }
        }
    }

    @Override
    public void testExistsQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": []}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().existsQuery(mockContext())), equalTo(1));
            }
        }
    }

    @Override
    public void testExistsQueryIsExpensive() throws IOException {
        checkExpensiveQuery(ScriptDoubleMappedFieldType::existsQuery);
    }

    public void testRangeQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2.5]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                MappedFieldType ft = simpleMappedFieldType();
                assertThat(searcher.count(ft.rangeQuery("2", "3", true, true, null, null, null, mockContext())), equalTo(2));
                assertThat(searcher.count(ft.rangeQuery(2, 3, true, true, null, null, null, mockContext())), equalTo(2));
                assertThat(searcher.count(ft.rangeQuery(1.1, 3, true, true, null, null, null, mockContext())), equalTo(2));
                assertThat(searcher.count(ft.rangeQuery(1.1, 3, false, true, null, null, null, mockContext())), equalTo(2));
                assertThat(searcher.count(ft.rangeQuery(2, 3, false, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(2.5, 3, true, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(2.5, 3, false, true, null, null, null, mockContext())), equalTo(0));
            }
        }
    }

    public void testRangeQueryIsExpensive() throws IOException {
        checkExpensiveQuery(
            (ft, ctx) -> ft.rangeQuery(randomLong(), randomLong(), randomBoolean(), randomBoolean(), null, null, null, ctx)
        );
    }

    @Override
    public void testTermQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().termQuery("1", mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termQuery(1, mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termQuery(1.1, mockContext())), equalTo(0));
                assertThat(searcher.count(build("add_param", Map.of("param", 1)).termQuery(2, mockContext())), equalTo(1));
            }
        }
    }

    @Override
    public void testTermQueryIsExpensive() throws IOException {
        checkExpensiveQuery((ft, ctx) -> ft.termQuery(randomLong(), ctx));
    }

    @Override
    public void testTermsQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2.1]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of("1"), mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(1), mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(1.1), mockContext())), equalTo(0));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(1.1, 2.1), mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(2.1, 1), mockContext())), equalTo(2));
            }
        }
    }

    @Override
    public void testTermsQueryIsExpensive() throws IOException {
        checkExpensiveQuery((ft, ctx) -> ft.termsQuery(List.of(randomLong()), ctx));
    }

    @Override
    protected ScriptDoubleMappedFieldType simpleMappedFieldType() throws IOException {
        return build("read_foo", Map.of());
    }

    @Override
    protected String runtimeType() {
        return "double";
    }

    private static ScriptDoubleMappedFieldType build(String code, Map<String, Object> params) throws IOException {
        return build(new Script(ScriptType.INLINE, "test", code, params));
    }

    private static ScriptDoubleMappedFieldType build(Script script) throws IOException {
        ScriptPlugin scriptPlugin = new ScriptPlugin() {
            @Override
            public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
                return new ScriptEngine() {
                    @Override
                    public String getType() {
                        return "test";
                    }

                    @Override
                    public Set<ScriptContext<?>> getSupportedContexts() {
                        return Set.of(DoubleScriptFieldScript.CONTEXT);
                    }

                    @Override
                    public <FactoryType> FactoryType compile(
                        String name,
                        String code,
                        ScriptContext<FactoryType> context,
                        Map<String, String> params
                    ) {
                        @SuppressWarnings("unchecked")
                        FactoryType factory = (FactoryType) factory(code);
                        return factory;
                    }

                    private DoubleScriptFieldScript.Factory factory(String code) {
                        switch (code) {
                            case "read_foo":
                                return (fieldName, params, lookup) -> (ctx) -> new DoubleScriptFieldScript(fieldName, params, lookup, ctx) {
                                    @Override
                                    public void execute() {
                                        for (Object foo : (List<?>) getSource().get("foo")) {
                                            emit(((Number) foo).doubleValue());
                                        }
                                    }
                                };
                            case "add_param":
                                return (fieldName, params, lookup) -> (ctx) -> new DoubleScriptFieldScript(fieldName, params, lookup, ctx) {
                                    @Override
                                    public void execute() {
                                        for (Object foo : (List<?>) getSource().get("foo")) {
                                            emit(((Number) foo).doubleValue() + ((Number) getParams().get("param")).doubleValue());
                                        }
                                    }
                                };
                            default:
                                throw new IllegalArgumentException("unsupported script [" + code + "]");
                        }
                    }
                };
            }
        };
        ScriptModule scriptModule = new ScriptModule(Settings.EMPTY, List.of(scriptPlugin, new RuntimeFields()));
        try (ScriptService scriptService = new ScriptService(Settings.EMPTY, scriptModule.engines, scriptModule.contexts)) {
            DoubleScriptFieldScript.Factory factory = scriptService.compile(script, DoubleScriptFieldScript.CONTEXT);
            return new ScriptDoubleMappedFieldType("test", script, factory, emptyMap());
        }
    }

    private void checkExpensiveQuery(BiConsumer<ScriptDoubleMappedFieldType, QueryShardContext> queryBuilder) throws IOException {
        ScriptDoubleMappedFieldType ft = simpleMappedFieldType();
        Exception e = expectThrows(ElasticsearchException.class, () -> queryBuilder.accept(ft, mockContext(false)));
        assertThat(
            e.getMessage(),
            equalTo("queries cannot be executed against [runtime] fields while [search.allow_expensive_queries] is set to [false].")
        );
    }
}
