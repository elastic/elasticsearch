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
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.painless.PainlessPlugin;
import org.elasticsearch.plugins.ExtensiblePlugin.ExtensionLoader;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.xpack.runtimefields.DoubleScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.RuntimeFields;
import org.elasticsearch.xpack.runtimefields.RuntimeFieldsPainlessExtension;
import org.elasticsearch.xpack.runtimefields.fielddata.ScriptDoubleFieldData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;

public class ScriptDoubleMappedFieldTypeTests extends AbstractNonTextScriptMappedFieldTypeTestCase {
    @Override
    public void testDocValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1.0]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [3.14, 1.4]}"))));
            List<Double> results = new ArrayList<>();
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                ScriptDoubleMappedFieldType ft = build("for (def v : source.foo) {value(v + params.param)}", Map.of("param", 1));
                ScriptDoubleFieldData ifd = ft.fielddataBuilder("test").build(null, null, null);
                ifd.setSearchLookup(mockContext().lookup());
                searcher.search(new MatchAllDocsQuery(), new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                        SortedNumericDoubleValues dv = ifd.load(context).getDoubleValues();
                        return new LeafCollector() {
                            @Override
                            public void setScorer(Scorable scorer) throws IOException {}

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
                ScriptDoubleMappedFieldType ft = build("for (def v : source.foo) { value(v)}");
                ScriptDoubleFieldData ifd = ft.fielddataBuilder("test").build(null, null, null);
                ifd.setSearchLookup(mockContext().lookup());
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
                QueryShardContext qsc = mockContext(true, build("for (def v : source.foo) {value(v)}"));
                assertThat(searcher.count(new ScriptScoreQuery(new MatchAllDocsQuery(), new Script("test"), new ScoreScript.LeafFactory() {
                    @Override
                    public boolean needs_score() {
                        return false;
                    }

                    @Override
                    public ScoreScript newInstance(LeafReaderContext ctx) throws IOException {
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
                assertThat(searcher.count(build("for (def v : source.foo) {value(v)}").existsQuery(mockContext())), equalTo(1));
            }
        }
    }

    @Override
    public void testExistsQueryIsExpensive() throws IOException {
        checkExpensiveQuery(ScriptDoubleMappedFieldType::existsQuery);
    }

    public void testRangeQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 1}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 2}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 2.5}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(
                    searcher.count(build("value(source.foo)").rangeQuery("2", "3", true, true, null, null, null, mockContext())),
                    equalTo(2)
                );
                assertThat(
                    searcher.count(build("value(source.foo)").rangeQuery(2, 3, true, true, null, null, null, mockContext())),
                    equalTo(2)
                );
                assertThat(
                    searcher.count(build("value(source.foo)").rangeQuery(1.1, 3, true, true, null, null, null, mockContext())),
                    equalTo(2)
                );
                assertThat(
                    searcher.count(build("value(source.foo)").rangeQuery(1.1, 3, false, true, null, null, null, mockContext())),
                    equalTo(2)
                );
                assertThat(
                    searcher.count(build("value(source.foo)").rangeQuery(2, 3, false, true, null, null, null, mockContext())),
                    equalTo(1)
                );
                assertThat(
                    searcher.count(build("value(source.foo)").rangeQuery(2.5, 3, true, true, null, null, null, mockContext())),
                    equalTo(1)
                );
                assertThat(
                    searcher.count(build("value(source.foo)").rangeQuery(2.5, 3, false, true, null, null, null, mockContext())),
                    equalTo(0)
                );
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
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 1}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 2}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(build("value(source.foo)").termQuery("1", mockContext())), equalTo(1));
                assertThat(searcher.count(build("value(source.foo)").termQuery(1, mockContext())), equalTo(1));
                assertThat(searcher.count(build("value(source.foo)").termQuery(1.1, mockContext())), equalTo(0));
                assertThat(
                    searcher.count(build("value(source.foo + params.param)", Map.of("param", 1)).termQuery(2, mockContext())),
                    equalTo(1)
                );
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
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 1}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": 2.1}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(build("value(source.foo)").termsQuery(List.of("1"), mockContext())), equalTo(1));
                assertThat(searcher.count(build("value(source.foo)").termsQuery(List.of(1), mockContext())), equalTo(1));
                assertThat(searcher.count(build("value(source.foo)").termsQuery(List.of(1.1), mockContext())), equalTo(0));
                assertThat(searcher.count(build("value(source.foo)").termsQuery(List.of(1.1, 2.1), mockContext())), equalTo(1));
                assertThat(searcher.count(build("value(source.foo)").termsQuery(List.of(2.1, 1), mockContext())), equalTo(2));
            }
        }
    }

    @Override
    public void testTermsQueryIsExpensive() throws IOException {
        checkExpensiveQuery((ft, ctx) -> ft.termsQuery(List.of(randomLong()), ctx));
    }

    @Override
    protected ScriptDoubleMappedFieldType simpleMappedFieldType() throws IOException {
        return build("value(source.foo)");
    }

    @Override
    protected String runtimeType() {
        return "double";
    }

    private static ScriptDoubleMappedFieldType build(String code) throws IOException {
        return build(new Script(code));
    }

    private static ScriptDoubleMappedFieldType build(String code, Map<String, Object> params) throws IOException {
        return build(new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, code, params));
    }

    private static ScriptDoubleMappedFieldType build(Script script) throws IOException {
        PainlessPlugin painlessPlugin = new PainlessPlugin();
        painlessPlugin.loadExtensions(new ExtensionLoader() {
            @Override
            @SuppressWarnings("unchecked") // We only ever load painless extensions here so it is fairly safe.
            public <T> List<T> loadExtensions(Class<T> extensionPointType) {
                return (List<T>) List.of(new RuntimeFieldsPainlessExtension());
            }
        });
        ScriptModule scriptModule = new ScriptModule(Settings.EMPTY, List.of(painlessPlugin, new RuntimeFields()));
        try (ScriptService scriptService = new ScriptService(Settings.EMPTY, scriptModule.engines, scriptModule.contexts)) {
            DoubleScriptFieldScript.Factory factory = scriptService.compile(script, DoubleScriptFieldScript.CONTEXT);
            return new ScriptDoubleMappedFieldType("test", script, factory, emptyMap());
        }
    }

    private static void checkExpensiveQuery(BiConsumer<ScriptDoubleMappedFieldType, QueryShardContext> queryBuilder) throws IOException {
        ScriptDoubleMappedFieldType ft = build("value(1)");
        Exception e = expectThrows(ElasticsearchException.class, () -> queryBuilder.accept(ft, mockContext(false)));
        assertThat(
            e.getMessage(),
            equalTo("queries cannot be executed against [script] fields while [search.allow_expensive_queries] is set to [false].")
        );
    }
}
