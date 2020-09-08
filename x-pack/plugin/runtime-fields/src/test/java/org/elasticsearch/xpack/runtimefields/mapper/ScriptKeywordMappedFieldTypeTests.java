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
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.query.MatchQueryBuilder;
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
import org.elasticsearch.xpack.runtimefields.RuntimeFields;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;
import org.elasticsearch.xpack.runtimefields.fielddata.ScriptBinaryFieldData;
import org.elasticsearch.xpack.runtimefields.fielddata.ScriptStringFieldData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;

public class ScriptKeywordMappedFieldTypeTests extends AbstractScriptMappedFieldTypeTestCase {
    @Override
    public void testDocValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2, 1]}"))));
            List<String> results = new ArrayList<>();
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                ScriptKeywordMappedFieldType ft = build("append_param", Map.of("param", "-suffix"));
                ScriptStringFieldData ifd = ft.fielddataBuilder("test", mockContext()::lookup).build(null, null, null);
                searcher.search(new MatchAllDocsQuery(), new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafCollector getLeafCollector(LeafReaderContext context) {
                        SortedBinaryDocValues dv = ifd.load(context).getBytesValues();
                        return new LeafCollector() {
                            @Override
                            public void setScorer(Scorable scorer) {}

                            @Override
                            public void collect(int doc) throws IOException {
                                if (dv.advanceExact(doc)) {
                                    for (int i = 0; i < dv.docValueCount(); i++) {
                                        results.add(dv.nextValue().utf8ToString());
                                    }
                                }
                            }
                        };
                    }
                });
                assertThat(results, equalTo(List.of("1-suffix", "1-suffix", "2-suffix")));
            }
        }
    }

    @Override
    public void testSort() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"a\"]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"d\"]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"b\"]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                ScriptBinaryFieldData ifd = simpleMappedFieldType().fielddataBuilder("test", mockContext()::lookup).build(null, null, null);
                SortField sf = ifd.sortField(null, MultiValueMode.MIN, null, false);
                TopFieldDocs docs = searcher.search(new MatchAllDocsQuery(), 3, new Sort(sf));
                assertThat(reader.document(docs.scoreDocs[0].doc).getBinaryValue("_source").utf8ToString(), equalTo("{\"foo\": [\"a\"]}"));
                assertThat(reader.document(docs.scoreDocs[1].doc).getBinaryValue("_source").utf8ToString(), equalTo("{\"foo\": [\"b\"]}"));
                assertThat(reader.document(docs.scoreDocs[2].doc).getBinaryValue("_source").utf8ToString(), equalTo("{\"foo\": [\"d\"]}"));
            }
        }
    }

    @Override
    public void testUsedInScript() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"a\"]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"aaa\"]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"aa\"]}"))));
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
                                ScriptDocValues.Strings bytes = (ScriptDocValues.Strings) getDoc().get("test");
                                return bytes.get(0).length();
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
        checkExpensiveQuery(ScriptKeywordMappedFieldType::existsQuery);
    }

    public void testFuzzyQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cat\"]}"))));   // No edits, matches
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"caat\"]}"))));  // Single insertion, matches
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cta\"]}"))));   // Single transposition, matches
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"caaat\"]}")))); // Two insertions, no match
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"dog\"]}"))));   // Totally wrong, no match
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(
                    searcher.count(simpleMappedFieldType().fuzzyQuery("cat", Fuzziness.AUTO, 0, 1, true, mockContext())),
                    equalTo(3)
                );
            }
        }
    }

    public void testFuzzyQueryIsExpensive() throws IOException {
        checkExpensiveQuery(
            (ft, ctx) -> ft.fuzzyQuery(
                randomAlphaOfLengthBetween(1, 1000),
                randomFrom(Fuzziness.AUTO, Fuzziness.ZERO, Fuzziness.ONE, Fuzziness.TWO),
                randomInt(),
                randomInt(),
                randomBoolean(),
                ctx
            )
        );
    }

    public void testPrefixQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cat\"]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cata\"]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"dog\"]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().prefixQuery("cat", null, mockContext())), equalTo(2));
            }
        }
    }

    public void testPrefixQueryIsExpensive() throws IOException {
        checkExpensiveQuery((ft, ctx) -> ft.prefixQuery(randomAlphaOfLengthBetween(1, 1000), null, ctx));
    }

    @Override
    public void testRangeQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cat\"]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cata\"]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"dog\"]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(
                    searcher.count(simpleMappedFieldType().rangeQuery("cat", "d", false, false, null, null, null, mockContext())),
                    equalTo(1)
                );
            }
        }
    }

    @Override
    public void testRangeQueryIsExpensive() throws IOException {
        checkExpensiveQuery(
            (ft, ctx) -> ft.rangeQuery(
                "a" + randomAlphaOfLengthBetween(0, 1000),
                "b" + randomAlphaOfLengthBetween(0, 1000),
                randomBoolean(),
                randomBoolean(),
                null,
                null,
                null,
                ctx
            )
        );
    }

    public void testRegexpQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cat\"]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cata\"]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"dog\"]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(
                    searcher.count(
                        simpleMappedFieldType().regexpQuery("ca.+", 0, Operations.DEFAULT_MAX_DETERMINIZED_STATES, null, mockContext())
                    ),
                    equalTo(2)
                );
            }
        }
    }

    public void testRegexpQueryIsExpensive() throws IOException {
        checkExpensiveQuery((ft, ctx) -> ft.regexpQuery(randomAlphaOfLengthBetween(1, 1000), randomInt(0xFFFF), randomInt(), null, ctx));
    }

    @Override
    public void testTermQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                ScriptKeywordMappedFieldType fieldType = build("append_param", Map.of("param", "-suffix"));
                assertThat(searcher.count(fieldType.termQuery("1-suffix", mockContext())), equalTo(1));
            }
        }
    }

    @Override
    public void testTermQueryIsExpensive() throws IOException {
        checkExpensiveQuery((ft, ctx) -> ft.termQuery(randomAlphaOfLengthBetween(1, 1000), ctx));
    }

    @Override
    public void testTermsQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [3]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [4]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of("1", "2"), mockContext())), equalTo(2));
            }
        }
    }

    @Override
    public void testTermsQueryIsExpensive() throws IOException {
        checkExpensiveQuery((ft, ctx) -> ft.termsQuery(randomList(100, () -> randomAlphaOfLengthBetween(1, 1000)), ctx));
    }

    public void testWildcardQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"aab\"]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"b\"]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().wildcardQuery("a*b", null, mockContext())), equalTo(1));
            }
        }
    }

    public void testWildcardQueryIsExpensive() throws IOException {
        checkExpensiveQuery((ft, ctx) -> ft.wildcardQuery(randomAlphaOfLengthBetween(1, 1000), null, ctx));
    }

    public void testMatchQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                ScriptKeywordMappedFieldType fieldType = build("append_param", Map.of("param", "-Suffix"));
                QueryShardContext queryShardContext = mockContext(true, fieldType);
                Query query = new MatchQueryBuilder("test", "1-Suffix").toQuery(queryShardContext);
                assertThat(searcher.count(query), equalTo(1));
            }
        }
    }

    @Override
    protected ScriptKeywordMappedFieldType simpleMappedFieldType() throws IOException {
        return build("read_foo", Map.of());
    }

    @Override
    protected String runtimeType() {
        return "keyword";
    }

    private static ScriptKeywordMappedFieldType build(String code, Map<String, Object> params) throws IOException {
        return build(new Script(ScriptType.INLINE, "test", code, params));
    }

    private static ScriptKeywordMappedFieldType build(Script script) throws IOException {
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
                        return Set.of(StringScriptFieldScript.CONTEXT);
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

                    private StringScriptFieldScript.Factory factory(String code) {
                        switch (code) {
                            case "read_foo":
                                return (params, lookup) -> (ctx) -> new StringScriptFieldScript(params, lookup, ctx) {
                                    @Override
                                    public void execute() {
                                        for (Object foo : (List<?>) getSource().get("foo")) {
                                            emitValue(foo.toString());
                                        }
                                    }
                                };
                            case "append_param":
                                return (params, lookup) -> (ctx) -> new StringScriptFieldScript(params, lookup, ctx) {
                                    @Override
                                    public void execute() {
                                        for (Object foo : (List<?>) getSource().get("foo")) {
                                            emitValue(foo.toString() + getParams().get("param").toString());
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
            StringScriptFieldScript.Factory factory = scriptService.compile(script, StringScriptFieldScript.CONTEXT);
            return new ScriptKeywordMappedFieldType("test", script, factory, emptyMap());
        }
    }

    private void checkExpensiveQuery(BiConsumer<ScriptKeywordMappedFieldType, QueryShardContext> queryBuilder) throws IOException {
        ScriptKeywordMappedFieldType ft = simpleMappedFieldType();
        Exception e = expectThrows(ElasticsearchException.class, () -> queryBuilder.accept(ft, mockContext(false)));
        assertThat(
            e.getMessage(),
            equalTo("queries cannot be executed against [runtime] fields while [search.allow_expensive_queries] is set to [false].")
        );
    }
}
