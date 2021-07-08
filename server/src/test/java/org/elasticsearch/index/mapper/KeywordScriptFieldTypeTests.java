/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

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
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.index.fielddata.BinaryScriptFieldData;
import org.elasticsearch.index.fielddata.StringScriptFieldData;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;

public class KeywordScriptFieldTypeTests extends AbstractScriptFieldTypeTestCase {

    @Override
    public void testDocValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2, 1]}"))));
            List<String> results = new ArrayList<>();
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                KeywordScriptFieldType ft = build("append_param", Map.of("param", "-suffix"));
                StringScriptFieldData ifd = ft.fielddataBuilder("test", mockContext()::lookup).build(null, null);
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
                BinaryScriptFieldData ifd = simpleMappedFieldType().fielddataBuilder("test", mockContext()::lookup).build(null, null);
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
                SearchExecutionContext searchContext = mockContext(true, simpleMappedFieldType());
                assertThat(searcher.count(new ScriptScoreQuery(new MatchAllDocsQuery(), new Script("test"), new ScoreScript.LeafFactory() {
                    @Override
                    public boolean needs_score() {
                        return false;
                    }

                    @Override
                    public ScoreScript newInstance(LeafReaderContext ctx) {
                        return new ScoreScript(Map.of(), searchContext.lookup(), ctx) {
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

    public void testFuzzyQueryIsExpensive() {
        checkExpensiveQuery(this::randomFuzzyQuery);
    }

    public void testFuzzyQueryInLoop() {
        checkLoop(this::randomFuzzyQuery);
    }

    private Query randomFuzzyQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return ft.fuzzyQuery(
            randomAlphaOfLengthBetween(1, 1000),
            randomFrom(Fuzziness.AUTO, Fuzziness.ZERO, Fuzziness.ONE, Fuzziness.TWO),
            randomInt(),
            randomInt(),
            randomBoolean(),
            ctx
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

    public void testPrefixQueryIsExpensive() {
        checkExpensiveQuery(this::randomPrefixQuery);
    }

    public void testPrefixQueryInLoop() {
        checkLoop(this::randomPrefixQuery);
    }

    private Query randomPrefixQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return ft.prefixQuery(randomAlphaOfLengthBetween(1, 1000), null, ctx);
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
                assertThat(
                    searcher.count(simpleMappedFieldType().rangeQuery(null, "d", true, false, null, null, null, mockContext())),
                    equalTo(2)
                );
                assertThat(
                    searcher.count(simpleMappedFieldType().rangeQuery("cat", null, false, true, null, null, null, mockContext())),
                    equalTo(2)
                );
                assertThat(
                    searcher.count(simpleMappedFieldType().rangeQuery(null, null, true, true, null, null, null, mockContext())),
                    equalTo(3)
                );
            }
        }
    }

    @Override
    protected Query randomRangeQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        boolean lowerNull = randomBoolean();
        boolean upperNull = randomBoolean();
        return ft.rangeQuery(
            lowerNull ? null : randomAlphaOfLengthBetween(0, 1000),
            upperNull ? null : randomAlphaOfLengthBetween(0, 1000),
            lowerNull || randomBoolean(),
            upperNull || randomBoolean(),
            null,
            null,
            null,
            ctx
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
                        simpleMappedFieldType().regexpQuery("ca.+", 0, 0, Operations.DEFAULT_MAX_DETERMINIZED_STATES, null, mockContext())
                    ),
                    equalTo(2)
                );
            }
        }
    }

    public void testRegexpQueryInLoop() throws IOException {
        checkLoop(this::randomRegexpQuery);
    }

    private Query randomRegexpQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return ft.regexpQuery(randomAlphaOfLengthBetween(1, 1000), randomInt(0xFF), 0, Integer.MAX_VALUE, null, ctx);
    }

    @Override
    public void testTermQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                KeywordScriptFieldType fieldType = build("append_param", Map.of("param", "-suffix"));
                assertThat(searcher.count(fieldType.termQuery("1-suffix", mockContext())), equalTo(1));
            }
        }
    }

    @Override
    protected Query randomTermQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return ft.termQuery(randomAlphaOfLengthBetween(1, 1000), ctx);
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
    protected Query randomTermsQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return ft.termsQuery(randomList(100, () -> randomAlphaOfLengthBetween(1, 1000)), ctx);
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

    public void testWildcardQueryIsExpensive() {
        checkExpensiveQuery(this::randomWildcardQuery);
    }

    public void testWildcardQueryInLoop() {
        checkLoop(this::randomWildcardQuery);
    }

    private Query randomWildcardQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return ft.wildcardQuery(randomAlphaOfLengthBetween(1, 1000), null, ctx);
    }

    public void testMatchQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                KeywordScriptFieldType fieldType = build("append_param", Map.of("param", "-Suffix"));
                SearchExecutionContext searchExecutionContext = mockContext(true, fieldType);
                Query query = new MatchQueryBuilder("test", "1-Suffix").toQuery(searchExecutionContext);
                assertThat(searcher.count(query), equalTo(1));
            }
        }
    }

    @Override
    protected KeywordScriptFieldType simpleMappedFieldType() {
        return build("read_foo", Map.of());
    }

    @Override
    protected KeywordScriptFieldType loopFieldType() {
        return build("loop", Map.of());
    }

    @Override
    protected String typeName() {
        return "keyword";
    }

    private static KeywordScriptFieldType build(String code, Map<String, Object> params) {
        return build(new Script(ScriptType.INLINE, "test", code, params));
    }

    private static StringFieldScript.Factory factory(Script script) {
        switch (script.getIdOrCode()) {
            case "read_foo":
                return (fieldName, params, lookup) -> ctx -> new StringFieldScript(fieldName, params, lookup, ctx) {
                    @Override
                    public void execute() {
                        for (Object foo : (List<?>) lookup.source().get("foo")) {
                            emit(foo.toString());
                        }
                    }
                };
            case "append_param":
                return (fieldName, params, lookup) -> ctx -> new StringFieldScript(fieldName, params, lookup, ctx) {
                    @Override
                    public void execute() {
                        for (Object foo : (List<?>) lookup.source().get("foo")) {
                            emit(foo.toString() + getParams().get("param").toString());
                        }
                    }
                };
            case "loop":
                return (fieldName, params, lookup) -> {
                    // Indicate that this script wants the field call "test", which *is* the name of this field
                    lookup.forkAndTrackFieldReferences("test");
                    throw new IllegalStateException("shoud have thrown on the line above");
                };
            default:
                throw new IllegalArgumentException("unsupported script [" + script.getIdOrCode() + "]");
        }
    }

    private static KeywordScriptFieldType build(Script script) {
        return new KeywordScriptFieldType("test", factory(script), script, emptyMap());
    }
}
