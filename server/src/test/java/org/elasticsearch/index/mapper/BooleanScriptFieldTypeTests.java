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
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.BooleanScriptFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.BooleanFieldScript;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptCompiler;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.field.BooleanDocValuesField;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class BooleanScriptFieldTypeTests extends AbstractNonTextScriptFieldTypeTestCase {

    @Override
    protected ScriptFactory parseFromSource() {
        return BooleanFieldScript.PARSE_FROM_SOURCE;
    }

    @Override
    protected ScriptFactory dummyScript() {
        return BooleanFieldScriptTests.DUMMY;
    }

    @Override
    public void testDocValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [true]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [true, false]}"))));
            List<Long> results = new ArrayList<>();
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                BooleanScriptFieldType ft = simpleMappedFieldType();
                BooleanScriptFieldData ifd = ft.fielddataBuilder(mockFielddataContext()).build(null, null);
                searcher.search(new MatchAllDocsQuery(), new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafCollector getLeafCollector(LeafReaderContext context) {
                        SortedNumericDocValues dv = ifd.load(context).getLongValues();
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
                assertThat(results, containsInAnyOrder(1L, 0L, 1L));
            }
        }
    }

    @Override
    public void testSort() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [true]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [false]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                BooleanScriptFieldData ifd = simpleMappedFieldType().fielddataBuilder(mockFielddataContext()).build(null, null);
                SortField sf = ifd.sortField(null, MultiValueMode.MIN, null, false);
                TopFieldDocs docs = searcher.search(new MatchAllDocsQuery(), 3, new Sort(sf));
                assertThat(reader.document(docs.scoreDocs[0].doc).getBinaryValue("_source").utf8ToString(), equalTo("{\"foo\": [false]}"));
                assertThat(reader.document(docs.scoreDocs[1].doc).getBinaryValue("_source").utf8ToString(), equalTo("{\"foo\": [true]}"));
            }
        }
    }

    @Override
    public void testUsedInScript() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [true]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [false]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                {
                    SearchExecutionContext searchContext = mockContext(true, simpleMappedFieldType());
                    assertThat(
                        searcher.count(new ScriptScoreQuery(new MatchAllDocsQuery(), new Script("test"), new ScoreScript.LeafFactory() {
                            @Override
                            public boolean needs_score() {
                                return false;
                            }

                            @Override
                            public ScoreScript newInstance(DocReader docReader) {
                                return new ScoreScript(Map.of(), searchContext.lookup(), docReader) {
                                    @Override
                                    public double execute(ExplanationHolder explanation) {
                                        ScriptDocValues.Booleans booleans = (ScriptDocValues.Booleans) getDoc().get("test");
                                        return booleans.get(0) ? 3 : 0;
                                    }
                                };
                            }
                        }, searchContext.lookup(), 2.5f, "test", 0, IndexVersion.current())),
                        equalTo(1)
                    );
                }
                {
                    SearchExecutionContext searchContext = mockContext(true, simpleMappedFieldType());
                    assertThat(
                        searcher.count(new ScriptScoreQuery(new MatchAllDocsQuery(), new Script("test"), new ScoreScript.LeafFactory() {
                            @Override
                            public boolean needs_score() {
                                return false;
                            }

                            @Override
                            public ScoreScript newInstance(DocReader docReader) {
                                return new ScoreScript(Map.of(), searchContext.lookup(), docReader) {
                                    @Override
                                    public double execute(ExplanationHolder explanation) {
                                        BooleanDocValuesField booleans = (BooleanDocValuesField) field("test");
                                        return booleans.getInternal(0) ? 3 : 0;
                                    }
                                };
                            }
                        }, searchContext.lookup(), 2.5f, "test", 0, IndexVersion.current())),
                        equalTo(1)
                    );
                }
            }
        }
    }

    @Override
    public void testExistsQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [false]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [true]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [true, false]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": []}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().existsQuery(mockContext())), equalTo(3));
            }
        }
    }

    @Override
    public void testRangeQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [true]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                MappedFieldType ft = simpleMappedFieldType();
                assertThat(searcher.count(ft.rangeQuery(true, true, true, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(false, true, true, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(false, true, false, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(false, false, true, true, null, null, null, mockContext())), equalTo(0));
            }
        }
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [false]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                MappedFieldType ft = simpleMappedFieldType();
                assertThat(searcher.count(ft.rangeQuery(false, false, true, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(false, true, true, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(false, true, true, false, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(true, true, true, true, null, null, null, mockContext())), equalTo(0));
            }
        }
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [false]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [true]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                MappedFieldType ft = simpleMappedFieldType();
                assertThat(searcher.count(ft.rangeQuery(false, false, true, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(true, true, true, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(false, true, true, true, null, null, null, mockContext())), equalTo(2));
                assertThat(searcher.count(ft.rangeQuery(false, false, false, false, null, null, null, mockContext())), equalTo(0));
                assertThat(searcher.count(ft.rangeQuery(true, true, false, false, null, null, null, mockContext())), equalTo(0));
            }
        }
    }

    public void testRangeQueryDegeneratesIntoNotExpensive() throws IOException {
        assertThat(
            simpleMappedFieldType().rangeQuery(true, true, false, false, null, null, null, mockContext()),
            instanceOf(MatchNoDocsQuery.class)
        );
        assertThat(
            simpleMappedFieldType().rangeQuery(false, false, false, false, null, null, null, mockContext()),
            instanceOf(MatchNoDocsQuery.class)
        );
        // Even if the running the field would blow up because it loops the query *still* just returns none.
        assertThat(
            loopFieldType().rangeQuery(true, true, false, false, null, null, null, mockContext()),
            instanceOf(MatchNoDocsQuery.class)
        );
        assertThat(
            loopFieldType().rangeQuery(false, false, false, false, null, null, null, mockContext()),
            instanceOf(MatchNoDocsQuery.class)
        );
    }

    @Override
    protected Query randomRangeQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        // Builds a random range query that doesn't degenerate into match none
        return switch (randomInt(2)) {
            case 0 -> ft.rangeQuery(true, true, true, true, null, null, null, ctx);
            case 1 -> ft.rangeQuery(false, true, true, true, null, null, null, ctx);
            case 2 -> ft.rangeQuery(false, true, false, true, null, null, null, ctx);
            default -> throw new UnsupportedOperationException();
        };
    }

    @Override
    public void testTermQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [true]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().termQuery(true, mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termQuery("true", mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termQuery(false, mockContext())), equalTo(0));
                assertThat(
                    searcher.count(build("xor_param", Map.of("param", false), OnScriptError.FAIL).termQuery(true, mockContext())),
                    equalTo(1)
                );
            }
        }
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [false]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().termQuery(false, mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termQuery("false", mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termQuery(null, mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termQuery(true, mockContext())), equalTo(0));
                assertThat(
                    searcher.count(build("xor_param", Map.of("param", false), OnScriptError.FAIL).termQuery(false, mockContext())),
                    equalTo(1)
                );
            }
        }
    }

    @Override
    protected Query randomTermQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return ft.termQuery(randomBoolean(), ctx);
    }

    @Override
    public void testTermsQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [true]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(true, true), mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of("true", "true"), mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(false, false), mockContext())), equalTo(0));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(true, false), mockContext())), equalTo(1));
            }
        }
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [false]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(false, false), mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of("false", "false"), mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(singletonList(null), mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(true, true), mockContext())), equalTo(0));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(true, false), mockContext())), equalTo(1));
            }
        }
    }

    public void testEmptyTermsQueryDegeneratesIntoMatchNone() throws IOException {
        assertThat(simpleMappedFieldType().termsQuery(List.of(), mockContext()), instanceOf(MatchNoDocsQuery.class));
    }

    @Override
    protected Query randomTermsQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return switch (randomInt(2)) {
            case 0 -> ft.termsQuery(List.of(true), ctx);
            case 1 -> ft.termsQuery(List.of(false), ctx);
            case 2 -> ft.termsQuery(List.of(false, true), ctx);
            default -> throw new UnsupportedOperationException();
        };
    }

    public void testDualingQueries() throws IOException {
        BooleanFieldMapper ootb = new BooleanFieldMapper.Builder("foo", ScriptCompiler.NONE, false, IndexVersion.current()).build(
            MapperBuilderContext.root(false, false)
        );
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            List<Boolean> values = randomList(0, 2, ESTestCase::randomBoolean);
            String source = "{\"foo\": " + values + "}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
                SourceToParse sourceToParse = new SourceToParse("test", new BytesArray(source), XContentType.JSON);
                DocumentParserContext ctx = new TestDocumentParserContext(MappingLookup.EMPTY, sourceToParse) {
                    @Override
                    public XContentParser parser() {
                        return parser;
                    }
                };
                ctx.doc().add(new StoredField("_source", new BytesRef(source)));

                ctx.parser().nextToken();
                ctx.parser().nextToken();
                ctx.parser().nextToken();
                while (ctx.parser().nextToken() != Token.END_ARRAY) {
                    ootb.parse(ctx);
                }
                addDocument(iw, ctx.doc());
                try (DirectoryReader reader = iw.getReader()) {
                    IndexSearcher searcher = newSearcher(reader);
                    assertSameCount(
                        searcher,
                        source,
                        "*",
                        simpleMappedFieldType().existsQuery(mockContext()),
                        ootb.fieldType().existsQuery(mockContext())
                    );
                    boolean term = randomBoolean();
                    assertSameCount(
                        searcher,
                        source,
                        term,
                        simpleMappedFieldType().termQuery(term, mockContext()),
                        ootb.fieldType().termQuery(term, mockContext())
                    );
                    List<Boolean> terms = randomList(0, 3, ESTestCase::randomBoolean);
                    assertSameCount(
                        searcher,
                        source,
                        terms,
                        simpleMappedFieldType().termsQuery(terms, mockContext()),
                        ootb.fieldType().termsQuery(terms, mockContext())
                    );
                    boolean low;
                    boolean high;
                    if (randomBoolean()) {
                        low = high = randomBoolean();
                    } else {
                        low = false;
                        high = true;
                    }
                    boolean includeLow = randomBoolean();
                    boolean includeHigh = randomBoolean();
                    assertSameCount(
                        searcher,
                        source,
                        (includeLow ? "[" : "(") + low + "," + high + (includeHigh ? "]" : ")"),
                        simpleMappedFieldType().rangeQuery(low, high, includeLow, includeHigh, null, null, null, mockContext()),
                        ootb.fieldType().rangeQuery(low, high, includeLow, includeHigh, null, null, null, mockContext())
                    );
                }
            }
        }
    }

    public void testBlockLoader() throws IOException {
        try (
            Directory directory = newDirectory();
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))
        ) {
            iw.addDocuments(
                List.of(
                    List.of(new StoredField("_source", new BytesRef("{\"foo\": [false]}"))),
                    List.of(new StoredField("_source", new BytesRef("{\"foo\": [true]}")))
                )
            );
            try (DirectoryReader reader = iw.getReader()) {
                BooleanScriptFieldType fieldType = build("xor_param", Map.of("param", false), OnScriptError.FAIL);
                List<Boolean> expected = List.of(false, true);
                assertThat(blockLoaderReadValuesFromColumnAtATimeReader(reader, fieldType), equalTo(expected));
                assertThat(blockLoaderReadValuesFromRowStrideReader(reader, fieldType), equalTo(expected));
            }
        }
    }

    private void assertSameCount(IndexSearcher searcher, String source, Object queryDescription, Query scriptedQuery, Query ootbQuery)
        throws IOException {
        assertThat(
            "source=" + source + ",query=" + queryDescription + ",scripted=" + scriptedQuery + ",ootb=" + ootbQuery,
            searcher.count(scriptedQuery),
            equalTo(searcher.count(ootbQuery))
        );
    }

    @Override
    protected BooleanScriptFieldType simpleMappedFieldType() {
        return build("read_foo", Map.of(), OnScriptError.FAIL);
    }

    @Override
    protected MappedFieldType loopFieldType() {
        return build("loop", Map.of(), OnScriptError.FAIL);
    }

    @Override
    protected String typeName() {
        return "boolean";
    }

    protected BooleanScriptFieldType build(String code, Map<String, Object> params, OnScriptError onScriptError) {
        Script script = new Script(ScriptType.INLINE, "test", code, params);
        return new BooleanScriptFieldType("test", factory(script), script, emptyMap(), onScriptError);
    }

    private static BooleanFieldScript.Factory factory(Script script) {
        return switch (script.getIdOrCode()) {
            case "read_foo" -> (fieldName, params, lookup, onScriptError) -> (ctx) -> new BooleanFieldScript(
                fieldName,
                params,
                lookup,
                onScriptError,
                ctx
            ) {
                @Override
                @SuppressWarnings("unchecked")
                public void execute() {
                    Map<String, Object> source = (Map<String, Object>) this.getParams().get("_source");
                    for (Object foo : (List<?>) source.get("foo")) {
                        emit((Boolean) foo);
                    }
                }
            };
            case "xor_param" -> (fieldName, params, lookup, onScriptError) -> (ctx) -> new BooleanFieldScript(
                fieldName,
                params,
                lookup,
                onScriptError,
                ctx
            ) {
                @Override
                @SuppressWarnings("unchecked")
                public void execute() {
                    Map<String, Object> source = (Map<String, Object>) this.getParams().get("_source");
                    for (Object foo : (List<?>) source.get("foo")) {
                        emit((Boolean) foo ^ ((Boolean) getParams().get("param")));
                    }
                }
            };
            case "loop" -> (fieldName, params, lookup, onScriptError) -> {
                // Indicate that this script wants the field call "test", which *is* the name of this field
                lookup.forkAndTrackFieldReferences("test");
                throw new IllegalStateException("should have thrown on the line above");
            };
            case "error" -> (fieldName, params, lookup, onScriptError) -> ctx -> new BooleanFieldScript(
                fieldName,
                params,
                lookup,
                onScriptError,
                ctx
            ) {
                @Override
                public void execute() {
                    throw new RuntimeException("test error");
                }
            };
            default -> throw new IllegalArgumentException("unsupported script [" + script.getIdOrCode() + "]");
        };
    }
}
