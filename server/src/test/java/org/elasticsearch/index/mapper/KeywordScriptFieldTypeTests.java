/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.BinaryScriptFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.StringScriptFieldData;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.StringFieldScript;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class KeywordScriptFieldTypeTests extends AbstractScriptFieldTypeTestCase {

    @Override
    protected ScriptFactory parseFromSource() {
        return StringFieldScript.PARSE_FROM_SOURCE;
    }

    @Override
    protected ScriptFactory dummyScript() {
        return StringFieldScriptTests.DUMMY;
    }

    @Override
    public void testDocValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [2, 1]}"))));
            List<String> results = new ArrayList<>();
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                KeywordScriptFieldType ft = build("append_param", Map.of("param", "-suffix"), OnScriptError.FAIL);
                StringScriptFieldData ifd = ft.fielddataBuilder(mockFielddataContext()).build(null, null);
                searcher.search(Queries.ALL_DOCS_INSTANCE, new Collector() {
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
                assertThat(results, containsInAnyOrder("1-suffix", "1-suffix", "2-suffix"));
            }
        }
    }

    @Override
    public void testSort() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"a\"]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"d\"]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"b\"]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                BinaryScriptFieldData ifd = simpleMappedFieldType().fielddataBuilder(mockFielddataContext()).build(null, null);
                SortField sf = ifd.sortField(null, MultiValueMode.MIN, null, false);
                TopFieldDocs docs = searcher.search(Queries.ALL_DOCS_INSTANCE, 3, new Sort(sf));
                StoredFields storedFields = reader.storedFields();
                assertThat(
                    storedFields.document(docs.scoreDocs[0].doc).getBinaryValue("_source").utf8ToString(),
                    equalTo("{\"foo\": [\"a\"]}")
                );
                assertThat(
                    storedFields.document(docs.scoreDocs[1].doc).getBinaryValue("_source").utf8ToString(),
                    equalTo("{\"foo\": [\"b\"]}")
                );
                assertThat(
                    storedFields.document(docs.scoreDocs[2].doc).getBinaryValue("_source").utf8ToString(),
                    equalTo("{\"foo\": [\"d\"]}")
                );
            }
        }
    }

    @Override
    public void testUsedInScript() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"a\"]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"aaa\"]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"aa\"]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchExecutionContext searchContext = mockContext(true, simpleMappedFieldType());
                assertThat(
                    searcher.count(new ScriptScoreQuery(Queries.ALL_DOCS_INSTANCE, new Script("test"), new ScoreScript.LeafFactory() {
                        @Override
                        public boolean needs_score() {
                            return false;
                        }

                        @Override
                        public boolean needs_termStats() {
                            return false;
                        }

                        @Override
                        public ScoreScript newInstance(DocReader docReader) {
                            return new ScoreScript(Map.of(), searchContext.lookup(), docReader) {
                                @Override
                                public double execute(ExplanationHolder explanation) {
                                    ScriptDocValues.Strings bytes = (ScriptDocValues.Strings) getDoc().get("test");
                                    return bytes.get(0).length();
                                }
                            };
                        }
                    }, searchContext.lookup(), 2.5f, "test", 0, IndexVersion.current())),
                    equalTo(1)
                );
            }
        }
    }

    @Override
    public void testExistsQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": []}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().existsQuery(mockContext())), equalTo(1));
            }
        }
    }

    public void testFuzzyQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cat\"]}"))));   // No edits, matches
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"caat\"]}"))));  // Single insertion, matches
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cta\"]}"))));   // Single transposition, matches
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"caaat\"]}")))); // Two insertions, no match
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"dog\"]}"))));   // Totally wrong, no match
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
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cat\"]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cata\"]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"dog\"]}"))));
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
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cat\"]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cata\"]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"dog\"]}"))));
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
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cat\"]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"cata\"]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"dog\"]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(
                    searcher.count(
                        simpleMappedFieldType().regexpQuery("ca.+", 0, 0, Operations.DEFAULT_DETERMINIZE_WORK_LIMIT, null, mockContext())
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
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                KeywordScriptFieldType fieldType = build("append_param", Map.of("param", "-suffix"), OnScriptError.FAIL);
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
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [3]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [4]}"))));
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
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"aab\"]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"b\"]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().wildcardQuery("a*b", null, mockContext())), equalTo(1));
            }
        }
    }

    // Normalized WildcardQueries are requested by the QueryStringQueryParser
    public void testNormalizedWildcardQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"aab\"]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [\"b\"]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().normalizedWildcardQuery("a*b", null, mockContext())), equalTo(1));
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
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                KeywordScriptFieldType fieldType = build("append_param", Map.of("param", "-Suffix"), OnScriptError.FAIL);
                SearchExecutionContext searchExecutionContext = mockContext(true, fieldType);
                Query query = new MatchQueryBuilder("test", "1-Suffix").toQuery(searchExecutionContext);
                assertThat(searcher.count(query), equalTo(1));
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
                    List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))),
                    List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}")))
                )
            );
            try (DirectoryReader reader = iw.getReader()) {
                KeywordScriptFieldType fieldType = build("append_param", Map.of("param", "-Suffix"), OnScriptError.FAIL);
                assertThat(
                    blockLoaderReadValuesFromColumnAtATimeReader(reader, fieldType, 0),
                    equalTo(List.of(new BytesRef("1-Suffix"), new BytesRef("2-Suffix")))
                );
                assertThat(blockLoaderReadValuesFromColumnAtATimeReader(reader, fieldType, 1), equalTo(List.of(new BytesRef("2-Suffix"))));
                assertThat(
                    blockLoaderReadValuesFromRowStrideReader(reader, fieldType),
                    equalTo(List.of(new BytesRef("1-Suffix"), new BytesRef("2-Suffix")))
                );
            }
        }
    }

    public void testBlockLoaderSourceOnlyRuntimeField() throws IOException {
        try (
            Directory directory = newDirectory();
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))
        ) {
            iw.addDocuments(
                List.of(
                    List.of(new StoredField("_source", new BytesRef("{\"test\": [\"cat\"]}"))),
                    List.of(new StoredField("_source", new BytesRef("{\"test\": [\"dog\"]}")))
                )
            );
            try (DirectoryReader reader = iw.getReader()) {
                KeywordScriptFieldType fieldType = simpleSourceOnlyMappedFieldType();

                // Assert implementations:
                BlockLoader loader = fieldType.blockLoader(blContext(Settings.EMPTY, true));
                assertThat(loader, instanceOf(KeywordScriptBlockDocValuesReader.KeywordScriptBlockLoader.class));
                // ignored source doesn't support column at a time loading:
                var columnAtATimeLoader = loader.columnAtATimeReader(reader.leaves().getFirst());
                assertThat(columnAtATimeLoader, instanceOf(KeywordScriptBlockDocValuesReader.class));
                var rowStrideReader = loader.rowStrideReader(reader.leaves().getFirst());
                assertThat(rowStrideReader, instanceOf(KeywordScriptBlockDocValuesReader.class));

                var catBytes = new BytesRef("cat");
                var dogBytes = new BytesRef("dog");

                // Assert values:
                assertThat(blockLoaderReadValuesFromColumnAtATimeReader(reader, fieldType, 0), equalTo(List.of(catBytes, dogBytes)));
                assertThat(blockLoaderReadValuesFromColumnAtATimeReader(reader, fieldType, 1), equalTo(List.of(dogBytes)));
                assertThat(blockLoaderReadValuesFromRowStrideReader(reader, fieldType), equalTo(List.of(catBytes, dogBytes)));
            }
        }
    }

    public void testBlockLoaderSourceOnlyRuntimeFieldWithSyntheticSource() throws IOException {
        var settings = Settings.builder().put("index.mapping.source.mode", "synthetic").build();
        try (
            Directory directory = newDirectory();
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))
        ) {

            var document1 = createDocumentWithIgnoredSource("[\"cat\"]");
            var document2 = createDocumentWithIgnoredSource("[\"dog\"]");

            iw.addDocuments(List.of(document1, document2));
            try (DirectoryReader reader = iw.getReader()) {
                KeywordScriptFieldType fieldType = simpleSourceOnlyMappedFieldType();

                // Assert implementations:
                BlockLoader loader = fieldType.blockLoader(blContext(settings, true));
                assertThat(loader, instanceOf(FallbackSyntheticSourceBlockLoader.class));
                // ignored source doesn't support column at a time loading:
                var columnAtATimeLoader = loader.columnAtATimeReader(reader.leaves().getFirst());
                assertThat(columnAtATimeLoader, nullValue());
                var rowStrideReader = loader.rowStrideReader(reader.leaves().getFirst());
                assertThat(
                    rowStrideReader.getClass().getName(),
                    equalTo("org.elasticsearch.index.mapper.FallbackSyntheticSourceBlockLoader$IgnoredSourceRowStrideReader")
                );

                // Assert values:
                assertThat(
                    blockLoaderReadValuesFromRowStrideReader(settings, reader, fieldType, true),
                    equalTo(List.of(new BytesRef("cat"), new BytesRef("dog")))
                );
            }
        }
    }

    private KeywordScriptFieldType simpleSourceOnlyMappedFieldType() {
        Script script = new Script(ScriptType.INLINE, "test", "", emptyMap());
        StringFieldScript.Factory factory = new StringFieldScript.Factory() {
            @Override
            public StringFieldScript.LeafFactory newFactory(
                String fieldName,
                Map<String, Object> params,
                SearchLookup searchLookup,
                OnScriptError onScriptError
            ) {
                return ctx -> new StringFieldScript(fieldName, params, searchLookup, onScriptError, ctx) {
                    @Override
                    @SuppressWarnings("unchecked")
                    public void execute() {
                        Map<String, Object> source = (Map<String, Object>) this.getParams().get("_source");
                        for (Object foo : (List<?>) source.get("test")) {
                            emit((String) foo);
                        }
                    }
                };
            }

            @Override
            public boolean isParsedFromSource() {
                return true;
            }
        };
        return new KeywordScriptFieldType("test", factory, script, emptyMap(), OnScriptError.FAIL);
    }

    @Override
    protected KeywordScriptFieldType simpleMappedFieldType() {
        return build("read_foo", Map.of(), OnScriptError.FAIL);
    }

    @Override
    protected KeywordScriptFieldType loopFieldType() {
        return build("loop", Map.of(), OnScriptError.FAIL);
    }

    @Override
    protected String typeName() {
        return "keyword";
    }

    protected KeywordScriptFieldType build(String code, Map<String, Object> params, OnScriptError onScriptError) {
        Script script = new Script(ScriptType.INLINE, "test", code, params);
        return new KeywordScriptFieldType("test", factory(script), script, emptyMap(), onScriptError);
    }

    private static StringFieldScript.Factory factory(Script script) {
        return switch (script.getIdOrCode()) {
            case "read_foo" -> (fieldName, params, lookup, onScriptError) -> ctx -> new StringFieldScript(
                fieldName,
                params,
                lookup,
                OnScriptError.FAIL,
                ctx
            ) {
                @Override
                @SuppressWarnings("unchecked")
                public void execute() {
                    Map<String, Object> source = (Map<String, Object>) this.getParams().get("_source");
                    for (Object foo : (List<?>) source.get("foo")) {
                        emit(foo.toString());
                    }
                }
            };
            case "append_param" -> (fieldName, params, lookup, onScriptError) -> ctx -> new StringFieldScript(
                fieldName,
                params,
                lookup,
                OnScriptError.FAIL,
                ctx
            ) {
                @Override
                @SuppressWarnings("unchecked")
                public void execute() {
                    Map<String, Object> source = (Map<String, Object>) this.getParams().get("_source");
                    for (Object foo : (List<?>) source.get("foo")) {
                        emit(foo.toString() + getParams().get("param").toString());
                    }
                }
            };
            case "loop" -> (fieldName, params, lookup, onScriptError) -> {
                // Indicate that this script wants the field call "test", which *is* the name of this field
                lookup.forkAndTrackFieldReferences("test");
                throw new IllegalStateException("should have thrown on the line above");
            };
            case "error" -> (fieldName, params, lookup, onScriptError) -> ctx -> new StringFieldScript(
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
