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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.DoubleScriptFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.mapper.blockloader.script.DoubleScriptBlockDocValuesReader;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.indices.CrankyCircuitBreakerService;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.DoubleFieldScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class DoubleScriptFieldTypeTests extends AbstractNonTextScriptFieldTypeTestCase {

    @Override
    protected ScriptFactory parseFromSource() {
        return DoubleFieldScript.PARSE_FROM_SOURCE;
    }

    @Override
    protected ScriptFactory dummyScript() {
        return DoubleFieldScriptTests.DUMMY;
    }

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
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [1.0]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [3.14, 1.4]}"))));
            List<Double> results = new ArrayList<>();
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                DoubleScriptFieldType ft = build("add_param", Map.of("param", 1), OnScriptError.FAIL);
                DoubleScriptFieldData ifd = ft.fielddataBuilder(mockFielddataContext()).build(null, null);
                searcher.search(Queries.ALL_DOCS_INSTANCE, new Collector() {
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
                assertThat(results, containsInAnyOrder(2.0, 2.4, 4.140000000000001));
            }
        }
    }

    @Override
    public void testSort() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [1.1]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [4.2]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [2.1]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                DoubleScriptFieldData ifd = simpleMappedFieldType().fielddataBuilder(mockFielddataContext()).build(null, null);
                SortField sf = ifd.sortField(null, MultiValueMode.MIN, null, false);
                TopFieldDocs docs = searcher.search(Queries.ALL_DOCS_INSTANCE, 3, new Sort(sf));
                StoredFields storedFields = reader.storedFields();
                assertThat(
                    storedFields.document(docs.scoreDocs[0].doc).getBinaryValue("_source").utf8ToString(),
                    equalTo("{\"foo\": [1.1]}")
                );
                assertThat(
                    storedFields.document(docs.scoreDocs[1].doc).getBinaryValue("_source").utf8ToString(),
                    equalTo("{\"foo\": [2.1]}")
                );
                assertThat(
                    storedFields.document(docs.scoreDocs[2].doc).getBinaryValue("_source").utf8ToString(),
                    equalTo("{\"foo\": [4.2]}")
                );
            }
        }
    }

    @Override
    public void testUsedInScript() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [1.1]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [4.2]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [2.1]}"))));
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
                                    ScriptDocValues.Doubles doubles = (ScriptDocValues.Doubles) getDoc().get("test");
                                    return doubles.get(0);
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

    @Override
    public void testRangeQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [2.5]}"))));
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

    @Override
    protected Query randomRangeQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return ft.rangeQuery(randomLong(), randomLong(), randomBoolean(), randomBoolean(), null, null, null, ctx);
    }

    @Override
    public void testTermQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().termQuery("1", mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termQuery(1, mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termQuery(1.1, mockContext())), equalTo(0));
                assertThat(
                    searcher.count(build("add_param", Map.of("param", 1), OnScriptError.FAIL).termQuery(2, mockContext())),
                    equalTo(1)
                );
            }
        }
    }

    @Override
    protected Query randomTermQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return ft.termQuery(randomLong(), ctx);
    }

    @Override
    public void testTermsQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": [2.1]}"))));
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

    public void testBlockLoader() throws IOException {
        testBlockLoader(newLimitedBreaker(ByteSizeValue.ofMb(1)), f -> f);
    }

    public void testWithCrankyBreaker() throws IOException {
        CircuitBreaker cranky = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
        try {
            testBlockLoader(cranky, r -> r);
            logger.info("Cranky breaker didn't break. This should be rare, but possible randomly.");
        } catch (CircuitBreakingException e) {
            logger.info("Cranky breaker broke", e);
        }
        assertThat(cranky.getUsed(), equalTo(0L));
    }

    public void testWithCrankyFactory() throws IOException {
        try {
            testBlockLoader(newLimitedBreaker(ByteSizeValue.ofMb(1)), CrankyLeafFactory::new);
            logger.info("Cranky factory didn't break.");
        } catch (IllegalStateException e) {
            logger.info("Cranky factory broke", e);
        }
    }

    public void testWithCrankyBreakerAndFactory() throws IOException {
        CircuitBreaker cranky = new CrankyCircuitBreakerService.CrankyCircuitBreaker();
        try {
            testBlockLoader(cranky, CrankyLeafFactory::new);
            logger.info("Cranky breaker nor reader didn't break. This should be rare, but possible randomly.");
        } catch (IllegalStateException | CircuitBreakingException e) {
            logger.info("Cranky breaker or reader broke", e);
        }
        assertThat(cranky.getUsed(), equalTo(0L));
    }

    private void testBlockLoader(
        CircuitBreaker breaker,
        Function<DoubleFieldScript.LeafFactory, DoubleFieldScript.LeafFactory> factoryWrapper
    ) throws IOException {
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
                DoubleScriptFieldType fieldType = buildWrapped("add_param", Map.of("param", 1), factoryWrapper);
                assertThat(blockLoaderReadValuesFromColumnAtATimeReader(breaker, reader, fieldType, 0), equalTo(List.of(2d, 3d)));
                assertThat(blockLoaderReadValuesFromColumnAtATimeReader(breaker, reader, fieldType, 1), equalTo(List.of(3d)));
                assertThat(blockLoaderReadValuesFromRowStrideReader(breaker, reader, fieldType), equalTo(List.of(2d, 3d)));
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
                    List.of(new StoredField("_source", new BytesRef("{\"test\": [1.1]}"))),
                    List.of(new StoredField("_source", new BytesRef("{\"test\": [2.1]}")))
                )
            );
            try (DirectoryReader reader = iw.getReader()) {
                DoubleScriptFieldType fieldType = simpleSourceOnlyMappedFieldType();

                // Assert implementations:
                BlockLoader loader = fieldType.blockLoader(blContext(Settings.EMPTY, true));
                assertThat(loader, instanceOf(DoubleScriptBlockDocValuesReader.DoubleScriptBlockLoader.class));
                // ignored source doesn't support column at a time loading:
                CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
                try (var columnAtATimeLoader = loader.columnAtATimeReader(reader.leaves().getFirst()).apply(breaker)) {
                    assertThat(columnAtATimeLoader, instanceOf(DoubleScriptBlockDocValuesReader.class));
                }
                try (var rowStrideReader = loader.rowStrideReader(breaker, reader.leaves().getFirst())) {
                    assertThat(rowStrideReader, instanceOf(DoubleScriptBlockDocValuesReader.class));
                }

                // Assert values:
                assertThat(blockLoaderReadValuesFromColumnAtATimeReader(breaker, reader, fieldType, 0), equalTo(List.of(1.1, 2.1)));
                assertThat(blockLoaderReadValuesFromColumnAtATimeReader(breaker, reader, fieldType, 1), equalTo(List.of(2.1)));
                assertThat(blockLoaderReadValuesFromRowStrideReader(breaker, reader, fieldType), equalTo(List.of(1.1, 2.1)));
            }
        }
    }

    public void testBlockLoaderSourceOnlyRuntimeFieldWithSyntheticSource() throws IOException {
        var settings = Settings.builder().put("index.mapping.source.mode", "synthetic").build();
        try (
            Directory directory = newDirectory();
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))
        ) {

            var document1 = createDocumentWithIgnoredSource("[1.1]");
            var document2 = createDocumentWithIgnoredSource("[2.1]");

            iw.addDocuments(List.of(document1, document2));
            try (DirectoryReader reader = iw.getReader()) {
                DoubleScriptFieldType fieldType = simpleSourceOnlyMappedFieldType();

                // Assert implementations:
                BlockLoader loader = fieldType.blockLoader(blContext(settings, true));
                assertThat(loader, instanceOf(FallbackSyntheticSourceBlockLoader.class));
                CircuitBreaker breaker = newLimitedBreaker(ByteSizeValue.ofMb(1));
                // ignored source doesn't support column at a time loading:
                assertThat(loader.columnAtATimeReader(reader.leaves().getFirst()), nullValue());
                try (var rowStrideReader = loader.rowStrideReader(breaker, reader.leaves().getFirst())) {
                    assertThat(
                        rowStrideReader.getClass().getName(),
                        equalTo("org.elasticsearch.index.mapper.FallbackSyntheticSourceBlockLoader$IgnoredSourceRowStrideReader")
                    );
                }

                // Assert values:
                assertThat(
                    blockLoaderReadValuesFromRowStrideReader(breaker, settings, reader, fieldType, true),
                    equalTo(List.of(1.1, 2.1))
                );
            }
        }
    }

    @Override
    protected Query randomTermsQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return ft.termsQuery(List.of(randomLong()), ctx);
    }

    @Override
    protected DoubleScriptFieldType simpleMappedFieldType() {
        return build("read_foo", Map.of(), OnScriptError.FAIL);
    }

    private DoubleScriptFieldType simpleSourceOnlyMappedFieldType() {
        return build("read_test", Map.of(), OnScriptError.FAIL);
    }

    @Override
    protected MappedFieldType loopFieldType() {
        return build("loop", Map.of(), OnScriptError.FAIL);
    }

    @Override
    protected String typeName() {
        return "double";
    }

    protected DoubleScriptFieldType build(String code, Map<String, Object> params, OnScriptError onScriptError) {
        Script script = new Script(ScriptType.INLINE, "test", code, params);
        return new DoubleScriptFieldType("test", factory(script), script, emptyMap(), onScriptError);
    }

    protected DoubleScriptFieldType buildWrapped(
        String code,
        Map<String, Object> params,
        Function<DoubleFieldScript.LeafFactory, DoubleFieldScript.LeafFactory> leafFactoryWrapper
    ) {
        Script script = new Script(ScriptType.INLINE, "test", code, params);
        DoubleFieldScript.Factory factory = factory(script);
        DoubleFieldScript.Factory wrapped = (fieldName, params1, searchLookup, onScriptError) -> leafFactoryWrapper.apply(
            factory.newFactory(fieldName, params1, searchLookup, onScriptError)
        );
        return new DoubleScriptFieldType("test", wrapped, script, emptyMap(), OnScriptError.FAIL);
    }

    private static DoubleFieldScript.Factory factory(Script script) {
        return switch (script.getIdOrCode()) {
            case "read_test" -> new DoubleFieldScript.Factory() {
                @Override
                public DoubleFieldScript.LeafFactory newFactory(
                    String fieldName,
                    Map<String, Object> params,
                    SearchLookup lookup,
                    OnScriptError onScriptError
                ) {
                    return (ctx) -> new DoubleFieldScript(fieldName, params, lookup, onScriptError, ctx) {
                        @Override
                        @SuppressWarnings("unchecked")
                        public void execute() {
                            Map<String, Object> source = (Map<String, Object>) this.getParams().get("_source");
                            for (Object foo : (List<?>) source.get("test")) {
                                emit(((Number) foo).doubleValue());
                            }
                        };
                    };
                }

                @Override
                public boolean isParsedFromSource() {
                    return true;
                }
            };
            case "read_foo" -> (fieldName, params, lookup, onScriptError) -> (ctx) -> new DoubleFieldScript(
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
                        emit(((Number) foo).doubleValue());
                    }
                }
            };
            case "add_param" -> (fieldName, params, lookup, onScriptError) -> (ctx) -> new DoubleFieldScript(
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
                        emit(((Number) foo).doubleValue() + ((Number) getParams().get("param")).doubleValue());
                    }
                }
            };
            case "loop" -> (fieldName, params, lookup, onScriptError) -> {
                // Indicate that this script wants the field call "test", which *is* the name of this field
                lookup.forkAndTrackFieldReferences("test");
                throw new IllegalStateException("should have thrown on the line above");
            };
            case "error" -> (fieldName, params, lookup, onScriptError) -> ctx -> new DoubleFieldScript(
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

    private static class CrankyLeafFactory implements DoubleFieldScript.LeafFactory {
        private final DoubleFieldScript.LeafFactory next;

        private CrankyLeafFactory(DoubleFieldScript.LeafFactory next) {
            this.next = next;
        }

        @Override
        public DoubleFieldScript newInstance(LeafReaderContext ctx) {
            if (between(0, 20) == 0) {
                throw new IllegalStateException("cranky");
            }
            return next.newInstance(ctx);
        }
    }
}
