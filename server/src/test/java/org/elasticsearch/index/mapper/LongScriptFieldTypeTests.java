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
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
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
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.LongScriptFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.LongFieldScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.MultiValueMode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class LongScriptFieldTypeTests extends AbstractNonTextScriptFieldTypeTestCase {

    public void testFormat() throws IOException {
        assertThat(simpleMappedFieldType().docValueFormat("#.0", null).format(1), equalTo("1.0"));
        assertThat(simpleMappedFieldType().docValueFormat("#,##0.##", null).format(11), equalTo("11"));
        assertThat(simpleMappedFieldType().docValueFormat("#,##0.##", null).format(1123), equalTo("1,123"));
    }

    public void testLongFromSource() throws IOException {
        MapperService mapperService = createMapperService(runtimeFieldMapping(b -> b.field("type", "long")));
        ParsedDocument doc = mapperService.documentMapper().parse(source(b -> b.field("field", "9223372036854775806.00")));
        withLuceneIndex(mapperService, iw -> iw.addDocuments(doc.docs()), ir -> {
            MappedFieldType ft = mapperService.fieldType("field");
            SearchExecutionContext sec = createSearchExecutionContext(mapperService);
            Query rangeQuery = ft.rangeQuery(0, 9223372036854775807L, false, false, ShapeRelation.CONTAINS, null, null, sec);
            IndexSearcher searcher = new IndexSearcher(ir);
            assertEquals(1, searcher.count(rangeQuery));
        });
    }

    @Override
    public void testDocValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2, 1]}"))));
            List<Long> results = new ArrayList<>();
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                LongScriptFieldType ft = build("add_param", Map.of("param", 1), OnScriptError.FAIL);
                LongScriptFieldData ifd = ft.fielddataBuilder(mockFielddataContext()).build(null, null);
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
                assertThat(results, equalTo(List.of(2L, 2L, 3L)));
            }
        }
    }

    @Override
    public void testSort() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [4]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                LongScriptFieldData ifd = simpleMappedFieldType().fielddataBuilder(mockFielddataContext()).build(null, null);
                SortField sf = ifd.sortField(null, MultiValueMode.MIN, null, false);
                TopFieldDocs docs = searcher.search(new MatchAllDocsQuery(), 3, new Sort(sf));
                assertThat(reader.document(docs.scoreDocs[0].doc).getBinaryValue("_source").utf8ToString(), equalTo("{\"foo\": [1]}"));
                assertThat(reader.document(docs.scoreDocs[1].doc).getBinaryValue("_source").utf8ToString(), equalTo("{\"foo\": [2]}"));
                assertThat(reader.document(docs.scoreDocs[2].doc).getBinaryValue("_source").utf8ToString(), equalTo("{\"foo\": [4]}"));
            }
        }
    }

    public void testNow() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"timestamp\": [1595432181354]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"timestamp\": [1595432181351]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"timestamp\": [1595432181356]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                LongScriptFieldData ifd = build("millis_ago", Map.of(), OnScriptError.FAIL).fielddataBuilder(mockFielddataContext())
                    .build(null, null);
                SortField sf = ifd.sortField(null, MultiValueMode.MIN, null, false);
                TopFieldDocs docs = searcher.search(new MatchAllDocsQuery(), 3, new Sort(sf));
                assertThat(readSource(reader, docs.scoreDocs[0].doc), equalTo("{\"timestamp\": [1595432181356]}"));
                assertThat(readSource(reader, docs.scoreDocs[1].doc), equalTo("{\"timestamp\": [1595432181354]}"));
                assertThat(readSource(reader, docs.scoreDocs[2].doc), equalTo("{\"timestamp\": [1595432181351]}"));
                long t1 = (Long) (((FieldDoc) docs.scoreDocs[0]).fields[0]);
                assertThat(t1, greaterThan(3638011399L));
                long t2 = (Long) (((FieldDoc) docs.scoreDocs[1]).fields[0]);
                long t3 = (Long) (((FieldDoc) docs.scoreDocs[2]).fields[0]);
                assertThat(t2, equalTo(t1 + 2));
                assertThat(t3, equalTo(t1 + 5));
            }
        }
    }

    @Override
    public void testUsedInScript() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [4]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchExecutionContext searchContext = mockContext(true, simpleMappedFieldType());
                assertThat(searcher.count(new ScriptScoreQuery(new MatchAllDocsQuery(), new Script("test"), new ScoreScript.LeafFactory() {
                    @Override
                    public boolean needs_score() {
                        return false;
                    }

                    @Override
                    public ScoreScript newInstance(DocReader docReader) {
                        return new ScoreScript(Map.of(), searchContext.lookup(), docReader) {
                            @Override
                            public double execute(ExplanationHolder explanation) {
                                ScriptDocValues.Longs longs = (ScriptDocValues.Longs) getDoc().get("test");
                                return longs.get(0);
                            }
                        };
                    }
                }, searchContext.lookup(), 2.5f, "test", 0, IndexVersion.current())), equalTo(1));
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
    public void testRangeQuery() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                MappedFieldType ft = simpleMappedFieldType();
                assertThat(searcher.count(ft.rangeQuery("2", "3", true, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(2, 3, true, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(1.1, 3, true, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(1.1, 3, false, true, null, null, null, mockContext())), equalTo(1));
                assertThat(searcher.count(ft.rangeQuery(2, 3, false, true, null, null, null, mockContext())), equalTo(0));
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
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
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
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [1]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": [2]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of("1"), mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(1), mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(1.1), mockContext())), equalTo(0));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(1.1, 2), mockContext())), equalTo(1));
                assertThat(searcher.count(simpleMappedFieldType().termsQuery(List.of(2, 1), mockContext())), equalTo(2));
            }
        }
    }

    @Override
    protected Query randomTermsQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return ft.termsQuery(List.of(randomLong()), ctx);
    }

    @Override
    protected LongScriptFieldType simpleMappedFieldType() {
        return build("read_foo", Map.of(), OnScriptError.FAIL);
    }

    @Override
    protected LongScriptFieldType loopFieldType() {
        return build("loop", Map.of(), OnScriptError.FAIL);
    }

    @Override
    protected String typeName() {
        return "long";
    }

    protected LongScriptFieldType build(String code, Map<String, Object> params, OnScriptError onScriptError) {
        Script script = new Script(ScriptType.INLINE, "test", code, params);
        return new LongScriptFieldType("test", factory(script), script, emptyMap(), onScriptError);
    }

    private static LongFieldScript.Factory factory(Script script) {
        switch (script.getIdOrCode()) {
            case "read_foo":
                return (fieldName, params, lookup, onScriptError) -> (ctx) -> new LongFieldScript(
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
                            emit(((Number) foo).longValue());
                        }
                    }
                };
            case "add_param":
                return (fieldName, params, lookup, onScriptError) -> (ctx) -> new LongFieldScript(
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
                            emit(((Number) foo).longValue() + ((Number) getParams().get("param")).longValue());
                        }
                    }
                };
            case "millis_ago":
                // Painless actually call System.currentTimeMillis. We could mock the time but this works fine too.
                long now = System.currentTimeMillis();
                return (fieldName, params, lookup, onScriptError) -> (ctx) -> new LongFieldScript(
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
                        for (Object timestamp : (List<?>) source.get("timestamp")) {
                            emit(now - ((Number) timestamp).longValue());
                        }
                    }
                };
            case "loop":
                return (fieldName, params, lookup, onScriptError) -> {
                    // Indicate that this script wants the field call "test", which *is* the name of this field
                    lookup.forkAndTrackFieldReferences("test");
                    throw new IllegalStateException("should have thrown on the line above");
                };
            case "error":
                return (fieldName, params, lookup, onScriptError) -> ctx -> new LongFieldScript(
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
            default:
                throw new IllegalArgumentException("unsupported script [" + script.getIdOrCode() + "]");
        }
    }
}
