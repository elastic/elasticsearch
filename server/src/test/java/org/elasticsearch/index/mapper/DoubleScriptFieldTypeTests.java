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
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.index.fielddata.DoubleScriptFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.DoubleFieldScript;
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

public class DoubleScriptFieldTypeTests extends AbstractNonTextScriptFieldTypeTestCase {

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
                DoubleScriptFieldType ft = build("add_param", Map.of("param", 1));
                DoubleScriptFieldData ifd = ft.fielddataBuilder(mockFielddataContext()).build(null, null);
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
                DoubleScriptFieldData ifd = simpleMappedFieldType().fielddataBuilder(mockFielddataContext()).build(null, null);
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
                                ScriptDocValues.Doubles doubles = (ScriptDocValues.Doubles) getDoc().get("test");
                                return doubles.get(0);
                            }
                        };
                    }
                }, searchContext.lookup(), 2.5f, "test", 0, Version.CURRENT)), equalTo(1));
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
                assertThat(searcher.count(build("add_param", Map.of("param", 1)).termQuery(2, mockContext())), equalTo(1));
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
    protected Query randomTermsQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return ft.termsQuery(List.of(randomLong()), ctx);
    }

    @Override
    protected DoubleScriptFieldType simpleMappedFieldType() {
        return build("read_foo", Map.of());
    }

    @Override
    protected MappedFieldType loopFieldType() {
        return build("loop", Map.of());
    }

    @Override
    protected String typeName() {
        return "double";
    }

    private static DoubleScriptFieldType build(String code, Map<String, Object> params) {
        return build(new Script(ScriptType.INLINE, "test", code, params));
    }

    private static DoubleFieldScript.Factory factory(Script script) {
        return switch (script.getIdOrCode()) {
            case "read_foo" -> (fieldName, params, lookup) -> (ctx) -> new DoubleFieldScript(fieldName, params, lookup, ctx) {
                @Override
                public void execute() {
                    for (Object foo : (List<?>) lookup.source().source().get("foo")) {
                        emit(((Number) foo).doubleValue());
                    }
                }
            };
            case "add_param" -> (fieldName, params, lookup) -> (ctx) -> new DoubleFieldScript(fieldName, params, lookup, ctx) {
                @Override
                public void execute() {
                    for (Object foo : (List<?>) lookup.source().source().get("foo")) {
                        emit(((Number) foo).doubleValue() + ((Number) getParams().get("param")).doubleValue());
                    }
                }
            };
            case "loop" -> (fieldName, params, lookup) -> {
                // Indicate that this script wants the field call "test", which *is* the name of this field
                lookup.forkAndTrackFieldReferences("test");
                throw new IllegalStateException("shoud have thrown on the line above");
            };
            default -> throw new IllegalArgumentException("unsupported script [" + script.getIdOrCode() + "]");
        };
    }

    private static DoubleScriptFieldType build(Script script) {
        return new DoubleScriptFieldType("test", factory(script), script, emptyMap());
    }
}
