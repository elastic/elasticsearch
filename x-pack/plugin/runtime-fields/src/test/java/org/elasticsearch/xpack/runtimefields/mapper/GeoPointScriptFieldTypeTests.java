/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
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
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
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
import org.elasticsearch.xpack.runtimefields.fielddata.GeoPointScriptFieldData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;

public class GeoPointScriptFieldTypeTests extends AbstractNonTextScriptFieldTypeTestCase {

    @Override
    protected boolean supportsTermQueries() {
        return false;
    }

    @Override
    protected boolean supportsRangeQueries() {
        return false;
    }

    @Override
    public void testDocValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": {\"lat\": 45.0, \"lon\" : 45.0}}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": {\"lat\": 0.0, \"lon\" : 0.0}}"))));
            List<Object> results = new ArrayList<>();
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                GeoPointScriptFieldType ft = build("fromLatLon", Map.of());
                GeoPointScriptFieldData ifd = ft.fielddataBuilder("test", mockContext()::lookup).build(null, null);
                searcher.search(new MatchAllDocsQuery(), new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafCollector getLeafCollector(LeafReaderContext context) {
                        MultiGeoPointValues dv = ifd.load(context).getGeoPointValues();
                        return new LeafCollector() {
                            @Override
                            public void setScorer(Scorable scorer) {}

                            @Override
                            public void collect(int doc) throws IOException {
                                if (dv.advanceExact(doc)) {
                                    for (int i = 0; i < dv.docValueCount(); i++) {
                                        final GeoPoint point = dv.nextValue();
                                        results.add(new GeoPoint(point.lat(), point.lon()));
                                    }
                                }
                            }
                        };
                    }
                });
                assertThat(results, equalTo(List.of(new GeoPoint(45.0, 45.0), new GeoPoint(0.0, 0.0))));
            }
        }
    }

    @Override
    public void testSort() throws IOException {
        GeoPointScriptFieldData ifd = simpleMappedFieldType().fielddataBuilder("test", mockContext()::lookup).build(null, null);
        Exception e = expectThrows(IllegalArgumentException.class, () -> ifd.sortField(null, MultiValueMode.MIN, null, false));
        assertThat(e.getMessage(), equalTo("can't sort on geo_point field without using specific sorting feature, like geo_distance"));
    }

    @Override
    public void testUsedInScript() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": {\"lat\": 45.0, \"lon\" : 45.0}}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": {\"lat\": 0.0, \"lon\" : 0.0}}"))));
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
                                ScriptDocValues.GeoPoints points = (ScriptDocValues.GeoPoints) getDoc().get("test");
                                return (int) points.get(0).lat() + 1;
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
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": {\"lat\": 45.0, \"lon\" : 45.0}}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"foo\": {\"lat\": 0.0, \"lon\" : 0.0}}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                assertThat(searcher.count(simpleMappedFieldType().existsQuery(mockContext())), equalTo(2));
            }
        }
    }

    @Override
    public void testRangeQuery() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> simpleMappedFieldType().rangeQuery("0.0", "45.0", false, false, null, null, null, mockContext())
        );
        assertThat(e.getMessage(), equalTo("Runtime field [test] of type [" + typeName() + "] does not support range queries"));
    }

    @Override
    protected Query randomRangeQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        throw new IllegalArgumentException("Unsupported");
    }

    @Override
    public void testTermQuery() {
        Exception e = expectThrows(IllegalArgumentException.class, () -> simpleMappedFieldType().termQuery("0.0,0.0", mockContext()));
        assertThat(
            e.getMessage(),
            equalTo("Geometry fields do not support exact searching, use dedicated geometry queries instead: [test]")
        );
    }

    @Override
    protected Query randomTermQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        throw new IllegalArgumentException("Unsupported");
    }

    @Override
    public void testTermsQuery() {
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> simpleMappedFieldType().termsQuery(List.of("0.0,0.0", "45.0,45.0"), mockContext())
        );

        assertThat(
            e.getMessage(),
            equalTo("Geometry fields do not support exact searching, use dedicated geometry queries instead: [test]")
        );

    }

    @Override
    protected Query randomTermsQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return ft.termsQuery(randomList(100, GeometryTestUtils::randomPoint), mockContext());
    }

    @Override
    protected GeoPointScriptFieldType simpleMappedFieldType() throws IOException {
        return build("fromLatLon", Map.of());
    }

    @Override
    protected MappedFieldType loopFieldType() throws IOException {
        return build("loop", Map.of());
    }

    @Override
    protected String typeName() {
        return "geo_point";
    }

    private static GeoPointScriptFieldType build(String code, Map<String, Object> params) throws IOException {
        return build(new Script(ScriptType.INLINE, "test", code, params));
    }

    private static GeoPointScriptFieldType build(Script script) throws IOException {
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
                        return Set.of(StringFieldScript.CONTEXT, GeoPointFieldScript.CONTEXT);
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

                    private GeoPointFieldScript.Factory factory(String code) {
                        switch (code) {
                            case "fromLatLon":
                                return (fieldName, params, lookup) -> (ctx) -> new GeoPointFieldScript(fieldName, params, lookup, ctx) {
                                    @Override
                                    public void execute() {
                                        Map<?, ?> foo = (Map<?, ?>) lookup.source().get("foo");
                                        emit(((Number) foo.get("lat")).doubleValue(), ((Number) foo.get("lon")).doubleValue());
                                    }
                                };
                            case "loop":
                                return (fieldName, params, lookup) -> {
                                    // Indicate that this script wants the field call "test", which *is* the name of this field
                                    lookup.forkAndTrackFieldReferences("test");
                                    throw new IllegalStateException("shoud have thrown on the line above");
                                };
                            default:
                                throw new IllegalArgumentException("unsupported script [" + code + "]");
                        }
                    }
                };
            }
        };
        ScriptModule scriptModule = new ScriptModule(Settings.EMPTY, List.of(scriptPlugin, new RuntimeFields(Settings.EMPTY)));
        try (ScriptService scriptService = new ScriptService(Settings.EMPTY, scriptModule.engines, scriptModule.contexts)) {
            GeoPointFieldScript.Factory factory = scriptService.compile(script, GeoPointFieldScript.CONTEXT);
            return new GeoPointScriptFieldType("test", factory, script, emptyMap(), (b, d) -> {});
        }
    }
}
