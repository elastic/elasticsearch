/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.mapper;

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
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoFormatterFactory;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.AbstractNonTextScriptFieldTypeTestCase;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.index.mapper.ValueFetcher;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.GeometryFieldScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.lookup.Source;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.AbstractAtomicGeoShapeShapeFieldData;
import org.elasticsearch.xpack.spatial.index.fielddata.plain.GeoShapeScriptFieldData;
import org.elasticsearch.xpack.vectortile.SpatialGeometryFormatterExtension;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.hamcrest.Matchers.equalTo;

public class GeoShapeScriptFieldTypeTests extends AbstractNonTextScriptFieldTypeTestCase {

    public static final GeometryFieldScript.Factory DUMMY = (fieldName, params, lookup, onScriptError) -> ctx -> new GeometryFieldScript(
        fieldName,
        params,
        lookup,
        OnScriptError.FAIL,
        ctx
    ) {
        @Override
        public void execute() {
            emitFromObject("LINESTRING(0 0, 1 1)");
        }
    };

    @Override
    protected Collection<Plugin> getPlugins() {
        return Collections.singletonList(new LocalStateSpatialPlugin());
    }

    @Override
    protected boolean supportsTermQueries() {
        return false;
    }

    @Override
    protected boolean supportsRangeQueries() {
        return false;
    }

    @Override
    protected ScriptFactory parseFromSource() {
        return GeometryFieldScript.PARSE_FROM_SOURCE;
    }

    @Override
    protected ScriptFactory dummyScript() {
        return DUMMY;
    }

    @Override
    public void testDocValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": \"LINESTRING(0.0 0.0, 1.0 1.0)\" }"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": \"LINESTRING(45.0 45.0, 3.0 3.0)\" }"))));
            List<Object> results = new ArrayList<>();
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                GeoShapeScriptFieldType ft = build("fromWKT", Map.of(), OnScriptError.FAIL);
                GeoShapeScriptFieldData ifd = ft.fielddataBuilder(mockFielddataContext()).build(null, null);
                searcher.search(new MatchAllDocsQuery(), new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafCollector getLeafCollector(LeafReaderContext context) {
                        GeoShapeValues dv = ifd.load(context).getShapeValues();
                        return new LeafCollector() {
                            @Override
                            public void setScorer(Scorable scorer) {}

                            @Override
                            public void collect(int doc) throws IOException {
                                if (dv.advanceExact(doc)) {
                                    results.add(dv.value());
                                }
                            }
                        };
                    }
                });
                assertEquals(2, results.size());
            }
        }
    }

    @Override
    public void testSort() throws IOException {
        GeoShapeScriptFieldData ifd = simpleMappedFieldType().fielddataBuilder(mockFielddataContext()).build(null, null);
        Exception e = expectThrows(IllegalArgumentException.class, () -> ifd.sortField(null, MultiValueMode.MIN, null, false));
        assertThat(e.getMessage(), equalTo("can't sort on geo_shape field"));
    }

    public void testFetch() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("""
                {"foo": {"coordinates": [[45.0, 45.0], [0.0, 0.0]], "type" : "LineString"}}"""))));
            try (DirectoryReader reader = iw.getReader()) {
                SearchExecutionContext searchContext = mockContext(true, simpleMappedFieldType());
                Source source = searchContext.lookup().getSource(reader.leaves().get(0), 0);
                ValueFetcher fetcher = simpleMappedFieldType().valueFetcher(searchContext, randomBoolean() ? null : "geojson");
                fetcher.setNextReader(reader.leaves().get(0));
                assertThat(
                    fetcher.fetchValues(source, 0, null),
                    equalTo(List.of(Map.of("type", "LineString", "coordinates", List.of(List.of(45.0, 45.0), List.of(0.0, 0.0)))))
                );
                fetcher = simpleMappedFieldType().valueFetcher(searchContext, "wkt");
                fetcher.setNextReader(reader.leaves().get(0));
                assertThat(fetcher.fetchValues(source, 0, null), equalTo(List.of("LINESTRING (45.0 45.0, 0.0 0.0)")));
            }
        }
    }

    @Override
    public void testUsedInScript() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": \"LINESTRING(0.0 0.0, 1.0 1.0)\" }"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": \"LINESTRING(45.0 45.0, 3.0 3.0)\" }"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchExecutionContext searchContext = mockContext(true, simpleMappedFieldType());
                assertThat(searcher.count(new ScriptScoreQuery(new MatchAllDocsQuery(), new Script("test"), new ScoreScript.LeafFactory() {
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
                                AbstractAtomicGeoShapeShapeFieldData.GeoShapeScriptValues values =
                                    (AbstractAtomicGeoShapeShapeFieldData.GeoShapeScriptValues) getDoc().get("test");
                                try {
                                    return values.get(0).getY() + 1;
                                } catch (IOException ioe) {
                                    throw new UncheckedIOException(ioe);
                                }
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
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": \"LINESTRING(0.0 0.0, 1.0 1.0)\" }"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": \"LINESTRING(45.0 45.0, 3.0 3.0)\" }"))));
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
    protected GeoShapeScriptFieldType simpleMappedFieldType() {
        return build("fromWKT", Map.of(), OnScriptError.FAIL);
    }

    @Override
    protected MappedFieldType loopFieldType() {
        return build("loop", Map.of(), OnScriptError.FAIL);
    }

    @Override
    protected String typeName() {
        return "geo_shape";
    }

    protected GeoShapeScriptFieldType build(String code, Map<String, Object> params, OnScriptError onScriptError) {
        final GeoFormatterFactory<Geometry> geoFormatterFactory = new GeoFormatterFactory<>(
            new SpatialGeometryFormatterExtension().getGeometryFormatterFactories()
        );
        Script script = new Script(ScriptType.INLINE, "test", code, params);
        return new GeoShapeScriptFieldType("test", factory(script), script, emptyMap(), onScriptError, geoFormatterFactory);
    }

    private static GeometryFieldScript.Factory factory(Script script) {
        return switch (script.getIdOrCode()) {
            case "fromWKT" -> (fieldName, params, lookup, onScriptError) -> (ctx) -> new GeometryFieldScript(
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
                    emitFromObject(source.get("foo"));
                }
            };
            case "loop" -> (fieldName, params, lookup, onScriptError) -> {
                // Indicate that this script wants the field call "test", which *is* the name of this field
                lookup.forkAndTrackFieldReferences("test");
                throw new IllegalStateException("should have thrown on the line above");
            };
            case "error" -> (fieldName, params, lookup, onScriptError) -> ctx -> new GeometryFieldScript(
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
