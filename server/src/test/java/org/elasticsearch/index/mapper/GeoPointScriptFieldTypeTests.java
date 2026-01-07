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
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.GeometryFormatterFactory;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.GeoPointScriptFieldData;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.script.DocReader;
import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.script.ScoreScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptFactory;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.Source;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.index.mapper.GeoPointFieldMapper.GeoPointFieldType.GEO_FORMATTER_FACTORY;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class GeoPointScriptFieldTypeTests extends AbstractNonTextScriptFieldTypeTestCase {

    private static final GeoPoint EMPTY_POINT = null;
    private static final GeoPoint MALFORMED_POINT = null;

    private static final Function<List<GeoPoint>, List<Object>> FORMATTER = GEO_FORMATTER_FACTORY.getFormatter(
        GeometryFormatterFactory.WKB,
        p -> {
            if (p != null) {
                return new org.elasticsearch.geometry.Point(p.getLon(), p.getLat());
            }
            return null;
        }
    );

    @Override
    protected ScriptFactory parseFromSource() {
        return GeoPointFieldScript.PARSE_FROM_SOURCE;
    }

    @Override
    protected ScriptFactory dummyScript() {
        return GeoPointFieldScriptTests.DUMMY;
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
    public void testDocValues() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": {\"lat\": 45.0, \"lon\" : 45.0}}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": {\"lat\": 0.0, \"lon\" : 0.0}}"))));
            List<Object> results = new ArrayList<>();
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                GeoPointScriptFieldType ft = build("fromLatLon", Map.of(), OnScriptError.FAIL);
                GeoPointScriptFieldData ifd = ft.fielddataBuilder(mockFielddataContext()).build(null, null);
                searcher.search(Queries.ALL_DOCS_INSTANCE, new Collector() {
                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }

                    @Override
                    public LeafCollector getLeafCollector(LeafReaderContext context) {
                        MultiGeoPointValues dv = ifd.load(context).getPointValues();
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
                assertThat(results, containsInAnyOrder(new GeoPoint(45.0, 45.0), new GeoPoint(0.0, 0.0)));
            }
        }
    }

    @Override
    public void testSort() throws IOException {
        GeoPointScriptFieldData ifd = simpleMappedFieldType().fielddataBuilder(mockFielddataContext()).build(null, null);
        Exception e = expectThrows(IllegalArgumentException.class, () -> ifd.sortField(null, MultiValueMode.MIN, null, false));
        assertThat(e.getMessage(), equalTo("can't sort on geo_point field without using specific sorting feature, like geo_distance"));
    }

    public void testFetch() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("""
                {"foo": {"lat": 45.0, "lon" : 45.0}}"""))));
            try (DirectoryReader reader = iw.getReader()) {
                SearchExecutionContext searchContext = mockContext(true, simpleMappedFieldType());
                Source source = searchContext.lookup().getSource(reader.leaves().get(0), 0);
                ValueFetcher fetcher = simpleMappedFieldType().valueFetcher(searchContext, randomBoolean() ? null : "geojson");
                fetcher.setNextReader(reader.leaves().get(0));
                assertThat(
                    fetcher.fetchValues(source, 0, null),
                    equalTo(List.of(Map.of("type", "Point", "coordinates", List.of(45.0, 45.0))))
                );
                fetcher = simpleMappedFieldType().valueFetcher(searchContext, "wkt");
                fetcher.setNextReader(reader.leaves().get(0));
                assertThat(fetcher.fetchValues(source, 0, null), equalTo(List.of("POINT (45.0 45.0)")));
            }
        }
    }

    @Override
    public void testUsedInScript() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": {\"lat\": 45.0, \"lon\" : 45.0}}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": {\"lat\": 0.0, \"lon\" : 0.0}}"))));
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
                                    ScriptDocValues.GeoPoints points = (ScriptDocValues.GeoPoints) getDoc().get("test");
                                    return (int) points.get(0).lat() + 1;
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
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": {\"lat\": 45.0, \"lon\" : 45.0}}"))));
            addDocument(iw, List.of(new StoredField("_source", new BytesRef("{\"foo\": {\"lat\": 0.0, \"lon\" : 0.0}}"))));
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

    public void testBlockLoaderSourceOnlyRuntimeFieldWithSyntheticSource() throws IOException {
        try (
            Directory directory = newDirectory();
            RandomIndexWriter iw = new RandomIndexWriter(random(), directory, newIndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))
        ) {
            // given
            iw.addDocuments(
                List.of(
                    createDocumentWithIgnoredSource("""
                        {"type": "Point", "coordinates": [11.22, -33.44]}
                        """),
                    createDocumentWithIgnoredSource("""
                        ["POINT (23.45 -56.78)"]
                        """),
                    createDocumentWithIgnoredSource("""
                        {"lat": 22.33, "lon" : -44.55}}
                        """),
                    createDocumentWithIgnoredSource("""
                        [-33.44, 55.66]
                        """),
                    createDocumentWithIgnoredSource("""
                        [-44.55, 66.77, 88.99]
                        """),
                    createDocumentWithIgnoredSource("""
                        ["55.66,77.88"]
                        """),
                    createDocumentWithIgnoredSource("""
                        ["drm3btev3e86"]
                        """),
                    // ensure empty points can be parsed
                    createDocumentWithIgnoredSource("""
                        []
                        """),
                    // ensure a malformed value doesn't crash
                    createDocumentWithIgnoredSource("""
                        ["potato", "tomato"]
                        """)
                )
            );

            Settings settings = Settings.builder().put("index.mapping.source.mode", "synthetic").build();
            GeoPointScriptFieldType fieldType = simpleSourceOnlyMappedFieldType();

            // note, the order of coordinates here differs from the documents above. This is expected - x and y coordinates get mapped to
            // lon and lat differently depending on the underlying type of the geo point itself. Sometimes they match 1:1, others they swap
            List<Object> expectedPoints = FORMATTER.apply(
                Arrays.asList(
                    new GeoPoint(-33.44, 11.22),
                    new GeoPoint(-56.78, 23.45),
                    new GeoPoint(22.33, -44.55),
                    new GeoPoint(55.66, -33.44),
                    new GeoPoint(66.77, -44.55),
                    new GeoPoint(55.66, 77.88),
                    new GeoPoint("drm3btev3e86")
                )
            );

            // add these separately because the formatter doesn't like nulls
            expectedPoints.add(EMPTY_POINT);
            expectedPoints.add(MALFORMED_POINT);

            // expected points converted to BytesRef
            List<BytesRef> expected = expectedPoints.stream().map(gp -> {
                if (gp instanceof byte[] wkb) {
                    return new BytesRef(wkb);
                }
                return null;
            }).toList();

            try (DirectoryReader reader = iw.getReader()) {
                // when
                BlockLoader loader = fieldType.blockLoader(blContext(settings, true));

                // then

                // assert loader is of expected instance type
                assertThat(loader, instanceOf(FallbackSyntheticSourceBlockLoader.class));

                // ignored source doesn't support column at a time loading:
                var columnAtATimeLoader = loader.columnAtATimeReader(reader.leaves().getFirst());
                assertThat(columnAtATimeLoader, nullValue());

                var rowStrideReader = loader.rowStrideReader(reader.leaves().getFirst());
                assertThat(
                    rowStrideReader.getClass().getName(),
                    equalTo("org.elasticsearch.index.mapper.FallbackSyntheticSourceBlockLoader$IgnoredSourceRowStrideReader")
                );

                // assert values
                assertThat(blockLoaderReadValuesFromRowStrideReader(settings, reader, fieldType, true), equalTo(expected));
            }
        }
    }

    /**
     * Returns a source only mapped field type. This is useful, since the available build() function doesn't override isParsedFromSource()
     */
    private GeoPointScriptFieldType simpleSourceOnlyMappedFieldType() {
        Script script = new Script(ScriptType.INLINE, "test", "", emptyMap());
        GeoPointFieldScript.Factory factory = new GeoPointFieldScript.Factory() {
            @Override
            public GeoPointFieldScript.LeafFactory newFactory(
                String fieldName,
                Map<String, Object> params,
                SearchLookup searchLookup,
                OnScriptError onScriptError
            ) {
                return ctx -> new GeoPointFieldScript(fieldName, params, searchLookup, onScriptError, ctx) {
                    @Override
                    @SuppressWarnings("unchecked")
                    public void execute() {
                        Map<String, Object> source = (Map<String, Object>) this.getParams().get("_source");
                        for (Object foo : (List<?>) source.get("test")) {
                            try {
                                // ignore the Z coordinate because we don't care about it anyway
                                // this conversion matches GeoPointFieldScript.emitPoint()
                                GeoPoint gp = GeoUtils.parseGeoPoint(foo, true);
                                emit(gp.lat(), gp.lon());
                            } catch (Exception e) {
                                // skip
                            }
                        }
                    }
                };
            }

            @Override
            public boolean isParsedFromSource() {
                return true;
            }
        };
        return new GeoPointScriptFieldType("test", factory, script, emptyMap(), OnScriptError.FAIL);
    }

    @Override
    protected Query randomTermsQuery(MappedFieldType ft, SearchExecutionContext ctx) {
        return ft.termsQuery(randomList(100, GeometryTestUtils::randomPoint), mockContext());
    }

    @Override
    protected GeoPointScriptFieldType simpleMappedFieldType() {
        return build("fromLatLon", Map.of(), OnScriptError.FAIL);
    }

    @Override
    protected MappedFieldType loopFieldType() {
        return build("loop", Map.of(), OnScriptError.FAIL);
    }

    @Override
    protected String typeName() {
        return "geo_point";
    }

    protected GeoPointScriptFieldType build(String code, Map<String, Object> params, OnScriptError onScriptError) {
        Script script = new Script(ScriptType.INLINE, "test", code, params);
        return new GeoPointScriptFieldType("test", factory(script), script, emptyMap(), onScriptError);
    }

    private static GeoPointFieldScript.Factory factory(Script script) {
        return switch (script.getIdOrCode()) {
            case "fromLatLon" -> (fieldName, params, lookup, onScriptError) -> (ctx) -> new GeoPointFieldScript(
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
                    Map<?, ?> foo = (Map<?, ?>) source.get("foo");
                    emit(((Number) foo.get("lat")).doubleValue(), ((Number) foo.get("lon")).doubleValue());
                }
            };
            case "loop" -> (fieldName, params, lookup, onScriptError) -> {
                // Indicate that this script wants the field call "test", which *is* the name of this field
                lookup.forkAndTrackFieldReferences("test");
                throw new IllegalStateException("should have thrown on the line above");
            };
            case "error" -> (fieldName, params, lookup, onScriptError) -> ctx -> new GeoPointFieldScript(
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
