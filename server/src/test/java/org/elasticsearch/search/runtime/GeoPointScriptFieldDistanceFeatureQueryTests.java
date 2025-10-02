/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.geo.GeoTestUtil;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.index.mapper.GeoPointScriptFieldType;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.OnScriptError;
import org.elasticsearch.index.mapper.SourceFieldMetrics;
import org.elasticsearch.script.AbstractLongFieldScript;
import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;
import org.elasticsearch.search.lookup.SourceProvider;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

public class GeoPointScriptFieldDistanceFeatureQueryTests extends AbstractScriptFieldQueryTestCase<
    GeoPointScriptFieldDistanceFeatureQuery> {
    private final Function<LeafReaderContext, AbstractLongFieldScript> leafFactory = ctx -> null;

    @Override
    protected GeoPointScriptFieldDistanceFeatureQuery createTestInstance() {
        double lat = GeoTestUtil.nextLatitude();
        double lon = GeoTestUtil.nextLongitude();
        double pivot = randomDouble() * GeoUtils.EARTH_EQUATOR;
        return new GeoPointScriptFieldDistanceFeatureQuery(randomScript(), leafFactory, randomAlphaOfLength(5), lat, lon, pivot);
    }

    @Override
    protected GeoPointScriptFieldDistanceFeatureQuery copy(GeoPointScriptFieldDistanceFeatureQuery orig) {
        return new GeoPointScriptFieldDistanceFeatureQuery(
            orig.script(),
            leafFactory,
            orig.fieldName(),
            orig.lat(),
            orig.lon(),
            orig.pivot()
        );
    }

    @Override
    protected GeoPointScriptFieldDistanceFeatureQuery mutate(GeoPointScriptFieldDistanceFeatureQuery orig) {
        Script script = orig.script();
        String fieldName = orig.fieldName();
        double lat = orig.lat();
        double lon = orig.lon();
        double pivot = orig.pivot();
        switch (randomInt(4)) {
            case 0 -> script = randomValueOtherThan(script, this::randomScript);
            case 1 -> fieldName += "modified";
            case 2 -> lat = randomValueOtherThan(lat, GeoTestUtil::nextLatitude);
            case 3 -> lon = randomValueOtherThan(lon, GeoTestUtil::nextLongitude);
            case 4 -> pivot = randomValueOtherThan(pivot, () -> randomDouble() * GeoUtils.EARTH_EQUATOR);
            default -> fail();
        }
        return new GeoPointScriptFieldDistanceFeatureQuery(script, leafFactory, fieldName, lat, lon, pivot);
    }

    @Override
    public void testMatches() throws IOException {
        IndexWriterConfig config = LuceneTestCase.newIndexWriterConfig(random(), new MockAnalyzer(random()));
        // Use LogDocMergePolicy to avoid randomization issues with the doc retrieval order.
        config.setMergePolicy(new LogDocMergePolicy());
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory, config)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"location\": [34, 6]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"location\": [-3.56, -45.98]}"))));

            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchLookup searchLookup = new SearchLookup(
                    null,
                    null,
                    SourceProvider.fromLookup(MappingLookup.EMPTY, null, SourceFieldMetrics.NOOP)
                );
                Function<LeafReaderContext, GeoPointFieldScript> leafFactory = ctx -> new GeoPointFieldScript(
                    "test",
                    Map.of(),
                    searchLookup,
                    OnScriptError.FAIL,
                    ctx
                ) {
                    @Override
                    @SuppressWarnings("unchecked")
                    public void execute() {
                        Map<String, Object> source = (Map<String, Object>) this.getParams().get("_source");
                        GeoPoint point = GeoUtils.parseGeoPoint(source.get("location"), true);
                        emit(point.lat(), point.lon());
                    }
                };
                GeoPointScriptFieldDistanceFeatureQuery query = new GeoPointScriptFieldDistanceFeatureQuery(
                    randomScript(),
                    GeoPointScriptFieldType.valuesEncodedAsLong(searchLookup, "test", leafFactory),
                    "test",
                    0,
                    0,
                    30000
                );
                TopDocs td = searcher.search(query, 2);
                assertThat(td.scoreDocs[0].score, equalTo(0.0077678584F));
                assertThat(td.scoreDocs[0].doc, equalTo(0));
                assertThat(td.scoreDocs[1].score, equalTo(0.005820022F));
                assertThat(td.scoreDocs[1].doc, equalTo(1));
            }
        }
    }

    public void testMaxScore() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of());
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                GeoPointScriptFieldDistanceFeatureQuery query = createTestInstance();
                float boost = randomFloat();
                assertThat(
                    query.createWeight(searcher, ScoreMode.COMPLETE, boost).scorer(reader.leaves().get(0)).getMaxScore(randomInt()),
                    equalTo(boost)
                );
            }
        }
    }

    @Override
    protected void assertToString(GeoPointScriptFieldDistanceFeatureQuery query) {
        assertThat(
            query.toString(query.fieldName()),
            equalTo("GeoPointScriptFieldDistanceFeatureQuery(lat=" + query.lat() + ",lon=" + query.lon() + ",pivot=" + query.pivot() + ")")
        );
    }

    @Override
    public final void testVisit() {
        assertEmptyVisit();
    }
}
