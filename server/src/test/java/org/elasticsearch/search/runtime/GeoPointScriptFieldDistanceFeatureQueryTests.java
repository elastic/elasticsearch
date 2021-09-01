/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.apache.lucene.document.StoredField;
import org.apache.lucene.geo.GeoTestUtil;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.script.AbstractLongFieldScript;
import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.lookup.SearchLookup;

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
            case 0:
                script = randomValueOtherThan(script, this::randomScript);
                break;
            case 1:
                fieldName += "modified";
                break;
            case 2:
                lat = randomValueOtherThan(lat, GeoTestUtil::nextLatitude);
                break;
            case 3:
                lon = randomValueOtherThan(lon, GeoTestUtil::nextLongitude);
                break;
            case 4:
                pivot = randomValueOtherThan(pivot, () -> randomDouble() * GeoUtils.EARTH_EQUATOR);
                break;
            default:
                fail();
        }
        return new GeoPointScriptFieldDistanceFeatureQuery(script, leafFactory, fieldName, lat, lon, pivot);
    }

    @Override
    public void testMatches() throws IOException {
        try (Directory directory = newDirectory(); RandomIndexWriter iw = new RandomIndexWriter(random(), directory)) {
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"location\": [34, 6]}"))));
            iw.addDocument(List.of(new StoredField("_source", new BytesRef("{\"location\": [-3.56, -45.98]}"))));
            try (DirectoryReader reader = iw.getReader()) {
                IndexSearcher searcher = newSearcher(reader);
                SearchLookup searchLookup = new SearchLookup(null, null);
                Function<LeafReaderContext, AbstractLongFieldScript> leafFactory = ctx -> new GeoPointFieldScript(
                    "test",
                    Map.of(),
                    searchLookup,
                    ctx
                ) {
                    final GeoPoint point = new GeoPoint();

                    @Override
                    public void execute() {
                        GeoUtils.parseGeoPoint(searchLookup.source().get("location"), point, true);
                        emit(point.lat(), point.lon());
                    }
                };
                GeoPointScriptFieldDistanceFeatureQuery query = new GeoPointScriptFieldDistanceFeatureQuery(
                    randomScript(),
                    leafFactory,
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
