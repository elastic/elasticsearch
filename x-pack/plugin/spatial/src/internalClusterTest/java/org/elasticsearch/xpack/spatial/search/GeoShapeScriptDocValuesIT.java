/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class GeoShapeScriptDocValuesIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(LocalStateSpatialPlugin.class, LocalStateCompositeXPackPlugin.class, CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("lat", this::scriptLat);
            scripts.put("lon", this::scriptLon);
            scripts.put("height", this::scriptHeight);
            scripts.put("width", this::scriptWidth);
            return scripts;
        }


        private double scriptHeight(Map<String, Object> vars) {
            Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
            ScriptDocValues.Geometry<?> geometry = assertGeometry(doc);
            if (geometry.size() == 0) {
                return Double.NaN;
            } else {
                GeoBoundingBox boundingBox = geometry.getBoundingBox();
                return boundingBox.topLeft().lat() - boundingBox.bottomRight().lat();
            }
        }

        private double scriptWidth(Map<String, Object> vars) {
            Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
            ScriptDocValues.Geometry<?> geometry = assertGeometry(doc);
            if (geometry.size() == 0) {
                return Double.NaN;
            } else {
                GeoBoundingBox boundingBox = geometry.getBoundingBox();
                return boundingBox.bottomRight().lon() - boundingBox.topLeft().lon();
            }
        }

        private double scriptLat(Map<String, Object> vars) {
            Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
            ScriptDocValues.Geometry<?> geometry = assertGeometry(doc);
            return geometry.size() == 0 ? Double.NaN : geometry.getCentroid().lat();
        }

        private double scriptLon(Map<String, Object> vars) {
            Map<?, ?> doc = (Map<?, ?>) vars.get("doc");
            ScriptDocValues.Geometry<?> geometry = assertGeometry(doc);
            return geometry.size() == 0 ? Double.NaN : geometry.getCentroid().lon();
        }

        private ScriptDocValues.Geometry<?> assertGeometry(Map<?, ?> doc) {
            ScriptDocValues.Geometry<?> geometry = (ScriptDocValues.Geometry<?>) doc.get("location");
            if (geometry.size() == 0) {
                assertThat(geometry.getBoundingBox(), Matchers.nullValue());
                assertThat(geometry.getCentroid(), Matchers.nullValue());
                assertThat(geometry.getDimensionalType(), equalTo(-1));
            } else {
                assertThat(geometry.getBoundingBox(), Matchers.notNullValue());
                assertThat(geometry.getCentroid(), Matchers.notNullValue());
                assertThat(geometry.getDimensionalType(), greaterThanOrEqualTo(0));
                assertThat(geometry.getDimensionalType(), lessThanOrEqualTo(2));
            }
            return geometry;
        }
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Before
    public void setupTestIndex() throws IOException {
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("_doc")
            .startObject("properties").startObject("location").field("type", "geo_shape");
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(client().admin().indices().prepareCreate("test").setMapping(xContentBuilder));
        ensureGreen();
    }

    public void testRandomShape() throws Exception {
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        Geometry geometry = indexer.prepareForIndexing(randomValueOtherThanMany(g -> {
            try {
                indexer.prepareForIndexing(g);
                return false;
            } catch (Exception e) {
                return true;
            }
        }, () -> GeometryTestUtils.randomGeometry(false)));
        doTestGeometry(geometry);
    }

    public void testPolygonDateline() throws Exception {
        Geometry geometry = new Polygon(new LinearRing(new double[]{170, 190, 190, 170, 170}, new double[]{-5, -5, 5, 5, -5}));
        doTestGeometry(geometry);
    }

    private void doTestGeometry(Geometry geometry) throws IOException  {
        client().prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject()
                .field("name", "TestPosition")
                .field("location", WellKnownText.toWKT(geometry))
                .endObject())
            .get();

        client().admin().indices().prepareRefresh("test").get();

        GeoShapeValues.GeoShapeValue value = GeoTestUtils.geoShapeValue(geometry);

        SearchResponse searchResponse = client().prepareSearch().addStoredField("_source")
            .addScriptField("lat", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "lat", Collections.emptyMap()))
            .addScriptField("lon", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "lon", Collections.emptyMap()))
            .addScriptField("height", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "height", Collections.emptyMap()))
            .addScriptField("width", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "width", Collections.emptyMap()))
            .get();
        assertSearchResponse(searchResponse);
        Map<String, DocumentField> fields = searchResponse.getHits().getHits()[0].getFields();
        assertThat(fields.get("lat").getValue(), equalTo(value.lat()));
        assertThat(fields.get("lon").getValue(), equalTo(value.lon()));
        assertThat(fields.get("height").getValue(), equalTo(value.boundingBox().maxY() - value.boundingBox().minY()));
        assertThat(fields.get("width").getValue(), equalTo(value.boundingBox().maxX() - value.boundingBox().minX()));
    }

    public void testNullShape() throws Exception {
        client().prepareIndex("test").setId("1")
            .setSource(jsonBuilder().startObject()
                .field("name", "TestPosition")
                .nullField("location")
                .endObject())
            .get();

        client().admin().indices().prepareRefresh("test").get();

        SearchResponse searchResponse = client().prepareSearch().addStoredField("_source")
            .addScriptField("lat", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "lat", Collections.emptyMap()))
            .addScriptField("lon", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "lon", Collections.emptyMap()))
            .addScriptField("height", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "height", Collections.emptyMap()))
            .addScriptField("width", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "width", Collections.emptyMap()))
            .get();
        assertSearchResponse(searchResponse);
        Map<String, DocumentField> fields = searchResponse.getHits().getHits()[0].getFields();
        assertThat(fields.get("lat").getValue(), equalTo(Double.NaN));
        assertThat(fields.get("lon").getValue(), equalTo(Double.NaN));
        assertThat(fields.get("height").getValue(), equalTo(Double.NaN));
        assertThat(fields.get("width").getValue(), equalTo(Double.NaN));
    }
}
