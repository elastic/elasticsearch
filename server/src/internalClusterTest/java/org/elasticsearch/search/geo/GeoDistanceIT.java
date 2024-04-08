/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.geo;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.range.InternalGeoDistance;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.closeTo;

public class GeoDistanceIT extends ESIntegTestCase {

    private static final double src_lat = 32.798;
    private static final double src_lon = -117.151;
    private static final double tgt_lat = 32.81;
    private static final double tgt_lon = -117.21;
    private static final String tgt_geohash = Geohash.stringEncode(tgt_lon, tgt_lat);

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("arcDistance", vars -> distanceScript(vars, location -> location.arcDistance(tgt_lat, tgt_lon)));
            scripts.put(
                "arcDistanceGeoUtils",
                vars -> distanceScript(vars, location -> GeoUtils.arcDistance(location.getLat(), location.getLon(), tgt_lat, tgt_lon))
            );
            scripts.put("planeDistance", vars -> distanceScript(vars, location -> location.planeDistance(tgt_lat, tgt_lon)));
            scripts.put("geohashDistance", vars -> distanceScript(vars, location -> location.geohashDistance(tgt_geohash)));
            scripts.put(
                "arcDistance(lat, lon + 360)/1000d",
                vars -> distanceScript(vars, location -> location.arcDistance(tgt_lat, tgt_lon + 360) / 1000d)
            );
            scripts.put(
                "arcDistance(lat + 360, lon)/1000d",
                vars -> distanceScript(vars, location -> location.arcDistance(tgt_lat + 360, tgt_lon) / 1000d)
            );

            return scripts;
        }

        static Double distanceScript(Map<String, Object> vars, Function<ScriptDocValues.GeoPoints, Double> distance) {
            Map<?, ?> doc = (Map) vars.get("doc");
            return distance.apply((ScriptDocValues.GeoPoints) doc.get("location"));
        }
    }

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Before
    public void setupTestIndex() throws IOException {
        IndexVersion version = IndexVersionUtils.randomCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("location")
            .field("type", "geo_point");
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).setMapping(xContentBuilder));
        ensureGreen();
    }

    public void testDistanceScript() throws Exception {
        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "TestPosition")
                    .startObject("location")
                    .field("lat", src_lat)
                    .field("lon", src_lon)
                    .endObject()
                    .endObject()
            )
            .get();

        refresh();

        // Test doc['location'].arcDistance(lat, lon)
        assertResponse(
            prepareSearch().addStoredField("_source")
                .addScriptField("distance", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "arcDistance", Collections.emptyMap())),
            response -> {
                Double resultDistance = response.getHits().getHits()[0].getFields().get("distance").getValue();
                assertThat(resultDistance, closeTo(GeoUtils.arcDistance(src_lat, src_lon, tgt_lat, tgt_lon), 0.01d));
            }
        );
        // Test doc['location'].planeDistance(lat, lon)
        assertResponse(
            prepareSearch().addStoredField("_source")
                .addScriptField(
                    "distance",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "planeDistance", Collections.emptyMap())
                ),
            response -> {
                Double resultDistance = response.getHits().getHits()[0].getFields().get("distance").getValue();
                assertThat(resultDistance, closeTo(GeoUtils.planeDistance(src_lat, src_lon, tgt_lat, tgt_lon), 0.01d));
            }
        );
        // Test doc['location'].geohashDistance(lat, lon)
        assertResponse(
            prepareSearch().addStoredField("_source")
                .addScriptField(
                    "distance",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "geohashDistance", Collections.emptyMap())
                ),
            response -> {
                Double resultDistance = response.getHits().getHits()[0].getFields().get("distance").getValue();
                assertThat(
                    resultDistance,
                    closeTo(
                        GeoUtils.arcDistance(src_lat, src_lon, Geohash.decodeLatitude(tgt_geohash), Geohash.decodeLongitude(tgt_geohash)),
                        0.01d
                    )
                );
            }
        );

        // Test doc['location'].arcDistance(lat, lon + 360)/1000d
        assertResponse(
            prepareSearch().addStoredField("_source")
                .addScriptField(
                    "distance",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "arcDistance(lat, lon + 360)/1000d", Collections.emptyMap())
                ),
            response -> {
                Double resultArcDistance = response.getHits().getHits()[0].getFields().get("distance").getValue();
                assertThat(resultArcDistance, closeTo(GeoUtils.arcDistance(src_lat, src_lon, tgt_lat, tgt_lon) / 1000d, 0.01d));
            }
        );
        // Test doc['location'].arcDistance(lat + 360, lon)/1000d
        assertResponse(
            prepareSearch().addStoredField("_source")
                .addScriptField(
                    "distance",
                    new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "arcDistance(lat + 360, lon)/1000d", Collections.emptyMap())
                ),
            response -> {
                Double resultArcDistance = response.getHits().getHits()[0].getFields().get("distance").getValue();
                assertThat(resultArcDistance, closeTo(GeoUtils.arcDistance(src_lat, src_lon, tgt_lat, tgt_lon) / 1000d, 0.01d));
            }
        );
    }

    public void testGeoDistanceAggregation() throws IOException {
        prepareIndex("test").setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "TestPosition")
                    .startObject("location")
                    .field("lat", src_lat)
                    .field("lon", src_lon)
                    .endObject()
                    .endObject()
            )
            .get();

        refresh();

        String name = "TestPosition";

        assertResponse(
            prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setSize(0) // no hits please
                .addAggregation(
                    AggregationBuilders.geoDistance(name, new GeoPoint(tgt_lat, tgt_lon))
                        .field("location")
                        .unit(DistanceUnit.MILES)
                        .addRange(0, 25000)
                ),
            response -> {
                InternalAggregations aggregations = response.getAggregations();
                assertNotNull(aggregations);
                InternalGeoDistance geoDistance = aggregations.get(name);
                assertNotNull(geoDistance);

                List<? extends Range.Bucket> buckets = ((Range) geoDistance).getBuckets();
                assertNotNull("Buckets should not be null", buckets);
                assertEquals("Unexpected number of buckets", 1, buckets.size());
                assertEquals("Unexpected doc count for geo distance", 1, buckets.get(0).getDocCount());
            }
        );
    }
}
