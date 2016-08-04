/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.geo;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptPlugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.script.ScriptService.ScriptType;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.closeTo;

public class GeoDistanceIT extends ESIntegTestCase {

    private static final double source_lat = 32.798;
    private static final double source_long = -117.151;
    private static final double target_lat = 32.81;
    private static final double target_long = -117.21;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(CustomScriptPlugin.class, InternalSettingsPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        @SuppressWarnings("unchecked")
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("arcDistance", vars -> distanceScript(vars,
                    location -> location.arcDistance(target_lat, target_long)));
            scripts.put("distance", vars -> distanceScript(vars,
                    location -> location.distance(target_lat, target_long)));
            scripts.put("arcDistanceInKm", vars -> distanceScript(vars,
                    location -> location.arcDistanceInKm(target_lat, target_long)));
            scripts.put("distanceInKm", vars -> distanceScript(vars,
                    location -> location.distanceInKm(target_lat, target_long)));
            scripts.put("arcDistanceInKm(lat, lon + 360)", vars -> distanceScript(vars,
                    location -> location.arcDistanceInKm(target_lat, target_long + 360)));
            scripts.put("arcDistanceInKm(lat + 360, lon)", vars -> distanceScript(vars,
                    location -> location.arcDistanceInKm(target_lat + 360, target_long)));
            scripts.put("arcDistanceInMiles", vars -> distanceScript(vars,
                    location -> location.arcDistanceInMiles(target_lat, target_long)));
            scripts.put("distanceInMiles", vars -> distanceScript(vars,
                    location -> location.distanceInMiles(target_lat, target_long)));

            return scripts;
        }

        @SuppressWarnings("unchecked")
        static Double distanceScript(Map<String, Object> vars, Function<ScriptDocValues.GeoPoints, Double> distance) {
            Map<?, ?> doc = (Map) vars.get("doc");
            return distance.apply((ScriptDocValues.GeoPoints) doc.get("location"));
        }
    }

    public void testDistanceScript() throws Exception {

        Version version = VersionUtils.randomVersionBetween(random(), Version.V_2_0_0, Version.CURRENT);
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location").field("type", "geo_point");
        if (version.before(Version.V_2_2_0)) {
            xContentBuilder.field("lat_lon", true);
        }
        xContentBuilder.endObject().endObject().endObject().endObject();
        assertAcked(prepareCreate("test").setSettings(settings).addMapping("type1", xContentBuilder));
        ensureGreen();

        client().prepareIndex("test", "type1", "1")
                .setSource(jsonBuilder().startObject()
                        .field("name", "TestPosition")
                        .startObject("location")
                        .field("lat", source_lat)
                        .field("lon", source_long)
                        .endObject()
                        .endObject())
                .get();

        refresh();

        // Test doc['location'].arcDistance(lat, lon)
        SearchResponse searchResponse1 = client().prepareSearch().addStoredField("_source")
                .addScriptField("distance", new Script("arcDistance", ScriptType.INLINE, CustomScriptPlugin.NAME, null))
                .get();
        Double resultDistance1 = searchResponse1.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance1,
                closeTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.DEFAULT), 0.01d));

        // Test doc['location'].distance(lat, lon)
        SearchResponse searchResponse2 = client().prepareSearch().addStoredField("_source")
                .addScriptField("distance", new Script("distance", ScriptType.INLINE, CustomScriptPlugin.NAME, null))
                .get();
        Double resultDistance2 = searchResponse2.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance2,
                closeTo(GeoDistance.PLANE.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.DEFAULT), 0.01d));

        // Test doc['location'].arcDistanceInKm(lat, lon)
        SearchResponse searchResponse3 = client().prepareSearch().addStoredField("_source")
                .addScriptField("distance", new Script("arcDistanceInKm", ScriptType.INLINE, CustomScriptPlugin.NAME, null))
                .get();
        Double resultArcDistance3 = searchResponse3.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultArcDistance3,
                closeTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.KILOMETERS), 0.01d));

        // Test doc['location'].distanceInKm(lat, lon)
        SearchResponse searchResponse4 = client().prepareSearch().addStoredField("_source")
                .addScriptField("distance", new Script("distanceInKm", ScriptType.INLINE, CustomScriptPlugin.NAME, null))
                .get();
        Double resultDistance4 = searchResponse4.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance4,
                closeTo(GeoDistance.PLANE.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.KILOMETERS), 0.01d));

        // Test doc['location'].arcDistanceInKm(lat, lon + 360)
        SearchResponse searchResponse5 = client().prepareSearch().addStoredField("_source")
                .addScriptField("distance", new Script("arcDistanceInKm(lat, lon + 360)", ScriptType.INLINE, CustomScriptPlugin.NAME, null))
                .get();
        Double resultArcDistance5 = searchResponse5.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultArcDistance5,
                closeTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.KILOMETERS), 0.01d));

        // Test doc['location'].arcDistanceInKm(lat + 360, lon)
        SearchResponse searchResponse6 = client().prepareSearch().addStoredField("_source")
                .addScriptField("distance", new Script("arcDistanceInKm(lat + 360, lon)", ScriptType.INLINE, CustomScriptPlugin.NAME, null))
                .get();
        Double resultArcDistance6 = searchResponse6.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultArcDistance6,
                closeTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.KILOMETERS), 0.01d));

        // Test doc['location'].arcDistanceInMiles(lat, lon)
        SearchResponse searchResponse7 = client().prepareSearch().addStoredField("_source")
                .addScriptField("distance", new Script("arcDistanceInMiles", ScriptType.INLINE, CustomScriptPlugin.NAME, null))
                .get();
        Double resultDistance7 = searchResponse7.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance7,
                closeTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.MILES), 0.01d));

        // Test doc['location'].distanceInMiles(lat, lon)
        SearchResponse searchResponse8 = client().prepareSearch().addStoredField("_source")
                .addScriptField("distance", new Script("distanceInMiles", ScriptType.INLINE, CustomScriptPlugin.NAME, null))
                .get();
        Double resultDistance8 = searchResponse8.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance8,
                closeTo(GeoDistance.PLANE.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.MILES), 0.01d));
    }
}
