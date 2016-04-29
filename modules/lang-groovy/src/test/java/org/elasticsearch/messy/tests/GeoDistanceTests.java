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

package org.elasticsearch.messy.tests;

import org.elasticsearch.Version;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.groovy.GroovyPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.VersionUtils;

import java.util.Collection;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.closeTo;

/**
 */
public class GeoDistanceTests extends ESIntegTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(GroovyPlugin.class, InternalSettingsPlugin.class);
    }

    public void testDistanceScript() throws Exception {
        double source_lat = 32.798;
        double source_long = -117.151;
        double target_lat = 32.81;
        double target_long = -117.21;

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

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "TestPosition")
                .startObject("location").field("lat", source_lat).field("lon", source_long).endObject()
                .endObject()).execute().actionGet();

        refresh();

        SearchResponse searchResponse1 = client().prepareSearch().addField("_source")
                .addScriptField("distance", new Script("doc['location'].arcDistance(" + target_lat + "," + target_long + ")")).execute()
                .actionGet();
        Double resultDistance1 = searchResponse1.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance1,
                closeTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.DEFAULT), 0.01d));

        SearchResponse searchResponse2 = client().prepareSearch().addField("_source")
                .addScriptField("distance", new Script("doc['location'].distance(" + target_lat + "," + target_long + ")")).execute()
                .actionGet();
        Double resultDistance2 = searchResponse2.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance2,
                closeTo(GeoDistance.PLANE.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.DEFAULT), 0.01d));

        SearchResponse searchResponse3 = client().prepareSearch().addField("_source")
                .addScriptField("distance", new Script("doc['location'].arcDistanceInKm(" + target_lat + "," + target_long + ")"))
                .execute().actionGet();
        Double resultArcDistance3 = searchResponse3.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultArcDistance3,
                closeTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.KILOMETERS), 0.01d));

        SearchResponse searchResponse4 = client().prepareSearch().addField("_source")
                .addScriptField("distance", new Script("doc['location'].distanceInKm(" + target_lat + "," + target_long + ")")).execute()
                .actionGet();
        Double resultDistance4 = searchResponse4.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance4,
                closeTo(GeoDistance.PLANE.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.KILOMETERS), 0.01d));

        SearchResponse searchResponse5 = client()
                .prepareSearch()
                .addField("_source")
                .addScriptField("distance", new Script("doc['location'].arcDistanceInKm(" + (target_lat) + "," + (target_long + 360) + ")"))
                .execute().actionGet();
        Double resultArcDistance5 = searchResponse5.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultArcDistance5,
                closeTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.KILOMETERS), 0.01d));

        SearchResponse searchResponse6 = client()
                .prepareSearch()
                .addField("_source")
                .addScriptField("distance", new Script("doc['location'].arcDistanceInKm(" + (target_lat + 360) + "," + (target_long) + ")"))
                .execute().actionGet();
        Double resultArcDistance6 = searchResponse6.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultArcDistance6,
                closeTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.KILOMETERS), 0.01d));

        SearchResponse searchResponse7 = client().prepareSearch().addField("_source")
                .addScriptField("distance", new Script("doc['location'].arcDistanceInMiles(" + target_lat + "," + target_long + ")"))
                .execute().actionGet();
        Double resultDistance7 = searchResponse7.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance7,
                closeTo(GeoDistance.ARC.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.MILES), 0.01d));

        SearchResponse searchResponse8 = client().prepareSearch().addField("_source")
                .addScriptField("distance", new Script("doc['location'].distanceInMiles(" + target_lat + "," + target_long + ")"))
                .execute().actionGet();
        Double resultDistance8 = searchResponse8.getHits().getHits()[0].getFields().get("distance").getValue();
        assertThat(resultDistance8,
                closeTo(GeoDistance.PLANE.calculate(source_lat, source_long, target_lat, target_long, DistanceUnit.MILES), 0.01d));
    }
}
