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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.LegacyGeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;

import java.io.IOException;

import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class LegacyGeoShapeIntegrationIT extends ESIntegTestCase {

    /**
     * Test that orientation parameter correctly persists across cluster restart
     */
    public void testOrientationPersistence() throws Exception {
        String idxName = "orientation";
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .field("orientation", "left")
                .endObject()
                .endObject().endObject());

        // create index
        assertAcked(prepareCreate(idxName).setMapping(mapping));

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .field("orientation", "right")
                .endObject()
                .endObject().endObject());

        assertAcked(prepareCreate(idxName+"2").setMapping(mapping));
        ensureGreen(idxName, idxName+"2");

        internalCluster().fullRestart();
        ensureGreen(idxName, idxName+"2");

        // left orientation test
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, findNodeName(idxName));
        IndexService indexService = indicesService.indexService(resolveIndex(idxName));
        MappedFieldType fieldType = indexService.mapperService().fieldType("location");
        assertThat(fieldType, instanceOf(LegacyGeoShapeFieldMapper.GeoShapeFieldType.class));

        LegacyGeoShapeFieldMapper.GeoShapeFieldType gsfm = (LegacyGeoShapeFieldMapper.GeoShapeFieldType)fieldType;
        ShapeBuilder.Orientation orientation = gsfm.orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.LEFT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CW));

        // right orientation test
        indicesService = internalCluster().getInstance(IndicesService.class, findNodeName(idxName+"2"));
        indexService = indicesService.indexService(resolveIndex((idxName+"2")));
        fieldType = indexService.mapperService().fieldType("location");
        assertThat(fieldType, instanceOf(LegacyGeoShapeFieldMapper.GeoShapeFieldType.class));

        gsfm = (LegacyGeoShapeFieldMapper.GeoShapeFieldType)fieldType;
        orientation = gsfm.orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.RIGHT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CCW));
    }

    /**
     * Test that ignore_malformed on GeoShapeFieldMapper does not fail the entire document
     */
    public void testIgnoreMalformed() throws Exception {
        // create index
        assertAcked(client().admin().indices().prepareCreate("test")
            .setMapping("shape", "type=geo_shape,tree=quadtree,ignore_malformed=true").get());
        ensureGreen();

        // test self crossing ccw poly not crossing dateline
        String polygonGeoJson = Strings.toString(XContentFactory.jsonBuilder().startObject().field("type", "Polygon")
            .startArray("coordinates")
            .startArray()
            .startArray().value(176.0).value(15.0).endArray()
            .startArray().value(-177.0).value(10.0).endArray()
            .startArray().value(-177.0).value(-10.0).endArray()
            .startArray().value(176.0).value(-15.0).endArray()
            .startArray().value(-177.0).value(15.0).endArray()
            .startArray().value(172.0).value(0.0).endArray()
            .startArray().value(176.0).value(15.0).endArray()
            .endArray()
            .endArray()
            .endObject());

        indexRandom(true, client().prepareIndex("test").setId("0").setSource("shape",
            polygonGeoJson));
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(matchAllQuery()).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    /**
     * Test that the indexed shape routing can be provided if it is required
     */
    public void testIndexShapeRouting() throws Exception {
        String mapping = "{\"_doc\":{\n" +
            "    \"_routing\": {\n" +
            "      \"required\": true\n" +
            "    },\n" +
            "    \"properties\": {\n" +
            "      \"shape\": {\n" +
            "        \"type\": \"geo_shape\",\n" +
            "        \"tree\" : \"quadtree\"\n" +
            "      }\n" +
            "    }\n" +
            "  }}";


        // create index
        assertAcked(client().admin().indices().prepareCreate("test").setMapping(mapping).get());
        ensureGreen();

        String source = "{\n" +
            "    \"shape\" : {\n" +
            "        \"type\" : \"bbox\",\n" +
            "        \"coordinates\" : [[-45.0, 45.0], [45.0, -45.0]]\n" +
            "    }\n" +
            "}";

        indexRandom(true, client().prepareIndex("test").setId("0").setSource(source, XContentType.JSON).setRouting("ABC"));

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(
            geoShapeQuery("shape", "0").indexedShapeIndex("test").indexedShapeRouting("ABC")
        ).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    /**
     * Test that the circle is still supported for the legacy shapes
     */
    public void testLegacyCircle() throws Exception {
        // create index
        assertAcked(client().admin().indices().prepareCreate("test")
            .setMapping("shape", "type=geo_shape,strategy=recursive,tree=geohash").get());
        ensureGreen();

        indexRandom(true, client().prepareIndex("test").setId("0").setSource("shape", (ToXContent) (builder, params) -> {
            builder.startObject().field("type", "circle")
                .startArray("coordinates").value(30).value(50).endArray()
                .field("radius","77km")
                .endObject();
            return builder;
        }));

        // test self crossing of circles
        SearchResponse searchResponse = client().prepareSearch("test").setQuery(geoShapeQuery("shape",
            new Circle(30, 50, 77000))).get();
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    public void testDisallowExpensiveQueries() throws InterruptedException, IOException {
        try {
            // create index
            assertAcked(client().admin().indices().prepareCreate("test")
                    .setMapping("shape", "type=geo_shape,strategy=recursive,tree=geohash").get());
            ensureGreen();

            indexRandom(true, client().prepareIndex("test").setId("0").setSource(
                    "shape", (ToXContent) (builder, params) -> {
                        builder.startObject().field("type", "circle")
                                .startArray("coordinates").value(30).value(50).endArray()
                                .field("radius", "77km")
                                .endObject();
                        return builder;
                    }));
            refresh();

            // Execute with search.allow_expensive_queries = null => default value = false => success
            SearchResponse searchResponse = client().prepareSearch("test").setQuery(geoShapeQuery("shape",
                    new Circle(30, 50, 77000))).get();
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", false));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

            // Set search.allow_expensive_queries to "false" => assert failure
            ElasticsearchException e = expectThrows(ElasticsearchException.class,
                    () -> client().prepareSearch("test").setQuery(geoShapeQuery("shape",
                            new Circle(30, 50, 77000))).get());
            assertEquals("[geo-shape] queries on [PrefixTree geo shapes] cannot be executed when " +
                            "'search.allow_expensive_queries' is set to false.", e.getCause().getMessage());

            // Set search.allow_expensive_queries to "true" => success
            updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", true));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
            searchResponse = client().prepareSearch("test").setQuery(geoShapeQuery("shape",
                    new Circle(30, 50, 77000))).get();
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        } finally {
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", (String) null));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        }
    }

    private String findNodeName(String index) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexShardRoutingTable shard = state.getRoutingTable().index(index).shard(0);
        String nodeId = shard.assignedShards().get(0).currentNodeId();
        return state.getNodes().get(nodeId).getName();
    }
}
