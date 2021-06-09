/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.geo;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.VersionUtils;

import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class GeoShapeIntegrationIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            // Check that only geo-shape queries on legacy PrefixTree based
            // geo shapes are disallowed.
            .put("search.allow_expensive_queries", false)
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .build();
    }

    /**
     * Test that orientation parameter correctly persists across cluster restart
     */
    public void testOrientationPersistence() throws Exception {
        String idxName = "orientation";
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("orientation", "left")
            .endObject()
            .endObject().endObject());

        // create index
        assertAcked(prepareCreate(idxName).setMapping(mapping));

        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
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
        assertThat(fieldType, instanceOf(GeoShapeFieldMapper.GeoShapeFieldType.class));

        GeoShapeFieldMapper.GeoShapeFieldType gsfm = (GeoShapeFieldMapper.GeoShapeFieldType)fieldType;
        Orientation orientation = gsfm.orientation();
        assertThat(orientation, equalTo(Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(Orientation.LEFT));
        assertThat(orientation, equalTo(Orientation.CW));

        // right orientation test
        indicesService = internalCluster().getInstance(IndicesService.class, findNodeName(idxName+"2"));
        indexService = indicesService.indexService(resolveIndex((idxName+"2")));
        fieldType = indexService.mapperService().fieldType("location");
        assertThat(fieldType, instanceOf(GeoShapeFieldMapper.GeoShapeFieldType.class));

        gsfm = (GeoShapeFieldMapper.GeoShapeFieldType)fieldType;
        orientation = gsfm.orientation();
        assertThat(orientation, equalTo(Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(Orientation.RIGHT));
        assertThat(orientation, equalTo(Orientation.CCW));
    }

    /**
     * Test that ignore_malformed on GeoShapeFieldMapper does not fail the entire document
     */
    public void testIgnoreMalformed() throws Exception {
        // create index
        assertAcked(client().admin().indices().prepareCreate("test")
            .setMapping("shape", "type=geo_shape,ignore_malformed=true").get());
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

    public void testMappingUpdate() {
        // create index
        final Version version = VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0);
        assertAcked(client().admin().indices().prepareCreate("test").setSettings(settings(version).build())
            .setMapping("shape", "type=geo_shape,strategy=recursive").get());
        ensureGreen();

        String update ="{\n" +
            "  \"properties\": {\n" +
            "    \"shape\": {\n" +
            "      \"type\": \"geo_shape\"" +
            "    }\n" +
            "  }\n" +
            "}";

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().admin().indices()
            .preparePutMapping("test")
            .setSource(update, XContentType.JSON).get());
        assertThat(e.getMessage(), containsString("mapper [shape] of type [geo_shape] cannot change strategy from [recursive] to [BKD]"));
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
            "        \"type\": \"geo_shape\"\n" +
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

    public void testIndexPolygonDateLine() throws Exception {
        String mappingVector = "{\n" +
            "    \"properties\": {\n" +
            "      \"shape\": {\n" +
            "        \"type\": \"geo_shape\"\n" +
            "      }\n" +
            "    }\n" +
            "  }";

        String mappingQuad = "{\n" +
            "    \"properties\": {\n" +
            "      \"shape\": {\n" +
            "        \"type\": \"geo_shape\",\n" +
            "        \"tree\": \"quadtree\"\n" +
            "      }\n" +
            "    }\n" +
            "  }";


        // create index
        assertAcked(client().admin().indices().prepareCreate("vector").setMapping(mappingVector).get());
        ensureGreen();

        final Version version = VersionUtils.randomPreviousCompatibleVersion(random(), Version.V_8_0_0);
        assertAcked(client().admin().indices().prepareCreate("quad")
            .setSettings(settings(version).build()).setMapping(mappingQuad).get());
        ensureGreen();

        String source = "{\n" +
            "    \"shape\" : \"POLYGON((179 0, -179 0, -179 2, 179 2, 179 0))\""+
            "}";

        indexRandom(true, client().prepareIndex("quad").setId("0").setSource(source, XContentType.JSON));
        indexRandom(true, client().prepareIndex("vector").setId("0").setSource(source, XContentType.JSON));

        try {
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", true));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());

            SearchResponse searchResponse = client().prepareSearch("quad").setQuery(
                geoShapeQuery("shape", new PointBuilder(-179.75, 1))
            ).get();

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

            searchResponse = client().prepareSearch("quad").setQuery(
                geoShapeQuery("shape", new PointBuilder(90, 1))
            ).get();

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));

            searchResponse = client().prepareSearch("quad").setQuery(
                geoShapeQuery("shape", new PointBuilder(-180, 1))
            ).get();

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

            searchResponse = client().prepareSearch("quad").setQuery(
                geoShapeQuery("shape", new PointBuilder(180, 1))
            ).get();

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        } finally {
            ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
            updateSettingsRequest.persistentSettings(Settings.builder().put("search.allow_expensive_queries", (String) null));
            assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
        }

        SearchResponse searchResponse = client().prepareSearch("vector").setQuery(
            geoShapeQuery("shape", new PointBuilder(90, 1))
        ).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));

        searchResponse = client().prepareSearch("vector").setQuery(
            geoShapeQuery("shape", new PointBuilder(-179.75, 1))
        ).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("vector").setQuery(
            geoShapeQuery("shape", new PointBuilder(-180, 1))
        ).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));

        searchResponse = client().prepareSearch("vector").setQuery(
            geoShapeQuery("shape", new PointBuilder(180, 1))
        ).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    private String findNodeName(String index) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexShardRoutingTable shard = state.getRoutingTable().index(index).shard(0);
        String nodeId = shard.assignedShards().get(0).currentNodeId();
        return state.getNodes().get(nodeId).getName();
    }
}
