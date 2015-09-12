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

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.GeometryCollectionBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.geo.GeoShapeFieldMapper;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.junit.Test;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.geoIntersectionQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.*;

public class GeoShapeIntegrationIT extends ESIntegTestCase {

    @Test
    public void testNullShape() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .endObject().endObject()
                .endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        indexRandom(false, client().prepareIndex("test", "type1", "aNullshape").setSource("{\"location\": null}"));
        GetResponse result = client().prepareGet("test", "type1", "aNullshape").execute().actionGet();
        assertThat(result.getField("location"), nullValue());
    }

    @Test
    public void testIndexPointsFilterRectangle() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .endObject().endObject()
                .endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        indexRandom(true,

                client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                        .field("name", "Document 1")
                        .startObject("location")
                        .field("type", "point")
                        .startArray("coordinates").value(-30).value(-30).endArray()
                        .endObject()
                        .endObject()),

                client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                        .field("name", "Document 2")
                        .startObject("location")
                        .field("type", "point")
                        .startArray("coordinates").value(-45).value(-50).endArray()
                        .endObject()
                        .endObject()));

        ShapeBuilder shape = ShapeBuilder.newEnvelope().topLeft(-45, 45).bottomRight(45, -45);

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(geoIntersectionQuery("location", shape))
                .execute().actionGet();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));

        searchResponse = client().prepareSearch()
                .setQuery(geoShapeQuery("location", shape))
                .execute().actionGet();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
    }

    @Test
    public void testEdgeCases() throws Exception {

        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .endObject().endObject()
                .endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureGreen();

        indexRandom(true, client().prepareIndex("test", "type1", "blakely").setSource(jsonBuilder().startObject()
                .field("name", "Blakely Island")
                .startObject("location")
                .field("type", "polygon")
                .startArray("coordinates").startArray()
                .startArray().value(-122.83).value(48.57).endArray()
                .startArray().value(-122.77).value(48.56).endArray()
                .startArray().value(-122.79).value(48.53).endArray()
                .startArray().value(-122.83).value(48.57).endArray() // close the polygon
                .endArray().endArray()
                .endObject()
                .endObject()));


        ShapeBuilder query = ShapeBuilder.newEnvelope().topLeft(-122.88, 48.62).bottomRight(-122.82, 48.54);

        // This search would fail if both geoshape indexing and geoshape filtering
        // used the bottom-level optimization in SpatialPrefixTree#recursiveGetNodes.
        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(geoIntersectionQuery("location", query))
                .execute().actionGet();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("blakely"));
    }

    @Test
    public void testIndexedShapeReference() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .endObject().endObject()
                .endObject().endObject().string();
        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        createIndex("shapes");
        ensureGreen();

        ShapeBuilder shape = ShapeBuilder.newEnvelope().topLeft(-45, 45).bottomRight(45, -45);

        indexRandom(true,
            client().prepareIndex("shapes", "shape_type", "Big_Rectangle").setSource(jsonBuilder().startObject()
                .field("shape", shape).endObject()),
            client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "Document 1")
                .startObject("location")
                .field("type", "point")
                .startArray("coordinates").value(-30).value(-30).endArray()
                .endObject()
                .endObject()));

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(geoIntersectionQuery("location", "Big_Rectangle", "shape_type"))
                .execute().actionGet();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(geoShapeQuery("location", "Big_Rectangle", "shape_type"))
                .execute().actionGet();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1l));
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).id(), equalTo("1"));
    }

    @Test
    public void testReusableBuilder() throws IOException {
        ShapeBuilder polygon = ShapeBuilder.newPolygon()
                .point(170, -10).point(190, -10).point(190, 10).point(170, 10)
                .hole().point(175, -5).point(185, -5).point(185, 5).point(175, 5).close()
                .close();
        assertUnmodified(polygon);

        ShapeBuilder linestring = ShapeBuilder.newLineString()
                .point(170, -10).point(190, -10).point(190, 10).point(170, 10);
        assertUnmodified(linestring);
    }

    private void assertUnmodified(ShapeBuilder builder) throws IOException {
        String before = jsonBuilder().startObject().field("area", builder).endObject().string();
        builder.build();
        String after = jsonBuilder().startObject().field("area", builder).endObject().string();
        assertThat(before, equalTo(after));
    }

    @Test
    public void testParsingMultipleShapes() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("type1")
                .startObject("properties")
                .startObject("location1")
                .field("type", "geo_shape")
                .endObject()
                .startObject("location2")
                .field("type", "geo_shape")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .string();

        assertAcked(prepareCreate("test").addMapping("type1", mapping));
        ensureYellow();

        String p1 = "\"location1\" : {\"type\":\"polygon\", \"coordinates\":[[[-10,-10],[10,-10],[10,10],[-10,10],[-10,-10]]]}";
        String p2 = "\"location2\" : {\"type\":\"polygon\", \"coordinates\":[[[-20,-20],[20,-20],[20,20],[-20,20],[-20,-20]]]}";
        String o1 = "{" + p1 + ", " + p2 + "}";

        indexRandom(true, client().prepareIndex("test", "type1", "1").setSource(o1));

        String filter = "{\"geo_shape\": {\"location2\": {\"indexed_shape\": {"
                + "\"id\": \"1\","
                + "\"type\": \"type1\","
                + "\"index\": \"test\","
                + "\"path\": \"location2\""
                + "}}}}";

        SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).execute().actionGet();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    @Test
    public void testShapeFetchingPath() throws Exception {
        createIndex("shapes");
        assertAcked(prepareCreate("test").addMapping("type", "location", "type=geo_shape"));

        String location = "\"location\" : {\"type\":\"polygon\", \"coordinates\":[[[-10,-10],[10,-10],[10,10],[-10,10],[-10,-10]]]}";
        indexRandom(true,
                client().prepareIndex("shapes", "type", "1")
                .setSource(
                        String.format(
                                Locale.ROOT, "{ %s, \"1\" : { %s, \"2\" : { %s, \"3\" : { %s } }} }", location, location, location, location
                        )
                ),
                client().prepareIndex("test", "type", "1")
                .setSource(jsonBuilder().startObject().startObject("location")
                        .field("type", "polygon")
                        .startArray("coordinates").startArray()
                        .startArray().value(-20).value(-20).endArray()
                        .startArray().value(20).value(-20).endArray()
                        .startArray().value(20).value(20).endArray()
                        .startArray().value(-20).value(20).endArray()
                        .startArray().value(-20).value(-20).endArray()
                        .endArray().endArray()
                        .endObject().endObject()));
        ensureSearchable("test", "shapes");

        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery("location", "1", "type", ShapeRelation.INTERSECTS)
                .indexedShapeIndex("shapes")
                .indexedShapePath("location");
        SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("location", "1", "type", ShapeRelation.INTERSECTS)
                .indexedShapeIndex("shapes")
                .indexedShapePath("1.location");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("location", "1", "type", ShapeRelation.INTERSECTS)
                .indexedShapeIndex("shapes")
                .indexedShapePath("1.2.location");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("location", "1", "type", ShapeRelation.INTERSECTS)
                .indexedShapeIndex("shapes")
                .indexedShapePath("1.2.3.location");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);

        // now test the query variant
        GeoShapeQueryBuilder query = QueryBuilders.geoShapeQuery("location", "1", "type")
                .indexedShapeIndex("shapes")
                .indexedShapePath("location");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery("location", "1", "type")
                .indexedShapeIndex("shapes")
                .indexedShapePath("1.location");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery("location", "1", "type")
                .indexedShapeIndex("shapes")
                .indexedShapePath("1.2.location");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery("location", "1", "type")
                .indexedShapeIndex("shapes")
                .indexedShapePath("1.2.3.location");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    @LuceneTestCase.AwaitsFix(bugUrl = "https://github.com/elasticsearch/elasticsearch/issues/9904")
    @Test
    public void testShapeFilterWithRandomGeoCollection() throws Exception {
        // Create a random geometry collection.
        GeometryCollectionBuilder gcb = RandomShapeGenerator.createGeometryCollection(getRandom());

        logger.info("Created Random GeometryCollection containing " + gcb.numShapes() + " shapes");

        createIndex("randshapes");
        assertAcked(prepareCreate("test").addMapping("type", "location", "type=geo_shape"));

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("location"), null).endObject();
        indexRandom(true, client().prepareIndex("test", "type", "1").setSource(docSource));

        ensureSearchable("test");

        ShapeBuilder filterShape = (gcb.getShapeAt(randomIntBetween(0, gcb.numShapes() - 1)));

        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery("location", filterShape, ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    @Test
    public void testShapeFilterWithDefinedGeoCollection() throws Exception {
        createIndex("shapes");
        assertAcked(prepareCreate("test").addMapping("type", "location", "type=geo_shape"));

        XContentBuilder docSource = jsonBuilder().startObject().startObject("location")
                .field("type", "geometrycollection")
                .startArray("geometries")
                .startObject()
                .field("type", "point")
                .startArray("coordinates")
                .value(100.0).value(0.0)
                .endArray()
                .endObject()
                .startObject()
                .field("type", "linestring")
                .startArray("coordinates")
                .startArray()
                .value(101.0).value(0.0)
                .endArray()
                .startArray()
                .value(102.0).value(1.0)
                .endArray()
                .endArray()
                .endObject()
                .endArray()
                .endObject().endObject();
        indexRandom(true,
                client().prepareIndex("test", "type", "1")
                .setSource(docSource));
        ensureSearchable("test");

        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery("location", ShapeBuilder.newGeometryCollection().polygon(ShapeBuilder.newPolygon().point(99.0, -1.0).point(99.0, 3.0).point(103.0, 3.0).point(103.0, -1.0).point(99.0, -1.0)), ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("location", ShapeBuilder.newGeometryCollection().polygon(ShapeBuilder.newPolygon().point(199.0, -11.0).point(199.0, 13.0).point(193.0, 13.0).point(193.0, -11.0).point(199.0, -11.0)), ShapeRelation.INTERSECTS);
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 0);
        filter = QueryBuilders.geoShapeQuery("location", ShapeBuilder.newGeometryCollection()
                .polygon(ShapeBuilder.newPolygon().point(99.0, -1.0).point(99.0, 3.0).point(103.0, 3.0).point(103.0, -1.0).point(99.0, -1.0))
                .polygon(ShapeBuilder.newPolygon().point(199.0, -11.0).point(199.0, 13.0).point(193.0, 13.0).point(193.0, -11.0).point(199.0, -11.0)), ShapeRelation.INTERSECTS);
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    /**
     * Test that orientation parameter correctly persists across cluster restart
     * @throws IOException
     */
    public void testOrientationPersistence() throws Exception {
        String idxName = "orientation";
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("shape")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("orientation", "left")
                .endObject().endObject()
                .endObject().endObject().string();

        // create index
        assertAcked(prepareCreate(idxName).addMapping("shape", mapping));

        mapping = XContentFactory.jsonBuilder().startObject().startObject("shape")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("orientation", "right")
                .endObject().endObject()
                .endObject().endObject().string();

        assertAcked(prepareCreate(idxName+"2").addMapping("shape", mapping));
        ensureGreen(idxName, idxName+"2");

        internalCluster().fullRestart();
        ensureGreen(idxName, idxName+"2");

        // left orientation test
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, findNodeName(idxName));
        IndexService indexService = indicesService.indexService(idxName);
        MappedFieldType fieldType = indexService.mapperService().smartNameFieldType("location");
        assertThat(fieldType, instanceOf(GeoShapeFieldMapper.GeoShapeFieldType.class));

        GeoShapeFieldMapper.GeoShapeFieldType gsfm = (GeoShapeFieldMapper.GeoShapeFieldType)fieldType;
        ShapeBuilder.Orientation orientation = gsfm.orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.LEFT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CW));

        // right orientation test
        indicesService = internalCluster().getInstance(IndicesService.class, findNodeName(idxName+"2"));
        indexService = indicesService.indexService(idxName+"2");
        fieldType = indexService.mapperService().smartNameFieldType("location");
        assertThat(fieldType, instanceOf(GeoShapeFieldMapper.GeoShapeFieldType.class));

        gsfm = (GeoShapeFieldMapper.GeoShapeFieldType)fieldType;
        orientation = gsfm.orientation();
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.COUNTER_CLOCKWISE));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.RIGHT));
        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CCW));
    }

    @Test
    public void testPointsOnly() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", randomBoolean() ? "quadtree" : "geohash")
                .field("tree_levels", "6")
                .field("distance_error_pct", "0.01")
                .field("points_only", true)
                .endObject().endObject()
                .endObject().endObject().string();

        assertAcked(prepareCreate("geo_points_only").addMapping("type1", mapping));
        ensureGreen();

        ShapeBuilder shape = RandomShapeGenerator.createShape(random());
        try {
            index("geo_points_only", "type1", "1", jsonBuilder().startObject().field("location", shape).endObject());
        } catch (MapperParsingException e) {
            // RandomShapeGenerator created something other than a POINT type, verify the correct exception is thrown
            assertThat(e.getCause().getMessage(), containsString("is configured for points only"));
            return;
        }

        refresh();
        // test that point was inserted
        SearchResponse response = client().prepareSearch()
                .setQuery(geoIntersectionQuery("location", shape))
                .execute().actionGet();

        assertEquals(1, response.getHits().getTotalHits());
    }

    private String findNodeName(String index) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexShardRoutingTable shard = state.getRoutingTable().index(index).shard(0);
        String nodeId = shard.assignedShards().get(0).currentNodeId();
        return state.getNodes().get(nodeId).name();
    }
}
