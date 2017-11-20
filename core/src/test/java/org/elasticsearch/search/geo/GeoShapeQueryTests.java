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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.locationtech.spatial4j.shape.Rectangle;
import com.vividsolutions.jts.geom.Coordinate;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.GeometryCollectionBuilder;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilders;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;

import java.io.IOException;
import java.util.Locale;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.geoIntersectionQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.geo.RandomShapeGenerator.createGeometryCollectionWithin;
import static org.elasticsearch.test.geo.RandomShapeGenerator.xRandomPoint;
import static org.elasticsearch.test.geo.RandomShapeGenerator.xRandomRectangle;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class GeoShapeQueryTests extends ESSingleNodeTestCase {
    public void testNullShape() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .endObject().endObject()
                .endObject().endObject().string();
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping, XContentType.JSON).execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "type1", "aNullshape").setSource("{\"location\": null}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();
        GetResponse result = client().prepareGet("test", "type1", "aNullshape").execute().actionGet();
        assertThat(result.getField("location"), nullValue());
    }

    public void testIndexPointsFilterRectangle() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .endObject().endObject()
                .endObject().endObject().string();
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping, XContentType.JSON).execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "Document 1")
                .startObject("location")
                .field("type", "point")
                .startArray("coordinates").value(-30).value(-30).endArray()
                .endObject()
                .endObject()).setRefreshPolicy(IMMEDIATE).get();

        client().prepareIndex("test", "type1", "2").setSource(jsonBuilder().startObject()
                .field("name", "Document 2")
                .startObject("location")
                .field("type", "point")
                .startArray("coordinates").value(-45).value(-50).endArray()
                .endObject()
                .endObject()).setRefreshPolicy(IMMEDIATE).get();

        ShapeBuilder shape = ShapeBuilders.newEnvelope(new Coordinate(-45, 45), new Coordinate(45, -45));

        SearchResponse searchResponse = client().prepareSearch("test").setTypes("type1")
                .setQuery(geoIntersectionQuery("location", shape))
                .execute().actionGet();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test").setTypes("type1")
                .setQuery(geoShapeQuery("location", shape))
                .execute().actionGet();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testEdgeCases() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .endObject().endObject()
                .endObject().endObject().string();
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping, XContentType.JSON).execute().actionGet();
        ensureGreen();

        client().prepareIndex("test", "type1", "blakely").setSource(jsonBuilder().startObject()
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
                .endObject()).setRefreshPolicy(IMMEDIATE).get();

        ShapeBuilder query = ShapeBuilders.newEnvelope(new Coordinate(-122.88, 48.62), new Coordinate(-122.82, 48.54));

        // This search would fail if both geoshape indexing and geoshape filtering
        // used the bottom-level optimization in SpatialPrefixTree#recursiveGetNodes.
        SearchResponse searchResponse = client().prepareSearch("test").setTypes("type1")
                .setQuery(geoIntersectionQuery("location", query))
                .execute().actionGet();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("blakely"));
    }

    public void testIndexedShapeReference() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", "quadtree")
                .endObject().endObject()
                .endObject().endObject().string();
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping, XContentType.JSON).execute().actionGet();
        createIndex("shapes");
        ensureGreen();

        ShapeBuilder shape = ShapeBuilders.newEnvelope(new Coordinate(-45, 45), new Coordinate(45, -45));

        client().prepareIndex("shapes", "shape_type", "Big_Rectangle").setSource(jsonBuilder().startObject()
                .field("shape", shape).endObject()).setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "Document 1")
                .startObject("location")
                .field("type", "point")
                .startArray("coordinates").value(-30).value(-30).endArray()
                .endObject()
                .endObject()).setRefreshPolicy(IMMEDIATE).get();

        SearchResponse searchResponse = client().prepareSearch("test").setTypes("type1")
                .setQuery(geoIntersectionQuery("location", "Big_Rectangle", "shape_type"))
                .execute().actionGet();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(geoShapeQuery("location", "Big_Rectangle", "shape_type"))
                .execute().actionGet();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits(), equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testIndexedShapeReferenceSourceDisabled() throws Exception {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", "quadtree")
                    .endObject()
                .endObject()
            .endObject();
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping).get();
        createIndex("shapes", Settings.EMPTY, "shape_type", "_source", "enabled=false");
        ensureGreen();

        ShapeBuilder shape = ShapeBuilders.newEnvelope(new Coordinate(-45, 45), new Coordinate(45, -45));

        client().prepareIndex("shapes", "shape_type", "Big_Rectangle").setSource(jsonBuilder().startObject()
            .field("shape", shape).endObject()).setRefreshPolicy(IMMEDIATE).get();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().prepareSearch("test").setTypes("type1")
            .setQuery(geoIntersectionQuery("location", "Big_Rectangle", "shape_type")).get());
        assertThat(e.getMessage(), containsString("source disabled"));
    }

    public void testReusableBuilder() throws IOException {
        ShapeBuilder polygon = ShapeBuilders.newPolygon(new CoordinatesBuilder()
                .coordinate(170, -10).coordinate(190, -10).coordinate(190, 10).coordinate(170, 10).close())
                .hole(new LineStringBuilder(new CoordinatesBuilder().coordinate(175, -5).coordinate(185, -5).coordinate(185, 5).coordinate(175, 5).close()));
        assertUnmodified(polygon);

        ShapeBuilder linestring = ShapeBuilders.newLineString(new CoordinatesBuilder()
                .coordinate(170, -10).coordinate(190, -10).coordinate(190, 10).coordinate(170, 10).close());
        assertUnmodified(linestring);
    }

    private void assertUnmodified(ShapeBuilder builder) throws IOException {
        String before = jsonBuilder().startObject().field("area", builder).endObject().string();
        builder.build();
        String after = jsonBuilder().startObject().field("area", builder).endObject().string();
        assertThat(before, equalTo(after));
    }

    public void testShapeFetchingPath() throws Exception {
        createIndex("shapes");
        client().admin().indices().prepareCreate("test").addMapping("type", "location", "type=geo_shape").execute().actionGet();

        String location = "\"location\" : {\"type\":\"polygon\", \"coordinates\":[[[-10,-10],[10,-10],[10,10],[-10,10],[-10,-10]]]}";

        client().prepareIndex("shapes", "type", "1")
                .setSource(
                        String.format(
                                Locale.ROOT, "{ %s, \"1\" : { %s, \"2\" : { %s, \"3\" : { %s } }} }", location, location, location, location
                        ), XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();
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
                        .endObject().endObject()).setRefreshPolicy(IMMEDIATE).get();

        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery("location", "1", "type").relation(ShapeRelation.INTERSECTS)
                .indexedShapeIndex("shapes")
                .indexedShapePath("location");
        SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("location", "1", "type").relation(ShapeRelation.INTERSECTS)
                .indexedShapeIndex("shapes")
                .indexedShapePath("1.location");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("location", "1", "type").relation(ShapeRelation.INTERSECTS)
                .indexedShapeIndex("shapes")
                .indexedShapePath("1.2.location");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("location", "1", "type").relation(ShapeRelation.INTERSECTS)
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

    public void testShapeFilterWithRandomGeoCollection() throws Exception {
        // Create a random geometry collection.
        GeometryCollectionBuilder gcb = RandomShapeGenerator.createGeometryCollection(random());

        logger.info("Created Random GeometryCollection containing {} shapes", gcb.numShapes());

        client().admin().indices().prepareCreate("test").addMapping("type", "location", "type=geo_shape,tree=quadtree")
                .execute().actionGet();

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("location"), null).endObject();
        client().prepareIndex("test", "type", "1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        ShapeBuilder filterShape = (gcb.getShapeAt(randomIntBetween(0, gcb.numShapes() - 1)));

        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery("location", filterShape);
        filter.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setTypes("type").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testContainsShapeQuery() throws Exception {
        // Create a random geometry collection.
        Rectangle mbr = xRandomRectangle(random(), xRandomPoint(random()), true);
        GeometryCollectionBuilder gcb = createGeometryCollectionWithin(random(), mbr);

        client().admin().indices().prepareCreate("test").addMapping("type", "location", "type=geo_shape,tree=quadtree" )
                .execute().actionGet();

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("location"), null).endObject();
        client().prepareIndex("test", "type", "1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        // index the mbr of the collection
        EnvelopeBuilder env = new EnvelopeBuilder(new Coordinate(mbr.getMinX(), mbr.getMaxY()), new Coordinate(mbr.getMaxX(), mbr.getMinY()));
        docSource = env.toXContent(jsonBuilder().startObject().field("location"), null).endObject();
        client().prepareIndex("test", "type", "2").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        ShapeBuilder filterShape = (gcb.getShapeAt(randomIntBetween(0, gcb.numShapes() - 1)));
        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery("location", filterShape)
                .relation(ShapeRelation.CONTAINS);
        SearchResponse response = client().prepareSearch("test").setTypes("type").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(response);

        assertThat(response.getHits().getTotalHits(), greaterThan(0L));
    }

    public void testShapeFilterWithDefinedGeoCollection() throws Exception {
        createIndex("shapes");
        client().admin().indices().prepareCreate("test").addMapping("type", "location", "type=geo_shape,tree=quadtree")
                .execute().actionGet();

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
        client().prepareIndex("test", "type", "1")
                .setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery(
                "location",
                ShapeBuilders.newGeometryCollection()
                        .polygon(
                                ShapeBuilders.newPolygon(new CoordinatesBuilder().coordinate(99.0, -1.0).coordinate(99.0, 3.0).coordinate(103.0, 3.0).coordinate(103.0, -1.0)
                                        .coordinate(99.0, -1.0)))).relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setTypes("type").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery(
                "location",
                ShapeBuilders.newGeometryCollection().polygon(
                        ShapeBuilders.newPolygon(new CoordinatesBuilder().coordinate(199.0, -11.0).coordinate(199.0, 13.0).coordinate(193.0, 13.0).coordinate(193.0, -11.0)
                                .coordinate(199.0, -11.0)))).relation(ShapeRelation.INTERSECTS);
        result = client().prepareSearch("test").setTypes("type").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 0);
        filter = QueryBuilders.geoShapeQuery("location", ShapeBuilders.newGeometryCollection()
                .polygon(ShapeBuilders.newPolygon(new CoordinatesBuilder().coordinate(99.0, -1.0).coordinate(99.0, 3.0).coordinate(103.0, 3.0).coordinate(103.0, -1.0).coordinate(99.0, -1.0)))
                        .polygon(
                                ShapeBuilders.newPolygon(new CoordinatesBuilder().coordinate(199.0, -11.0).coordinate(199.0, 13.0).coordinate(193.0, 13.0).coordinate(193.0, -11.0)
                                        .coordinate(199.0, -11.0)))).relation(ShapeRelation.INTERSECTS);
        result = client().prepareSearch("test").setTypes("type").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        // no shape
        filter = QueryBuilders.geoShapeQuery("location", ShapeBuilders.newGeometryCollection());
        result = client().prepareSearch("test").setTypes("type").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 0);
    }

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

        client().admin().indices().prepareCreate("geo_points_only").addMapping("type1", mapping, XContentType.JSON).execute().actionGet();
        ensureGreen();

        ShapeBuilder shape = RandomShapeGenerator.createShape(random());
        try {
            client().prepareIndex("geo_points_only", "type1", "1")
                    .setSource(jsonBuilder().startObject().field("location", shape).endObject())
                    .setRefreshPolicy(IMMEDIATE).get();
        } catch (MapperParsingException e) {
            // RandomShapeGenerator created something other than a POINT type, verify the correct exception is thrown
            assertThat(e.getCause().getMessage(), containsString("is configured for points only"));
            return;
        }

        // test that point was inserted
        SearchResponse response = client().prepareSearch("geo_points_only").setTypes("type1")
                .setQuery(matchAllQuery())
                .execute().actionGet();

        assertEquals(1, response.getHits().getTotalHits());
    }

    public void testPointsOnlyExplicit() throws Exception {
        String mapping = XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("tree", randomBoolean() ? "quadtree" : "geohash")
            .field("tree_levels", "6")
            .field("distance_error_pct", "0.01")
            .field("points_only", true)
            .endObject().endObject()
            .endObject().endObject().string();

        client().admin().indices().prepareCreate("geo_points_only").addMapping("type1", mapping, XContentType.JSON).execute().actionGet();
        ensureGreen();

        // MULTIPOINT
        ShapeBuilder shape = RandomShapeGenerator.createShape(random(), RandomShapeGenerator.ShapeType.MULTIPOINT);
        client().prepareIndex("geo_points_only", "type1", "1")
            .setSource(jsonBuilder().startObject().field("location", shape).endObject())
            .setRefreshPolicy(IMMEDIATE).get();

        // POINT
        shape = RandomShapeGenerator.createShape(random(), RandomShapeGenerator.ShapeType.POINT);
        client().prepareIndex("geo_points_only", "type1", "2")
            .setSource(jsonBuilder().startObject().field("location", shape).endObject())
            .setRefreshPolicy(IMMEDIATE).get();

        // test that point was inserted
        SearchResponse response = client().prepareSearch("geo_points_only").setTypes("type1")
            .setQuery(matchAllQuery())
            .execute().actionGet();

        assertEquals(2, response.getHits().getTotalHits());
    }
}
