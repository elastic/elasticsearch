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

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.apache.lucene.geo.GeoTestUtil;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.SpatialStrategy;
import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.EnvelopeBuilder;
import org.elasticsearch.common.geo.builders.GeometryCollectionBuilder;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiPointBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.mapper.LegacyGeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.spatial4j.shape.Rectangle;

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
import static org.hamcrest.Matchers.nullValue;

public class GeoShapeQueryTests extends ESSingleNodeTestCase {
    private static final String[] PREFIX_TREES = new String[] {
        LegacyGeoShapeFieldMapper.DeprecatedParameters.PrefixTrees.GEOHASH,
        LegacyGeoShapeFieldMapper.DeprecatedParameters.PrefixTrees.QUADTREE
    };

    private XContentBuilder createMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape");
        if (randomBoolean()) {
            xcb = xcb.field("tree", randomFrom(PREFIX_TREES))
            .field("strategy", randomFrom(SpatialStrategy.RECURSIVE, SpatialStrategy.TERM));
        }
        xcb = xcb.endObject().endObject().endObject().endObject();

        return xcb;
    }

    public void testNullShape() throws Exception {
        String mapping = Strings.toString(createMapping());
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping, XContentType.JSON).get();
        ensureGreen();

        client().prepareIndex("test", "type1", "aNullshape").setSource("{\"location\": null}", XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();
        GetResponse result = client().prepareGet("test", "type1", "aNullshape").get();
        assertThat(result.getField("location"), nullValue());
    }

    public void testIndexPointsFilterRectangle() throws Exception {
        String mapping = Strings.toString(createMapping());
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping, XContentType.JSON).get();
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

        EnvelopeBuilder shape = new EnvelopeBuilder(new Coordinate(-45, 45), new Coordinate(45, -45));

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(geoIntersectionQuery("location", shape))
                .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(geoShapeQuery("location", shape))
                .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testEdgeCases() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .endObject().endObject().endObject().endObject();
        String mapping = Strings.toString(xcb);
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping, XContentType.JSON).get();
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

        EnvelopeBuilder query = new EnvelopeBuilder(new Coordinate(-122.88, 48.62), new Coordinate(-122.82, 48.54));

        // This search would fail if both geoshape indexing and geoshape filtering
        // used the bottom-level optimization in SpatialPrefixTree#recursiveGetNodes.
        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(geoIntersectionQuery("location", query))
                .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("blakely"));
    }

    public void testIndexedShapeReference() throws Exception {
        String mapping = Strings.toString(createMapping());
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping, XContentType.JSON).get();
        createIndex("shapes");
        ensureGreen();

        EnvelopeBuilder shape = new EnvelopeBuilder(new Coordinate(-45, 45), new Coordinate(45, -45));

        client().prepareIndex("shapes", "shape_type", "Big_Rectangle").setSource(jsonBuilder().startObject()
                .field("shape", shape).endObject()).setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "Document 1")
                .startObject("location")
                .field("type", "point")
                .startArray("coordinates").value(-30).value(-30).endArray()
                .endObject()
                .endObject()).setRefreshPolicy(IMMEDIATE).get();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(geoIntersectionQuery("location", "Big_Rectangle"))
                .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(geoShapeQuery("location", "Big_Rectangle"))
                .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

     public void testIndexedShapeReferenceWithTypes() throws Exception {
        String mapping = Strings.toString(createMapping());
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping, XContentType.JSON).get();
        createIndex("shapes");
        ensureGreen();

        EnvelopeBuilder shape = new EnvelopeBuilder(new Coordinate(-45, 45), new Coordinate(45, -45));

        client().prepareIndex("shapes", "shape_type", "Big_Rectangle").setSource(jsonBuilder().startObject()
                .field("shape", shape).endObject()).setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
                .field("name", "Document 1")
                .startObject("location")
                .field("type", "point")
                .startArray("coordinates").value(-30).value(-30).endArray()
                .endObject()
                .endObject()).setRefreshPolicy(IMMEDIATE).get();

        SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(geoIntersectionQuery("location", "Big_Rectangle"))
                .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
                .setQuery(geoShapeQuery("location", "Big_Rectangle"))
                .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testIndexedShapeReferenceSourceDisabled() throws Exception {
        XContentBuilder mapping = createMapping();
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping).get();
        createIndex("shapes", Settings.EMPTY, "shape_type", "_source", "enabled=false");
        ensureGreen();

        EnvelopeBuilder shape = new EnvelopeBuilder(new Coordinate(-45, 45), new Coordinate(45, -45));

        client().prepareIndex("shapes", "shape_type", "Big_Rectangle").setSource(jsonBuilder().startObject()
            .field("shape", shape).endObject()).setRefreshPolicy(IMMEDIATE).get();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().prepareSearch("test")
            .setQuery(geoIntersectionQuery("location", "Big_Rectangle")).get());
        assertThat(e.getMessage(), containsString("source disabled"));
    }

    public void testReusableBuilder() throws IOException {
        PolygonBuilder polygon = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(170, -10).coordinate(190, -10).coordinate(190, 10).coordinate(170, 10).close())
                .hole(new LineStringBuilder(new CoordinatesBuilder().coordinate(175, -5).coordinate(185, -5).coordinate(185, 5)
                        .coordinate(175, 5).close()));
        assertUnmodified(polygon);

        LineStringBuilder linestring = new LineStringBuilder(new CoordinatesBuilder()
                .coordinate(170, -10).coordinate(190, -10).coordinate(190, 10).coordinate(170, 10).close());
        assertUnmodified(linestring);
    }

    private void assertUnmodified(ShapeBuilder builder) throws IOException {
        String before = Strings.toString(jsonBuilder().startObject().field("area", builder).endObject());
        builder.buildS4J();
        String after = Strings.toString(jsonBuilder().startObject().field("area", builder).endObject());
        assertThat(before, equalTo(after));
    }

    public void testShapeFetchingPath() throws Exception {
        createIndex("shapes");
        client().admin().indices().prepareCreate("test").addMapping("type", "location", "type=geo_shape").get();

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

        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery("location", "1").relation(ShapeRelation.INTERSECTS)
                .indexedShapeIndex("shapes")
                .indexedShapePath("location");
        SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("location", "1").relation(ShapeRelation.INTERSECTS)
                .indexedShapeIndex("shapes")
                .indexedShapePath("1.location");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("location", "1").relation(ShapeRelation.INTERSECTS)
                .indexedShapeIndex("shapes")
                .indexedShapePath("1.2.location");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("location", "1").relation(ShapeRelation.INTERSECTS)
                .indexedShapeIndex("shapes")
                .indexedShapePath("1.2.3.location");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);

        // now test the query variant
        GeoShapeQueryBuilder query = QueryBuilders.geoShapeQuery("location", "1")
                .indexedShapeIndex("shapes")
                .indexedShapePath("location");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery("location", "1")
                .indexedShapeIndex("shapes")
                .indexedShapePath("1.location");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery("location", "1")
                .indexedShapeIndex("shapes")
                .indexedShapePath("1.2.location");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery("location", "1")
                .indexedShapeIndex("shapes")
                .indexedShapePath("1.2.3.location");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testQueryRandomGeoCollection() throws Exception {
        // Create a random geometry collection.
        GeometryCollectionBuilder gcb = RandomShapeGenerator.createGeometryCollection(random());
        org.apache.lucene.geo.Polygon randomPoly = GeoTestUtil.nextPolygon();
        CoordinatesBuilder cb = new CoordinatesBuilder();
        for (int i = 0; i < randomPoly.numPoints(); ++i) {
            cb.coordinate(randomPoly.getPolyLon(i), randomPoly.getPolyLat(i));
        }
        gcb.shape(new PolygonBuilder(cb));

        logger.info("Created Random GeometryCollection containing {} shapes", gcb.numShapes());

        if (randomBoolean()) {
            client().admin().indices().prepareCreate("test")
                .addMapping("type", "location", "type=geo_shape").get();
        } else {
            client().admin().indices().prepareCreate("test")
                .addMapping("type", "location", "type=geo_shape,tree=quadtree").get();
        }

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("location"), null).endObject();
        client().prepareIndex("test", "type", "1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        ShapeBuilder filterShape = (gcb.getShapeAt(gcb.numShapes() - 1));

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery("location", filterShape);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assumeTrue("Skipping the check for the polygon with a degenerated dimension until "
                +" https://issues.apache.org/jira/browse/LUCENE-8634 is fixed",
            randomPoly.maxLat - randomPoly.minLat > 8.4e-8 &&  randomPoly.maxLon - randomPoly.minLon > 8.4e-8);
        assertHitCount(result, 1);
    }

    public void testRandomGeoCollectionQuery() throws Exception {
        boolean usePrefixTrees = randomBoolean();
        // Create a random geometry collection to index.
        GeometryCollectionBuilder gcb;
        if (usePrefixTrees) {
            gcb = RandomShapeGenerator.createGeometryCollection(random());
        } else {
            // vector strategy does not yet support multipoint queries
            gcb = new GeometryCollectionBuilder();
            int numShapes = RandomNumbers.randomIntBetween(random(), 1, 4);
            for (int i = 0; i < numShapes; ++i) {
                ShapeBuilder shape;
                do {
                    shape = RandomShapeGenerator.createShape(random());
                } while (shape instanceof MultiPointBuilder);
                gcb.shape(shape);
            }
        }
        org.apache.lucene.geo.Polygon randomPoly = GeoTestUtil.nextPolygon();

        assumeTrue("Skipping the check for the polygon with a degenerated dimension",
            randomPoly.maxLat - randomPoly.minLat > 8.4e-8 &&  randomPoly.maxLon - randomPoly.minLon > 8.4e-8);

        CoordinatesBuilder cb = new CoordinatesBuilder();
        for (int i = 0; i < randomPoly.numPoints(); ++i) {
            cb.coordinate(randomPoly.getPolyLon(i), randomPoly.getPolyLat(i));
        }
        gcb.shape(new PolygonBuilder(cb));

        logger.info("Created Random GeometryCollection containing {} shapes using {} tree", gcb.numShapes(),
            usePrefixTrees ? "default" : "quadtree");

        if (usePrefixTrees == false) {
            client().admin().indices().prepareCreate("test").addMapping("type", "location", "type=geo_shape")
                .execute().actionGet();
        } else {
            client().admin().indices().prepareCreate("test").addMapping("type", "location", "type=geo_shape,tree=quadtree")
                .execute().actionGet();
        }

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("location"), null).endObject();
        client().prepareIndex("test", "type", "1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        // Create a random geometry collection to query
        GeometryCollectionBuilder queryCollection = RandomShapeGenerator.createGeometryCollection(random());
        queryCollection.shape(new PolygonBuilder(cb));

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery("location", queryCollection);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assertTrue("query: " + geoShapeQueryBuilder.toString() + " doc: " + Strings.toString(docSource),
            result.getHits().getTotalHits().value > 0);
    }

    /** tests querying a random geometry collection with a point */
    public void testPointQuery() throws Exception {
        // Create a random geometry collection to index.
        GeometryCollectionBuilder gcb = RandomShapeGenerator.createGeometryCollection(random());
        double[] pt = new double[] {GeoTestUtil.nextLongitude(), GeoTestUtil.nextLatitude()};
        PointBuilder pb = new PointBuilder(pt[0], pt[1]);
        gcb.shape(pb);
        if (randomBoolean()) {
            client().admin().indices().prepareCreate("test").addMapping("type", "location", "type=geo_shape")
                .execute().actionGet();
        } else {
            client().admin().indices().prepareCreate("test").addMapping("type", "location", "type=geo_shape,tree=quadtree")
                .execute().actionGet();
        }
        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("location"), null).endObject();
        client().prepareIndex("test", "type", "1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery("location", pb);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testContainsShapeQuery() throws Exception {
        // Create a random geometry collection.
        Rectangle mbr = xRandomRectangle(random(), xRandomPoint(random()), true);
        GeometryCollectionBuilder gcb = createGeometryCollectionWithin(random(), mbr);

        client().admin().indices().prepareCreate("test").addMapping("type", "location", "type=geo_shape,tree=quadtree" )
                .get();

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("location"), null).endObject();
        client().prepareIndex("test", "type", "1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        // index the mbr of the collection
        EnvelopeBuilder env = new EnvelopeBuilder(new Coordinate(mbr.getMinX(), mbr.getMaxY()),
                new Coordinate(mbr.getMaxX(), mbr.getMinY()));
        docSource = env.toXContent(jsonBuilder().startObject().field("location"), null).endObject();
        client().prepareIndex("test", "type", "2").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        ShapeBuilder filterShape = (gcb.getShapeAt(randomIntBetween(0, gcb.numShapes() - 1)));
        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery("location", filterShape)
                .relation(ShapeRelation.CONTAINS);
        SearchResponse response = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(response);

        assertThat(response.getHits().getTotalHits().value, greaterThan(0L));
    }

    public void testExistsQuery() throws Exception {
        // Create a random geometry collection.
        GeometryCollectionBuilder gcb = RandomShapeGenerator.createGeometryCollection(random());
        logger.info("Created Random GeometryCollection containing {} shapes", gcb.numShapes());

        if (randomBoolean()) {
            client().admin().indices().prepareCreate("test").addMapping("type", "location", "type=geo_shape")
                .execute().actionGet();
        } else {
            client().admin().indices().prepareCreate("test").addMapping("type", "location", "type=geo_shape,tree=quadtree")
                .execute().actionGet();
        }

        XContentBuilder docSource = gcb.toXContent(jsonBuilder().startObject().field("location"), null).endObject();
        client().prepareIndex("test", "type", "1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        ExistsQueryBuilder eqb = QueryBuilders.existsQuery("location");
        SearchResponse result = client().prepareSearch("test").setQuery(eqb).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testShapeFilterWithDefinedGeoCollection() throws Exception {
        createIndex("shapes");
        client().admin().indices().prepareCreate("test").addMapping("type", "location", "type=geo_shape,tree=quadtree")
                .get();

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
                new GeometryCollectionBuilder()
                        .polygon(
                                new PolygonBuilder(new CoordinatesBuilder().coordinate(99.0, -1.0).coordinate(99.0, 3.0)
                                        .coordinate(103.0, 3.0).coordinate(103.0, -1.0)
                                        .coordinate(99.0, -1.0)))).relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery(
                "location",
                new GeometryCollectionBuilder().polygon(
                        new PolygonBuilder(new CoordinatesBuilder().coordinate(199.0, -11.0).coordinate(199.0, 13.0)
                                .coordinate(193.0, 13.0).coordinate(193.0, -11.0)
                                .coordinate(199.0, -11.0)))).relation(ShapeRelation.INTERSECTS);
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 0);
        filter = QueryBuilders.geoShapeQuery("location", new GeometryCollectionBuilder()
                .polygon(new PolygonBuilder(new CoordinatesBuilder().coordinate(99.0, -1.0).coordinate(99.0, 3.0).coordinate(103.0, 3.0)
                        .coordinate(103.0, -1.0).coordinate(99.0, -1.0)))
                        .polygon(
                                new PolygonBuilder(new CoordinatesBuilder().coordinate(199.0, -11.0).coordinate(199.0, 13.0)
                                        .coordinate(193.0, 13.0).coordinate(193.0, -11.0)
                                        .coordinate(199.0, -11.0)))).relation(ShapeRelation.INTERSECTS);
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        // no shape
        filter = QueryBuilders.geoShapeQuery("location", new GeometryCollectionBuilder());
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
                .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 0);
    }

    public void testPointsOnly() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
                .startObject("properties").startObject("location")
                .field("type", "geo_shape")
                .field("tree", randomBoolean() ? "quadtree" : "geohash")
                .field("tree_levels", "6")
                .field("distance_error_pct", "0.01")
                .field("points_only", true)
                .endObject().endObject()
                .endObject().endObject());

        client().admin().indices().prepareCreate("geo_points_only").addMapping("type1", mapping, XContentType.JSON).get();
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
        SearchResponse response = client().prepareSearch("geo_points_only")
                .setQuery(geoIntersectionQuery("location", shape))
                .get();

        assertEquals(1, response.getHits().getTotalHits().value);
    }

    public void testPointsOnlyExplicit() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("location")
            .field("type", "geo_shape")
            .field("tree", randomBoolean() ? "quadtree" : "geohash")
            .field("tree_levels", "6")
            .field("distance_error_pct", "0.01")
            .field("points_only", true)
            .endObject().endObject()
            .endObject().endObject());

        client().admin().indices().prepareCreate("geo_points_only").addMapping("type1", mapping, XContentType.JSON).get();
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
        SearchResponse response = client().prepareSearch("geo_points_only")
            .setQuery(matchAllQuery())
            .get();

        assertEquals(2, response.getHits().getTotalHits().value);
    }

    public void testFieldAlias() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("type")
                .startObject("properties")
                    .startObject("location")
                        .field("type", "geo_shape")
                        .field("tree", randomBoolean() ? "quadtree" : "geohash")
                    .endObject()
                    .startObject("alias")
                        .field("type", "alias")
                        .field("path", "location")
                    .endObject()
                .endObject()
            .endObject()
        .endObject();

        createIndex("test", Settings.EMPTY, "type", mapping);

        ShapeBuilder shape = RandomShapeGenerator.createShape(random(), RandomShapeGenerator.ShapeType.MULTIPOINT);
        client().prepareIndex("test", "type", "1")
            .setSource(jsonBuilder().startObject().field("location", shape).endObject())
            .setRefreshPolicy(IMMEDIATE).get();

        SearchResponse response = client().prepareSearch("test")
            .setQuery(geoShapeQuery("alias", shape))
            .get();
        assertEquals(1, response.getHits().getTotalHits().value);
    }

    // Test for issue #34418
    public void testEnvelopeSpanningDateline() throws IOException {
        XContentBuilder mapping = XContentFactory.jsonBuilder().startObject()
            .startObject("doc")
                .startObject("properties")
                    .startObject("geo").field("type", "geo_shape").endObject()
                .endObject()
            .endObject()
        .endObject();

        createIndex("test", Settings.builder().put("index.number_of_shards", 1).build(), "doc", mapping);

        String doc1 = "{\"geo\": {\r\n" + "\"coordinates\": [\r\n" + "-33.918711,\r\n" + "18.847685\r\n" + "],\r\n" +
                "\"type\": \"Point\"\r\n" + "}}";
        client().index(new IndexRequest("test", "doc", "1").source(doc1, XContentType.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc2 = "{\"geo\": {\r\n" + "\"coordinates\": [\r\n" + "-49.0,\r\n" + "18.847685\r\n" + "],\r\n" +
            "\"type\": \"Point\"\r\n" + "}}";
        client().index(new IndexRequest("test", "doc", "2").source(doc2, XContentType.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc3 = "{\"geo\": {\r\n" + "\"coordinates\": [\r\n" + "49.0,\r\n" + "18.847685\r\n" + "],\r\n" +
            "\"type\": \"Point\"\r\n" + "}}";
        client().index(new IndexRequest("test", "doc", "3").source(doc3, XContentType.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        @SuppressWarnings("unchecked") CheckedSupplier<GeoShapeQueryBuilder, IOException> querySupplier = randomFrom(
            () -> QueryBuilders.geoShapeQuery(
                "geo",
                new EnvelopeBuilder(new Coordinate(-21, 44), new Coordinate(-39, 9))
            ).relation(ShapeRelation.WITHIN),
            () -> {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                    .startObject("geo")
                    .startObject("shape")
                    .field("type", "envelope")
                    .startArray("coordinates")
                    .startArray().value(-21).value(44).endArray()
                    .startArray().value(-39).value(9).endArray()
                    .endArray()
                    .endObject()
                    .field("relation", "within")
                    .endObject()
                    .endObject();
                try (XContentParser parser = createParser(builder)){
                    parser.nextToken();
                    return GeoShapeQueryBuilder.fromXContent(parser);
                }
            },
            () -> {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                    .startObject("geo")
                    .field("shape", "BBOX (-21, -39, 44, 9)")
                    .field("relation", "within")
                    .endObject()
                    .endObject();
                try (XContentParser parser = createParser(builder)){
                    parser.nextToken();
                    return GeoShapeQueryBuilder.fromXContent(parser);
                }
            }
        );

        SearchResponse response = client().prepareSearch("test")
            .setQuery(querySupplier.get())
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);
        assertNotEquals("1", response.getHits().getAt(0).getId());
        assertNotEquals("1", response.getHits().getAt(1).getId());
    }
}
