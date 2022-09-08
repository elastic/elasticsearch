/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.geo;

import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.query.AbstractGeometryQueryBuilder;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.existsQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Collection of tests that can be applied to both geographic and cartesian coordinate data.
 * For geographic data see child classes like GeoShapeQueryTests and GeoShapeWithDocValuesQueryTests.
 * For cartesian data see child class CartesianShapeWithDocValuesQueryTests.
 */
public abstract class BaseShapeQueryTestCase<T extends AbstractGeometryQueryBuilder<T>> extends BasePointShapeQueryTestCase<T> {

    @Override
    protected void createMapping(String indexName, String fieldName, Settings settings) throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(fieldName)
            .field("type", fieldTypeName())
            .endObject()
            .endObject()
            .endObject();
        client().admin().indices().prepareCreate(indexName).setMapping(xcb).setSettings(settings).get();
    }

    public void testFieldAlias() throws IOException {
        String mapping = Strings.toString(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject(defaultFieldName)
                .field("type", fieldTypeName())
                .endObject()
                .startObject("alias")
                .field("type", "alias")
                .field("path", defaultFieldName)
                .endObject()
                .endObject()
                .endObject()
        );

        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(false);
        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(GeoJson.toXContent(multiPoint, jsonBuilder().startObject().field(defaultFieldName), null).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        SearchResponse response = client().prepareSearch(defaultIndexName).setQuery(queryBuilder().shapeQuery("alias", multiPoint)).get();
        assertEquals(1, response.getHits().getTotalHits().value);
    }

    public void testShapeFetchingPath() throws Exception {
        createIndex("shapes");
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        String geo = """
            "geo" : {"type":"polygon", "coordinates":[[[-10,-10],[10,-10],[10,10],[-10,10],[-10,-10]]]}""";

        client().prepareIndex("shapes").setId("1").setSource("""
            { %s, "1" : { %s, "2" : { %s, "3" : { %s } }} }
            """.formatted(geo, geo, geo, geo), XContentType.JSON).setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .startObject(defaultFieldName)
                    .field("type", "polygon")
                    .startArray("coordinates")
                    .startArray()
                    .startArray()
                    .value(-20)
                    .value(-20)
                    .endArray()
                    .startArray()
                    .value(20)
                    .value(-20)
                    .endArray()
                    .startArray()
                    .value(20)
                    .value(20)
                    .endArray()
                    .startArray()
                    .value(-20)
                    .value(20)
                    .endArray()
                    .startArray()
                    .value(-20)
                    .value(-20)
                    .endArray()
                    .endArray()
                    .endArray()
                    .endObject()
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();

        QueryBuilder filter = queryBuilder().shapeQuery(defaultFieldName, "1")
            .relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath(defaultFieldName);
        SearchResponse result = client().prepareSearch(defaultIndexName).setQuery(matchAllQuery()).setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = queryBuilder().shapeQuery(defaultFieldName, "1")
            .relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.geo");
        result = client().prepareSearch(defaultIndexName).setQuery(matchAllQuery()).setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = queryBuilder().shapeQuery(defaultFieldName, "1")
            .relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.2.geo");
        result = client().prepareSearch(defaultIndexName).setQuery(matchAllQuery()).setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = queryBuilder().shapeQuery(defaultFieldName, "1")
            .relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.2.3.geo");
        result = client().prepareSearch(defaultIndexName).setQuery(matchAllQuery()).setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);

        // now test the query variant
        QueryBuilder query = queryBuilder().shapeQuery(defaultFieldName, "1")
            .indexedShapeIndex("shapes")
            .indexedShapePath(defaultFieldName);
        result = client().prepareSearch(defaultIndexName).setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = queryBuilder().shapeQuery(defaultFieldName, "1").indexedShapeIndex("shapes").indexedShapePath("1.geo");
        result = client().prepareSearch(defaultIndexName).setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = queryBuilder().shapeQuery(defaultFieldName, "1").indexedShapeIndex("shapes").indexedShapePath("1.2.geo");
        result = client().prepareSearch(defaultIndexName).setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = queryBuilder().shapeQuery(defaultFieldName, "1").indexedShapeIndex("shapes").indexedShapePath("1.2.3.geo");
        result = client().prepareSearch(defaultIndexName).setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testRandomGeoCollectionQuery() throws Exception {
        // Create a random geometry collection to index.
        Polygon polygon = nextPolygon2();
        GeometryCollection<Geometry> gcb = makeRandomGeometryCollectionWithoutCircle(polygon);

        logger.info("Created Random GeometryCollection containing {} shapes", gcb.size());

        createMapping(defaultIndexName, defaultFieldName, Settings.builder().put("index.number_of_shards", 1).build());
        ensureGreen();

        XContentBuilder docSource = GeoJson.toXContent(gcb, jsonBuilder().startObject().field(defaultFieldName), null).endObject();
        client().prepareIndex(defaultIndexName).setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        // Create a random geometry collection to query
        GeometryCollection<Geometry> randomQueryCollection = makeRandomGeometryCollection();

        List<Geometry> queryGeometries = new ArrayList<>();
        for (Geometry geometry : randomQueryCollection) {
            queryGeometries.add(geometry);
        }
        queryGeometries.add(polygon);
        GeometryCollection<Geometry> queryCollection = new GeometryCollection<>(queryGeometries);

        QueryBuilder intersects = queryBuilder().intersectionQuery(defaultFieldName, queryCollection);
        SearchResponse result = client().prepareSearch(defaultIndexName).setQuery(intersects).get();
        assertSearchResponse(result);
        assertTrue("query: " + intersects + " doc: " + Strings.toString(docSource), result.getHits().getTotalHits().value > 0);
    }

    public void testGeometryCollectionRelations() throws Exception {
        Settings settings = Settings.builder().put("index.number_of_shards", 1).build();
        createMapping(defaultIndexName, defaultFieldName, settings);
        ensureGreen();

        Rectangle envelope = new Rectangle(-10, 10, 10, -10);

        client().index(
            new IndexRequest(defaultIndexName).source(
                jsonBuilder().startObject().field(defaultFieldName, WellKnownText.toWKT(envelope)).endObject()
            ).setRefreshPolicy(IMMEDIATE)
        ).actionGet();

        {
            // A geometry collection that is fully within the indexed shape
            List<Geometry> geometries = new ArrayList<>();
            geometries.add(new Point(1, 2));
            geometries.add(new Point(-2, -1));
            GeometryCollection<Geometry> collection = new GeometryCollection<>(geometries);
            SearchResponse response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, collection).relation(ShapeRelation.CONTAINS))
                .get();
            assertEquals(1, response.getHits().getTotalHits().value);
            response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, collection).relation(ShapeRelation.INTERSECTS))
                .get();
            assertEquals(1, response.getHits().getTotalHits().value);
            response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, collection).relation(ShapeRelation.DISJOINT))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
        }
        {
            // A geometry collection that is partially within the indexed shape
            List<Geometry> geometries = new ArrayList<>();
            geometries.add(new Point(1, 2));
            geometries.add(new Point(20, 30));
            GeometryCollection<Geometry> collection = new GeometryCollection<>(geometries);
            SearchResponse response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, collection).relation(ShapeRelation.CONTAINS))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
            response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, collection).relation(ShapeRelation.INTERSECTS))
                .get();
            assertEquals(1, response.getHits().getTotalHits().value);
            response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, collection).relation(ShapeRelation.DISJOINT))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
        }
        {
            // A geometry collection that is disjoint with the indexed shape
            List<Geometry> geometries = new ArrayList<>();
            geometries.add(new Point(-20, -30));
            geometries.add(new Point(20, 30));
            GeometryCollection<Geometry> collection = new GeometryCollection<>(geometries);
            SearchResponse response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, collection).relation(ShapeRelation.CONTAINS))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
            response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, collection).relation(ShapeRelation.INTERSECTS))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
            response = client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, collection).relation(ShapeRelation.DISJOINT))
                .get();
            assertEquals(1, response.getHits().getTotalHits().value);
        }
    }

    public void testEdgeCases() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(defaultFieldName)
            .field("type", fieldTypeName())
            .endObject()
            .endObject()
            .endObject();
        String mapping = Strings.toString(xcb);
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName)
            .setId("blakely")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "Blakely Island")
                    .startObject(defaultFieldName)
                    .field("type", "polygon")
                    .startArray("coordinates")
                    .startArray()
                    .startArray()
                    .value(-122.83)
                    .value(48.57)
                    .endArray()
                    .startArray()
                    .value(-122.77)
                    .value(48.56)
                    .endArray()
                    .startArray()
                    .value(-122.79)
                    .value(48.53)
                    .endArray()
                    .startArray()
                    .value(-122.83)
                    .value(48.57)
                    .endArray() // close the polygon
                    .endArray()
                    .endArray()
                    .endObject()
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();

        Rectangle query = new Rectangle(-122.88, -122.82, 48.62, 48.54);

        // This search would fail if both geoshape indexing and geoshape filtering
        // used the bottom-level optimization in SpatialPrefixTree#recursiveGetNodes.
        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(queryBuilder().intersectionQuery(defaultFieldName, query))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("blakely"));
    }

    public void testIndexedShapeReferenceSourceDisabled() throws Exception {
        createMapping(defaultIndexName, defaultFieldName, Settings.builder().put("index.number_of_shards", 1).build());
        createIndex("shapes", Settings.EMPTY, "shape_type", "_source", "enabled=false");
        ensureGreen();

        Rectangle shape = new Rectangle(-45, 45, 45, -45);

        client().prepareIndex("shapes")
            .setId("Big_Rectangle")
            .setSource(jsonBuilder().startObject().field("shape", WellKnownText.toWKT(shape)).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareSearch(defaultIndexName)
                .setQuery(queryBuilder().intersectionQuery(defaultFieldName, "Big_Rectangle"))
                .get()
        );
        assertThat(e.getMessage(), containsString("source disabled"));
    }

    /** tests querying a random geometry collection with a point */
    public void testPointQuery() throws Exception {
        // Create a random geometry collection to index.
        Point point = nextPoint();
        GeometryCollection<Geometry> gcb = makeRandomGeometryCollectionWithoutCircle(point);

        // create mapping
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        XContentBuilder docSource = GeoJson.toXContent(gcb, jsonBuilder().startObject().field(defaultFieldName), ToXContent.EMPTY_PARAMS)
            .endObject();
        client().prepareIndex(defaultIndexName).setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        SearchResponse result = client().prepareSearch(defaultIndexName)
            .setQuery(queryBuilder().intersectionQuery(defaultFieldName, point))
            .get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testContainsShapeQuery() throws Exception {
        Polygon polygon = new Polygon(new LinearRing(new double[] { -30, 30, 30, -30, -30 }, new double[] { -30, -30, 30, 30, -30 }));
        Polygon innerPolygon = new Polygon(new LinearRing(new double[] { -5, 5, 5, -5, -5 }, new double[] { -5, -5, 5, 5, -5 }));
        createMapping(defaultIndexName, defaultFieldName);

        XContentBuilder docSource = GeoJson.toXContent(polygon, jsonBuilder().startObject().field(defaultFieldName), null).endObject();
        client().prepareIndex(defaultIndexName).setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();
        QueryBuilder filter = queryBuilder().shapeQuery(defaultFieldName, innerPolygon).relation(ShapeRelation.CONTAINS);
        SearchResponse response = client().prepareSearch(defaultIndexName).setQuery(filter).get();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value, equalTo(1L));
    }

    public void testExistsQuery() throws Exception {
        // Create a random geometry collection.
        GeometryCollection<Geometry> gcb = makeRandomGeometryCollectionWithoutCircle();
        logger.info("Created Random GeometryCollection containing {} shapes", gcb.size());

        createMapping(defaultIndexName, defaultFieldName);

        XContentBuilder docSource = GeoJson.toXContent(gcb, jsonBuilder().startObject().field(defaultFieldName), null).endObject();
        client().prepareIndex(defaultIndexName).setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        ExistsQueryBuilder eqb = existsQuery(defaultFieldName);
        SearchResponse result = client().prepareSearch(defaultIndexName).setQuery(eqb).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testIndexedShapeReference() throws Exception {

        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        Rectangle shape = new Rectangle(-45, 45, 45, -45);

        client().prepareIndex("shapes")
            .setId("Big_Rectangle")
            .setSource(GeoJson.toXContent(shape, jsonBuilder().startObject().field("shape"), null).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();
        client().prepareIndex(defaultIndexName)
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("name", "Document 1")
                    .startObject(defaultFieldName)
                    .field("type", "point")
                    .startArray("coordinates")
                    .value(-30)
                    .value(-30)
                    .endArray()
                    .endObject()
                    .endObject()
            )
            .setRefreshPolicy(IMMEDIATE)
            .get();

        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(queryBuilder().intersectionQuery(defaultFieldName, "Big_Rectangle"))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(queryBuilder().shapeQuery(defaultFieldName, "Big_Rectangle"))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testQueryRandomGeoCollection() throws Exception {
        // Create a random geometry collection.
        Polygon polygon = nextPolygon();
        GeometryCollection<Geometry> gcb = makeRandomGeometryCollectionWithoutCircle(polygon);

        logger.info("Created Random GeometryCollection containing {} shapes", gcb.size());

        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        XContentBuilder docSource = GeoJson.toXContent(gcb, jsonBuilder().startObject().field(defaultFieldName), null).endObject();
        client().prepareIndex(defaultIndexName).setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        SearchResponse result = client().prepareSearch(defaultIndexName)
            .setQuery(queryBuilder().intersectionQuery(defaultFieldName, polygon))
            .get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testShapeFilterWithDefinedGeoCollection() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        XContentBuilder docSource = jsonBuilder().startObject()
            .startObject(defaultFieldName)
            .field("type", "geometrycollection")
            .startArray("geometries")
            .startObject()
            .field("type", "point")
            .startArray("coordinates")
            .value(100.0)
            .value(0.0)
            .endArray()
            .endObject()
            .startObject()
            .field("type", "linestring")
            .startArray("coordinates")
            .startArray()
            .value(101.0)
            .value(0.0)
            .endArray()
            .startArray()
            .value(102.0)
            .value(1.0)
            .endArray()
            .endArray()
            .endObject()
            .endArray()
            .endObject()
            .endObject();
        client().prepareIndex(defaultIndexName).setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        Polygon polygon1 = new Polygon(
            new LinearRing(new double[] { 99.0, 99.0, 103.0, 103.0, 99.0 }, new double[] { -1.0, 3.0, 3.0, -1.0, -1.0 })
        );
        Polygon polygon2 = new Polygon(
            new LinearRing(new double[] { 199.0, 199.0, 193.0, 193.0, 199.0 }, new double[] { -11.0, 13.0, 13.0, -11.0, -11.0 })
        );

        {
            QueryBuilder filter = queryBuilder().intersectionQuery(defaultFieldName, new GeometryCollection<>(List.of(polygon1)));
            SearchResponse result = client().prepareSearch(defaultIndexName).setQuery(matchAllQuery()).setPostFilter(filter).get();
            assertSearchResponse(result);
            assertHitCount(result, 1);
        }
        {
            QueryBuilder filter = queryBuilder().intersectionQuery(defaultFieldName, new GeometryCollection<>(List.of(polygon2)));
            SearchResponse result = client().prepareSearch(defaultIndexName).setQuery(matchAllQuery()).setPostFilter(filter).get();
            assertSearchResponse(result);
            assertHitCount(result, 0);
        }
        {
            QueryBuilder filter = queryBuilder().intersectionQuery(defaultFieldName, new GeometryCollection<>(List.of(polygon1, polygon2)));
            SearchResponse result = client().prepareSearch(defaultIndexName).setQuery(matchAllQuery()).setPostFilter(filter).get();
            assertSearchResponse(result);
            assertHitCount(result, 1);
        }
        {
            // no shape
            QueryBuilder filter = queryBuilder().shapeQuery(defaultFieldName, GeometryCollection.EMPTY);
            SearchResponse result = client().prepareSearch(defaultIndexName).setQuery(matchAllQuery()).setPostFilter(filter).get();
            assertSearchResponse(result);
            assertHitCount(result, 0);
        }
    }

    public void testDistanceQuery() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        // Geo uses different units for x/y and radius, so we need a much larger number in the geo test
        double radius = fieldTypeName().contains("geo") ? 350000 : 35;
        Circle circle = new Circle(1, 0, radius);

        client().index(
            new IndexRequest(defaultIndexName).source(
                jsonBuilder().startObject().field(defaultFieldName, WellKnownText.toWKT(new Point(2, 2))).endObject()
            ).setRefreshPolicy(IMMEDIATE)
        ).actionGet();
        client().index(
            new IndexRequest(defaultIndexName).source(
                jsonBuilder().startObject().field(defaultFieldName, WellKnownText.toWKT(new Point(3, 1))).endObject()
            ).setRefreshPolicy(IMMEDIATE)
        ).actionGet();
        client().index(
            new IndexRequest(defaultIndexName).source(
                jsonBuilder().startObject().field(defaultFieldName, WellKnownText.toWKT(new Point(-20, -30))).endObject()
            ).setRefreshPolicy(IMMEDIATE)
        ).actionGet();
        client().index(
            new IndexRequest(defaultIndexName).source(
                jsonBuilder().startObject().field(defaultFieldName, WellKnownText.toWKT(new Point(20, 30))).endObject()
            ).setRefreshPolicy(IMMEDIATE)
        ).actionGet();

        SearchResponse response = client().prepareSearch(defaultIndexName)
            .setQuery(queryBuilder().shapeQuery(defaultFieldName, circle).relation(ShapeRelation.WITHIN))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);
        response = client().prepareSearch(defaultIndexName)
            .setQuery(queryBuilder().shapeQuery(defaultFieldName, circle).relation(ShapeRelation.INTERSECTS))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);
        response = client().prepareSearch(defaultIndexName)
            .setQuery(queryBuilder().shapeQuery(defaultFieldName, circle).relation(ShapeRelation.DISJOINT))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);
        response = client().prepareSearch(defaultIndexName)
            .setQuery(queryBuilder().shapeQuery(defaultFieldName, circle).relation(ShapeRelation.CONTAINS))
            .get();
        assertEquals(0, response.getHits().getTotalHits().value);
    }

    public void testIndexLineQueryPoints() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        Line line = makeRandomLine();

        client().prepareIndex(defaultIndexName)
            .setSource(jsonBuilder().startObject().field(defaultFieldName, WellKnownText.toWKT(line)).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();
        // all points from a line intersect with the line
        for (int i = 0; i < line.length(); i++) {
            Point point = new Point(line.getLon(i), line.getLat(i));
            SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
                .setTrackTotalHits(true)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.INTERSECTS))
                .get();
            assertSearchResponse(searchResponse);
            SearchHits searchHits = searchResponse.getHits();
            assertThat(searchHits.getTotalHits().value, equalTo(1L));
        }
    }

    public void testIndexPolygonQueryPoints() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        Polygon polygon = makeRandomPolygon();

        client().prepareIndex(defaultIndexName)
            .setSource(jsonBuilder().startObject().field(defaultFieldName, WellKnownText.toWKT(polygon)).endObject())
            .setRefreshPolicy(IMMEDIATE)
            .get();

        // all points from a polygon intersect with the polygon
        LinearRing linearRing = polygon.getPolygon();
        for (int i = 0; i < linearRing.length(); i++) {
            Point point = new Point(linearRing.getLon(i), linearRing.getLat(i));
            SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
                .setTrackTotalHits(true)
                .setQuery(queryBuilder().shapeQuery(defaultFieldName, point).relation(ShapeRelation.INTERSECTS))
                .get();
            assertSearchResponse(searchResponse);
            SearchHits searchHits = searchResponse.getHits();
            assertThat(searchHits.getTotalHits().value, equalTo(1L));
        }
    }

    public void testNeighbours() throws Exception {
        createMapping(defaultIndexName, defaultFieldName);
        ensureGreen();

        String[] polygons = new String[] {
            "POLYGON((1 1, 2 1, 2 2, 1 2, 1 1))", // center
            "POLYGON((0 1, 1 1, 1 2, 0 2, 0 1))", // west
            "POLYGON((0 2, 1 2, 1 3, 0 3, 0 2))", // northwest
            "POLYGON((1 2, 2 2, 2 3, 1 3, 1 2))", // north
            "POLYGON((2 2, 3 2, 3 3, 2 3, 2 2))", // northeast
            "POLYGON((2 1, 3 1, 3 2, 2 2, 2 1))", // east
            "POLYGON((2 0, 3 0, 3 1, 2 1, 2 0))", // southeast
            "POLYGON((1 0, 2 0, 2 1, 1 1, 1 0))", // south
            "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))" // southwest
        };

        for (String polygon : polygons) {
            client().prepareIndex(defaultIndexName)
                .setSource(jsonBuilder().startObject().field(defaultFieldName, polygon).endObject())
                .setRefreshPolicy(IMMEDIATE)
                .get();
        }
        Geometry center = WellKnownText.fromWKT(StandardValidator.instance(false), false, polygons[0]);
        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setTrackTotalHits(true)
            .setQuery(queryBuilder().shapeQuery(defaultFieldName, center).relation(ShapeRelation.INTERSECTS))
            .get();
        assertSearchResponse(searchResponse);
        SearchHits searchHits = searchResponse.getHits();
        assertThat(searchHits.getTotalHits().value, equalTo((long) polygons.length));
    }

    protected abstract Line makeRandomLine();

    protected abstract Polygon makeRandomPolygon();

    protected abstract GeometryCollection<Geometry> makeRandomGeometryCollection();

    protected abstract GeometryCollection<Geometry> makeRandomGeometryCollectionWithoutCircle(Geometry... extra);

    protected abstract Point nextPoint();

    protected abstract Polygon nextPolygon();

    protected abstract Polygon nextPolygon2();
}
