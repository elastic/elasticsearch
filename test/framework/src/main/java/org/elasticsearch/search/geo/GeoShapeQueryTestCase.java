/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.geo;

import org.apache.lucene.geo.GeoTestUtil;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.query.ExistsQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.QueryBuilders.geoIntersectionQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class GeoShapeQueryTestCase extends GeoPointShapeQueryTestCase {

    public void testShapeFetchingPath() throws Exception {
        createIndex("shapes");
        createMapping(defaultIndexName, defaultGeoFieldName);
        ensureGreen();

        String geo = "\"geo\" : {\"type\":\"polygon\", \"coordinates\":[[[-10,-10],[10,-10],[10,10],[-10,10],[-10,-10]]]}";

        client().prepareIndex("shapes").setId("1")
            .setSource(
                String.format(
                    Locale.ROOT, "{ %s, \"1\" : { %s, \"2\" : { %s, \"3\" : { %s } }} }", geo, geo, geo, geo
                ), XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex(defaultIndexName).setId("1")
            .setSource(jsonBuilder().startObject().startObject(defaultGeoFieldName)
                .field("type", "polygon")
                .startArray("coordinates").startArray()
                .startArray().value(-20).value(-20).endArray()
                .startArray().value(20).value(-20).endArray()
                .startArray().value(20).value(20).endArray()
                .startArray().value(-20).value(20).endArray()
                .startArray().value(-20).value(-20).endArray()
                .endArray().endArray()
                .endObject().endObject()).setRefreshPolicy(IMMEDIATE).get();

        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery(defaultGeoFieldName, "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath(defaultGeoFieldName);
        SearchResponse result = client().prepareSearch(defaultIndexName).setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery(defaultGeoFieldName, "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.geo");
        result = client().prepareSearch(defaultIndexName).setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery(defaultGeoFieldName, "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.2.geo");
        result = client().prepareSearch(defaultIndexName).setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery(defaultGeoFieldName, "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.2.3.geo");
        result = client().prepareSearch(defaultIndexName).setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);

        // now test the query variant
        GeoShapeQueryBuilder query = QueryBuilders.geoShapeQuery(defaultGeoFieldName, "1")
            .indexedShapeIndex("shapes")
            .indexedShapePath(defaultGeoFieldName);
        result = client().prepareSearch(defaultIndexName).setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery(defaultGeoFieldName, "1")
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.geo");
        result = client().prepareSearch(defaultIndexName).setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery(defaultGeoFieldName, "1")
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.2.geo");
        result = client().prepareSearch(defaultIndexName).setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery(defaultGeoFieldName, "1")
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.2.3.geo");
        result = client().prepareSearch(defaultIndexName).setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testRandomGeoCollectionQuery() throws Exception {
        // Create a random geometry collection to index.
        GeometryCollection<Geometry> randomIndexCollection = GeometryTestUtils.randomGeometryCollectionWithoutCircle(false);
        org.apache.lucene.geo.Polygon randomPoly = GeoTestUtil.nextPolygon();

        assumeTrue("Skipping the check for the polygon with a degenerated dimension",
            randomPoly.maxLat - randomPoly.minLat > 8.4e-8 &&  randomPoly.maxLon - randomPoly.minLon > 8.4e-8);

        Polygon polygon = new Polygon(new LinearRing(randomPoly.getPolyLons(), randomPoly.getPolyLats()));
        List<Geometry> indexGeometries = new ArrayList<>();
        for (Geometry geometry : randomIndexCollection) {
            indexGeometries.add(geometry);
        }
        indexGeometries.add(polygon);
        GeometryCollection<Geometry> gcb = new GeometryCollection<>(indexGeometries);

        logger.info("Created Random GeometryCollection containing {} shapes", gcb.size());

        createMapping(defaultIndexName, defaultGeoFieldName, Settings.builder().put("index.number_of_shards", 1).build());
        ensureGreen();

        XContentBuilder docSource = GeoJson.toXContent(gcb, jsonBuilder().startObject().field(defaultGeoFieldName), null).endObject();
        client().prepareIndex(defaultIndexName).setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        // Create a random geometry collection to query
        GeometryCollection<Geometry> randomQueryCollection = GeometryTestUtils.randomGeometryCollection(false);

        List<Geometry> queryGeometries = new ArrayList<>();
        for (Geometry geometry : randomQueryCollection) {
            queryGeometries.add(geometry);
        }
        queryGeometries.add(polygon);
        GeometryCollection<Geometry> queryCollection = new GeometryCollection<>(queryGeometries);

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery(defaultGeoFieldName, queryCollection);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch(defaultIndexName).setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assertTrue("query: " + geoShapeQueryBuilder + " doc: " + Strings.toString(docSource),
            result.getHits().getTotalHits().value > 0);
    }

    // Test for issue #34418
    public void testEnvelopeSpanningDateline() throws Exception {
        createMapping(defaultIndexName, defaultGeoFieldName);
        ensureGreen();

        String doc1 = "{\"geo\": {\r\n" + "\"coordinates\": [\r\n" + "-33.918711,\r\n" + "18.847685\r\n" + "],\r\n" +
            "\"type\": \"Point\"\r\n" + "}}";
        client().index(new IndexRequest(defaultIndexName).id("1").source(doc1, XContentType.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc2 = "{\"geo\": {\r\n" + "\"coordinates\": [\r\n" + "-49.0,\r\n" + "18.847685\r\n" + "],\r\n" +
            "\"type\": \"Point\"\r\n" + "}}";
        client().index(new IndexRequest(defaultIndexName).id("2").source(doc2, XContentType.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc3 = "{\"geo\": {\r\n" + "\"coordinates\": [\r\n" + "49.0,\r\n" + "18.847685\r\n" + "],\r\n" +
            "\"type\": \"Point\"\r\n" + "}}";
        client().index(new IndexRequest(defaultIndexName).id("3").source(doc3, XContentType.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        @SuppressWarnings("unchecked") CheckedSupplier<GeoShapeQueryBuilder, IOException> querySupplier = randomFrom(
            () -> QueryBuilders.geoShapeQuery(
                defaultGeoFieldName,
                new Rectangle(-21, -39, 44, 9)
            ).relation(ShapeRelation.WITHIN),
            () -> {
                XContentBuilder builder = XContentFactory.jsonBuilder().startObject()
                    .startObject(defaultGeoFieldName)
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
                    .startObject(defaultGeoFieldName)
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

        SearchResponse response = client().prepareSearch(defaultIndexName)
            .setQuery(querySupplier.get())
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);
        assertNotEquals("1", response.getHits().getAt(0).getId());
        assertNotEquals("1", response.getHits().getAt(1).getId());
    }

    public void testGeometryCollectionRelations() throws Exception {
        Settings settings = Settings.builder().put("index.number_of_shards", 1).build();
        createMapping(defaultIndexName, defaultGeoFieldName, settings);
        ensureGreen();

        Rectangle envelope = new Rectangle(-10, 10, 10, -10);

        client().index(new IndexRequest(defaultIndexName)
            .source(jsonBuilder().startObject().field(defaultGeoFieldName, WellKnownText.toWKT(envelope)).endObject())
            .setRefreshPolicy(IMMEDIATE)).actionGet();

        {
            // A geometry collection that is fully within the indexed shape
            List<Geometry> geometries = new ArrayList<>();
            geometries.add(new Point(1, 2));
            geometries.add(new Point(-2, -1));
            GeometryCollection<Geometry> collection = new GeometryCollection<>(geometries);
            SearchResponse response = client().prepareSearch(defaultIndexName)
                .setQuery(geoShapeQuery(defaultGeoFieldName, collection).relation(ShapeRelation.CONTAINS))
                .get();
            assertEquals(1, response.getHits().getTotalHits().value);
            response = client().prepareSearch(defaultIndexName)
                .setQuery(geoShapeQuery(defaultGeoFieldName, collection).relation(ShapeRelation.INTERSECTS))
                .get();
            assertEquals(1, response.getHits().getTotalHits().value);
            response = client().prepareSearch(defaultIndexName)
                .setQuery(geoShapeQuery(defaultGeoFieldName, collection).relation(ShapeRelation.DISJOINT))
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
                .setQuery(geoShapeQuery(defaultGeoFieldName, collection).relation(ShapeRelation.CONTAINS))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
            response = client().prepareSearch(defaultIndexName)
                .setQuery(geoShapeQuery(defaultGeoFieldName, collection).relation(ShapeRelation.INTERSECTS))
                .get();
            assertEquals(1, response.getHits().getTotalHits().value);
            response = client().prepareSearch(defaultIndexName)
                .setQuery(geoShapeQuery(defaultGeoFieldName, collection).relation(ShapeRelation.DISJOINT))
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
                .setQuery(geoShapeQuery(defaultGeoFieldName, collection).relation(ShapeRelation.CONTAINS))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
            response = client().prepareSearch(defaultIndexName)
                .setQuery(geoShapeQuery(defaultGeoFieldName, collection).relation(ShapeRelation.INTERSECTS))
                .get();
            assertEquals(0, response.getHits().getTotalHits().value);
            response = client().prepareSearch(defaultIndexName)
                .setQuery(geoShapeQuery(defaultGeoFieldName, collection).relation(ShapeRelation.DISJOINT))
                .get();
            assertEquals(1, response.getHits().getTotalHits().value);
        }
    }

    public void testEdgeCases() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject(defaultGeoFieldName)
            .field("type", "geo_shape")
            .endObject().endObject().endObject();
        String mapping = Strings.toString(xcb);
        client().admin().indices().prepareCreate(defaultIndexName).setMapping(mapping).get();
        ensureGreen();

        client().prepareIndex(defaultIndexName).setId("blakely").setSource(jsonBuilder().startObject()
                .field("name", "Blakely Island")
                .startObject(defaultGeoFieldName)
                .field("type", "polygon")
                .startArray("coordinates").startArray()
                .startArray().value(-122.83).value(48.57).endArray()
                .startArray().value(-122.77).value(48.56).endArray()
                .startArray().value(-122.79).value(48.53).endArray()
                .startArray().value(-122.83).value(48.57).endArray() // close the polygon
                .endArray().endArray()
                .endObject()
                .endObject()).setRefreshPolicy(IMMEDIATE).get();

        Rectangle query = new Rectangle(-122.88, -122.82, 48.62, 48.54);

        // This search would fail if both geoshape indexing and geoshape filtering
        // used the bottom-level optimization in SpatialPrefixTree#recursiveGetNodes.
        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
                .setQuery(geoIntersectionQuery(defaultGeoFieldName, query))
                .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("blakely"));
    }

    public void testIndexedShapeReferenceSourceDisabled() throws Exception {
        createMapping(defaultIndexName, defaultGeoFieldName, Settings.builder().put("index.number_of_shards", 1).build());
        createIndex("shapes", Settings.EMPTY, "shape_type", "_source", "enabled=false");
        ensureGreen();

        Rectangle shape = new Rectangle(-45, 45, 45, -45);

        client().prepareIndex("shapes").setId("Big_Rectangle").setSource(jsonBuilder().startObject()
            .field("shape", WellKnownText.toWKT(shape)).endObject()).setRefreshPolicy(IMMEDIATE).get();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().prepareSearch(defaultIndexName)
            .setQuery(geoIntersectionQuery(defaultGeoFieldName, "Big_Rectangle")).get());
        assertThat(e.getMessage(), containsString("source disabled"));
    }

    /** tests querying a random geometry collection with a point */
    public void testPointQuery() throws Exception {
        // Create a random geometry collection to index.
        GeometryCollection<Geometry> randomCollection = GeometryTestUtils.randomGeometryCollectionWithoutCircle(false);
        Point point = GeometryTestUtils.randomPoint(false);
        List<Geometry> geometries = new ArrayList<>();
        for (Geometry geometry : randomCollection) {
            geometries.add(geometry);
        }
        geometries.add(point);
        GeometryCollection<Geometry> gcb = new GeometryCollection<>(geometries);

            // create mapping
        createMapping(defaultIndexName, defaultGeoFieldName);
        ensureGreen();

        XContentBuilder docSource =
            GeoJson.toXContent(gcb, jsonBuilder().startObject().field(defaultGeoFieldName), ToXContent.EMPTY_PARAMS).endObject();
        client().prepareIndex(defaultIndexName).setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery(defaultGeoFieldName, point);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch(defaultIndexName).setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testContainsShapeQuery() throws Exception {
        Polygon polygon = new Polygon(
            new LinearRing(
                new double[] {-30, 30, 30, -30, -30},
                new double[] {-30, -30, 30, 30, -30}
            )
        );
        Polygon innerPolygon = new Polygon(
            new LinearRing(
                new double[] {-5, 5, 5, -5, -5},
                new double[] {-5, -5, 5, 5, -5}
            )
        );
        createMapping(defaultIndexName, defaultGeoFieldName);

        XContentBuilder docSource = GeoJson.toXContent(polygon, jsonBuilder().startObject().field(defaultGeoFieldName), null).endObject();
        client().prepareIndex(defaultIndexName).setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();
        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery(defaultGeoFieldName, innerPolygon).relation(ShapeRelation.CONTAINS);
        SearchResponse response = client().prepareSearch(defaultIndexName).setQuery(filter).get();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value, equalTo(1L));
    }

    public void testExistsQuery() throws Exception {
        // Create a random geometry collection.
        GeometryCollection<Geometry> gcb = GeometryTestUtils.randomGeometryCollectionWithoutCircle(false);
        logger.info("Created Random GeometryCollection containing {} shapes", gcb.size());

        createMapping(defaultIndexName, defaultGeoFieldName);

        XContentBuilder docSource = GeoJson.toXContent(gcb, jsonBuilder().startObject().field(defaultGeoFieldName), null).endObject();
        client().prepareIndex(defaultIndexName).setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        ExistsQueryBuilder eqb = QueryBuilders.existsQuery(defaultGeoFieldName);
        SearchResponse result = client().prepareSearch(defaultIndexName).setQuery(eqb).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }

    public void testIndexedShapeReference() throws Exception {

        createMapping(defaultIndexName, defaultGeoFieldName);
        ensureGreen();

        Rectangle shape = new Rectangle(-45, 45, 45, -45);

        client().prepareIndex("shapes").setId("Big_Rectangle").setSource(
            GeoJson.toXContent(shape, jsonBuilder().startObject().field("shape"), null).endObject()).setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex(defaultIndexName).setId("1").setSource(jsonBuilder().startObject()
            .field("name", "Document 1")
            .startObject(defaultGeoFieldName)
            .field("type", "point")
            .startArray("coordinates").value(-30).value(-30).endArray()
            .endObject()
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        SearchResponse searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(geoIntersectionQuery(defaultGeoFieldName, "Big_Rectangle"))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        searchResponse = client().prepareSearch(defaultIndexName)
            .setQuery(geoShapeQuery(defaultGeoFieldName, "Big_Rectangle"))
            .get();

        assertSearchResponse(searchResponse);
        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testQueryRandomGeoCollection() throws Exception {
        // Create a random geometry collection.
        GeometryCollection<Geometry> randomCollection = GeometryTestUtils.randomGeometryCollectionWithoutCircle(false);
        org.apache.lucene.geo.Polygon randomPoly = GeoTestUtil.nextPolygon();
        Polygon polygon = new Polygon(new LinearRing(randomPoly.getPolyLons(), randomPoly.getPolyLats()));

        List<Geometry> geometries = new ArrayList<>();
        for(Geometry geometry : randomCollection) {
            geometries.add(geometry);
        }
        geometries.add(polygon);
        GeometryCollection<Geometry> gcb = new GeometryCollection<>(geometries);

        logger.info("Created Random GeometryCollection containing {} shapes", gcb.size());

        createMapping(defaultIndexName, defaultGeoFieldName);
        ensureGreen();

        XContentBuilder docSource = GeoJson.toXContent(gcb, jsonBuilder().startObject().field(defaultGeoFieldName), null).endObject();
        client().prepareIndex(defaultIndexName).setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery(defaultGeoFieldName, polygon);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch(defaultIndexName).setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assumeTrue("Skipping the check for the polygon with a degenerated dimension until "
                +" https://issues.apache.org/jira/browse/LUCENE-8634 is fixed",
            randomPoly.maxLat - randomPoly.minLat > 8.4e-8 &&  randomPoly.maxLon - randomPoly.minLon > 8.4e-8);
        assertHitCount(result, 1);
    }

    public void testShapeFilterWithDefinedGeoCollection() throws Exception {
        createMapping(defaultIndexName, defaultGeoFieldName);
        ensureGreen();

        XContentBuilder docSource = jsonBuilder().startObject().startObject(defaultGeoFieldName)
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
        client().prepareIndex(defaultIndexName).setId("1")
            .setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        Polygon polygon1 = new Polygon(
            new LinearRing(
                new double[] {99.0, 99.0, 103.0, 103.0, 99.0},
                new double[] {-1.0, 3.0, 3.0, -1.0, -1.0}
            )
        );
        Polygon polygon2 = new Polygon(
            new LinearRing(
                new double[] {199.0, 199.0, 193.0, 193.0, 199.0},
                new double[] {-11.0, 13.0, 13.0, -11.0, -11.0}
            )
        );

        {
            GeoShapeQueryBuilder filter =
                QueryBuilders.geoShapeQuery(defaultGeoFieldName,
                    new GeometryCollection<>(List.of(polygon1))).relation(ShapeRelation.INTERSECTS);
            SearchResponse result =
                client().prepareSearch(defaultIndexName).setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
            assertSearchResponse(result);
            assertHitCount(result, 1);
        }
        {
            GeoShapeQueryBuilder filter =
                QueryBuilders.geoShapeQuery(defaultGeoFieldName,
                    new GeometryCollection<>(List.of(polygon2))).relation(ShapeRelation.INTERSECTS);
            SearchResponse result =
                client().prepareSearch(defaultIndexName).setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
            assertSearchResponse(result);
            assertHitCount(result, 0);
        }
        {
            GeoShapeQueryBuilder filter =
                QueryBuilders.geoShapeQuery(defaultGeoFieldName,
                    new GeometryCollection<>(List.of(polygon1, polygon2))).relation(ShapeRelation.INTERSECTS);
            SearchResponse result =
                client().prepareSearch(defaultIndexName).setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
            assertSearchResponse(result);
            assertHitCount(result, 1);
        }
        {
            // no shape
            GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery(defaultGeoFieldName, GeometryCollection.EMPTY);
            SearchResponse result =
                client().prepareSearch(defaultIndexName).setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
            assertSearchResponse(result);
            assertHitCount(result, 0);
        }
    }

    public void testDistanceQuery() throws Exception {
        createMapping(defaultIndexName, defaultGeoFieldName);
        ensureGreen();

        Circle circle = new Circle(1, 0, 350000);

        client().index(new IndexRequest(defaultIndexName)
            .source(jsonBuilder().startObject().field(defaultGeoFieldName, WellKnownText.toWKT(new Point(2, 2))).endObject())
            .setRefreshPolicy(IMMEDIATE)).actionGet();
        client().index(new IndexRequest(defaultIndexName)
            .source(jsonBuilder().startObject().field(defaultGeoFieldName, WellKnownText.toWKT(new Point(3, 1))).endObject())
            .setRefreshPolicy(IMMEDIATE)).actionGet();
        client().index(new IndexRequest(defaultIndexName)
            .source(jsonBuilder().startObject().field(defaultGeoFieldName, WellKnownText.toWKT(new Point(-20, -30))).endObject())
            .setRefreshPolicy(IMMEDIATE)).actionGet();
        client().index(new IndexRequest(defaultIndexName)
            .source(jsonBuilder().startObject().field(defaultGeoFieldName, WellKnownText.toWKT(new Point(20, 30))).endObject())
            .setRefreshPolicy(IMMEDIATE)).actionGet();

        SearchResponse response = client().prepareSearch(defaultIndexName)
            .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, circle).relation(ShapeRelation.WITHIN))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);
        response = client().prepareSearch(defaultIndexName)
            .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, circle).relation(ShapeRelation.INTERSECTS))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);
        response = client().prepareSearch(defaultIndexName)
            .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, circle).relation(ShapeRelation.DISJOINT))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);
        response = client().prepareSearch(defaultIndexName)
            .setQuery(QueryBuilders.geoShapeQuery(defaultGeoFieldName, circle).relation(ShapeRelation.CONTAINS))
            .get();
        assertEquals(0, response.getHits().getTotalHits().value);
    }

    public void testIndexRectangleSpanningDateLine() throws Exception {
        createMapping(defaultIndexName, defaultGeoFieldName);
        ensureGreen();

        Rectangle envelope = new Rectangle(178, -178, 10, -10);

        XContentBuilder docSource = GeoJson.toXContent(envelope, jsonBuilder().startObject().field(defaultGeoFieldName), null).endObject();
        client().prepareIndex(defaultIndexName).setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        Point filterShape = new Point(179, 0);

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery(defaultGeoFieldName, filterShape);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch(defaultIndexName).setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }
}
