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
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.index.mapper.LegacyGeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MapperParsingException;
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
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public class GeoShapeQueryTests extends GeoQueryTests {
    protected static final String[] PREFIX_TREES = new String[] {
        LegacyGeoShapeFieldMapper.PrefixTrees.GEOHASH,
        LegacyGeoShapeFieldMapper.PrefixTrees.QUADTREE
    };

    @Override
    protected XContentBuilder createDefaultMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject("geo")
            .field("type", "geo_shape")
            .endObject()
            .endObject()
            .endObject();
        return xcb;
    }

    protected XContentBuilder createPrefixTreeMapping(String tree) throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject("geo")
            .field("type", "geo_shape")
            .field("tree", tree)
            .endObject()
            .endObject()
            .endObject();
        return xcb;
    }

    protected XContentBuilder createRandomMapping() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject()
            .startObject("properties").startObject("geo")
            .field("type", "geo_shape");
        if (randomBoolean()) {
            xcb = xcb.field("tree", randomFrom(PREFIX_TREES));
        }
        xcb = xcb.endObject().endObject().endObject();

        return xcb;
    }

    public void testShapeFetchingPath() throws Exception {
        createIndex("shapes");
        client().admin().indices().prepareCreate("test").addMapping("type", "geo", "type=geo_shape").get();

        String location = "\"geo\" : {\"type\":\"polygon\", \"coordinates\":[[[-10,-10],[10,-10],[10,10],[-10,10],[-10,-10]]]}";

        client().prepareIndex("shapes", "type", "1")
            .setSource(
                String.format(
                    Locale.ROOT, "{ %s, \"1\" : { %s, \"2\" : { %s, \"3\" : { %s } }} }", location, location, location, location
                ), XContentType.JSON)
            .setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("test", "type", "1")
            .setSource(jsonBuilder().startObject().startObject("geo")
                .field("type", "polygon")
                .startArray("coordinates").startArray()
                .startArray().value(-20).value(-20).endArray()
                .startArray().value(20).value(-20).endArray()
                .startArray().value(20).value(20).endArray()
                .startArray().value(-20).value(20).endArray()
                .startArray().value(-20).value(-20).endArray()
                .endArray().endArray()
                .endObject().endObject()).setRefreshPolicy(IMMEDIATE).get();

        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery("geo", "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("geo");
        SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("geo", "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.geo");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("geo", "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.2.geo");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        filter = QueryBuilders.geoShapeQuery("geo", "1").relation(ShapeRelation.INTERSECTS)
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.2.3.geo");
        result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery())
            .setPostFilter(filter).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);

        // now test the query variant
        GeoShapeQueryBuilder query = QueryBuilders.geoShapeQuery("geo", "1")
            .indexedShapeIndex("shapes")
            .indexedShapePath("geo");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery("geo", "1")
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.geo");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery("geo", "1")
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.2.geo");
        result = client().prepareSearch("test").setQuery(query).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
        query = QueryBuilders.geoShapeQuery("geo", "1")
            .indexedShapeIndex("shapes")
            .indexedShapePath("1.2.3.geo");
        result = client().prepareSearch("test").setQuery(query).get();
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

        XContentBuilder mapping = createRandomMapping();
        Settings settings = Settings.builder().put("index.number_of_shards", 1).build();
        client().admin().indices().prepareCreate("test").addMapping("_doc",mapping).setSettings(settings).get();
        ensureGreen();

        XContentBuilder docSource = GeoJson.toXContent(gcb, jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test","_doc").setId("1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        // Create a random geometry collection to query
        GeometryCollection<Geometry> randomQueryCollection = GeometryTestUtils.randomGeometryCollection(false);

        List<Geometry> queryGeometries = new ArrayList<>();
        for (Geometry geometry : randomQueryCollection) {
            queryGeometries.add(geometry);
        }
        queryGeometries.add(polygon);
        GeometryCollection<Geometry> queryCollection = new GeometryCollection<>(queryGeometries);

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery("geo", queryCollection);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assertThat(result.getHits().getHits().length, greaterThan(0));
    }

    // Test for issue #34418
    public void testEnvelopeSpanningDateline() throws Exception {
        XContentBuilder mapping = createDefaultMapping();
        client().admin().indices().prepareCreate("test").addMapping("_doc",mapping).get();
        ensureGreen();

        String doc1 = "{\"geo\": {\r\n" + "\"coordinates\": [\r\n" + "-33.918711,\r\n" + "18.847685\r\n" + "],\r\n" +
            "\"type\": \"Point\"\r\n" + "}}";
        client().index(new IndexRequest("test").id("1").source(doc1, XContentType.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc2 = "{\"geo\": {\r\n" + "\"coordinates\": [\r\n" + "-49.0,\r\n" + "18.847685\r\n" + "],\r\n" +
            "\"type\": \"Point\"\r\n" + "}}";
        client().index(new IndexRequest("test").id("2").source(doc2, XContentType.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        String doc3 = "{\"geo\": {\r\n" + "\"coordinates\": [\r\n" + "49.0,\r\n" + "18.847685\r\n" + "],\r\n" +
            "\"type\": \"Point\"\r\n" + "}}";
        client().index(new IndexRequest("test").id("3").source(doc3, XContentType.JSON).setRefreshPolicy(IMMEDIATE)).actionGet();

        @SuppressWarnings("unchecked") CheckedSupplier<GeoShapeQueryBuilder, IOException> querySupplier = randomFrom(
            () -> QueryBuilders.geoShapeQuery(
                "geo",
                new Rectangle(-21, -39, 44, 9)
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
        assertHitCount(response, 2);
        assertThat(response.getHits().getAt(0).getId(), not(equalTo("1")));
        assertThat(response.getHits().getAt(1).getId(), not(equalTo("1")));
    }

    public void testGeometryCollectionRelations() throws Exception {
        XContentBuilder mapping = createDefaultMapping();
        createIndex("test", Settings.builder().put("index.number_of_shards", 1).build(), "doc", mapping);

        Rectangle envelope = new Rectangle(-10, 10, 10, -10);

        client().index(new IndexRequest("test")
            .source(jsonBuilder().startObject().field("geo", WellKnownText.toWKT(envelope)).endObject())
            .setRefreshPolicy(IMMEDIATE)).actionGet();

        {
            // A geometry collection that is fully within the indexed shape
            List<Geometry> geometries = new ArrayList<>();
            geometries.add(new Point(1, 2));
            geometries.add(new Point(-2, -1));
            GeometryCollection<Geometry> collection = new GeometryCollection<>(geometries);
            SearchResponse response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", collection).relation(ShapeRelation.CONTAINS))
                .get();
            assertHitCount(response, 1);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", collection).relation(ShapeRelation.INTERSECTS))
                .get();
            assertHitCount(response, 1);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", collection).relation(ShapeRelation.DISJOINT))
                .get();
            assertHitCount(response, 0);
        }
        {
            // A geometry collection that is partially within the indexed shape
            List<Geometry> geometries = new ArrayList<>();
            geometries.add(new Point(1, 2));
            geometries.add(new Point(20, 30));
            GeometryCollection<Geometry> collection = new GeometryCollection<>(geometries);
            SearchResponse response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", collection).relation(ShapeRelation.CONTAINS))
                .get();
            assertHitCount(response, 0);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", collection).relation(ShapeRelation.INTERSECTS))
                .get();
            assertHitCount(response, 1);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", collection).relation(ShapeRelation.DISJOINT))
                .get();
            assertHitCount(response, 0);
        }
        {
            // A geometry collection that is disjoint with the indexed shape
            List<Geometry> geometries = new ArrayList<>();
            geometries.add(new Point(-20, -30));
            geometries.add(new Point(20, 30));
            GeometryCollection<Geometry> collection = new GeometryCollection<>(geometries);
            SearchResponse response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", collection).relation(ShapeRelation.CONTAINS))
                .get();
            assertHitCount(response, 0);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", collection).relation(ShapeRelation.INTERSECTS))
                .get();
            assertHitCount(response, 0);
            response = client().prepareSearch("test")
                .setQuery(geoShapeQuery("geo", collection).relation(ShapeRelation.DISJOINT))
                .get();
            assertHitCount(response, 1);
        }
    }

    public void testEdgeCases() throws Exception {
        XContentBuilder xcb = XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("geo")
            .field("type", "geo_shape")
            .endObject().endObject().endObject().endObject();
        String mapping = Strings.toString(xcb);
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping, XContentType.JSON).get();
        ensureGreen();

        client().prepareIndex("test", "type1", "blakely").setSource(jsonBuilder().startObject()
            .field("name", "Blakely Island")
            .startObject("geo")
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
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(geoIntersectionQuery("geo", query))
            .get();

        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("blakely"));
    }

    public void testIndexedShapeReferenceSourceDisabled() throws Exception {
        XContentBuilder mapping = createDefaultMapping();
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping).get();
        createIndex("shapes", Settings.EMPTY, "shape_type", "_source", "enabled=false");
        ensureGreen();

        Rectangle shape = new Rectangle(-45, 45, 45, -45);

        client().prepareIndex("shapes", "shape_type", "Big_Rectangle").setSource(jsonBuilder().startObject()
            .field("shape", WellKnownText.toWKT(shape)).endObject()).setRefreshPolicy(IMMEDIATE).get();

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> client().prepareSearch("test")
            .setQuery(geoIntersectionQuery("geo", "Big_Rectangle")).get());
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
        if (randomBoolean()) {
            client().admin().indices().prepareCreate("test").addMapping("type", "geo", "type=geo_shape")
                .execute().actionGet();
        } else {
            client().admin().indices().prepareCreate("test").addMapping("type", "geo", "type=geo_shape,tree=quadtree")
                .execute().actionGet();
        }

        XContentBuilder docSource = GeoJson.toXContent(gcb, jsonBuilder().startObject().field("geo"), ToXContent.EMPTY_PARAMS).endObject();
        client().prepareIndex("test", "type", "1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery("geo", point);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
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

        if (randomBoolean()) {
            client().admin().indices().prepareCreate("test").addMapping("type", "geo", "type=geo_shape,tree=quadtree")
                .execute().actionGet();
        } else {
            client().admin().indices().prepareCreate("test").addMapping("type", "geo", "type=geo_shape")
                .execute().actionGet();
        }
        ensureGreen();

        XContentBuilder docSource = GeoJson.toXContent(polygon, jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test", "type", "1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();
        GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery("geo", innerPolygon).relation(ShapeRelation.CONTAINS);
        SearchResponse response = client().prepareSearch("test").setQuery(filter).get();
        assertSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value, equalTo(1L));
    }

    public void testExistsQuery() throws Exception {
        // Create a random geometry collection.
        GeometryCollection<Geometry> gcb = GeometryTestUtils.randomGeometryCollectionWithoutCircle(false);
        logger.info("Created Random GeometryCollection containing {} shapes", gcb.size());

        XContentBuilder builder = createRandomMapping();
        client().admin().indices().prepareCreate("test").addMapping("type", builder)
            .execute().actionGet();

        XContentBuilder docSource = GeoJson.toXContent(gcb, jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test", "type", "1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        ExistsQueryBuilder eqb = QueryBuilders.existsQuery("geo");
        SearchResponse result = client().prepareSearch("test").setQuery(eqb).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
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

        Geometry geometry = GeometryTestUtils.randomGeometry(false);
        try {
            client().prepareIndex("geo_points_only", "type1", "1")
                .setSource(GeoJson.toXContent(geometry, jsonBuilder().startObject().field("location"), null).endObject())
                .setRefreshPolicy(IMMEDIATE).get();
        } catch (MapperParsingException e) {
            // Random geometry generator created something other than a POINT type, verify the correct exception is thrown
            assertThat(e.getMessage(), containsString("is configured for points only"));
            return;
        }

        // test that point was inserted
        SearchResponse response = client().prepareSearch("geo_points_only").setTypes("type1")
            .setQuery(geoIntersectionQuery("location", geometry))
            .get();

        assertHitCount(response, 1);
    }

    public void testPointsOnlyExplicit() throws Exception {
        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("type1")
            .startObject("properties").startObject("geo")
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
        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(false);
        client().prepareIndex("geo_points_only", "type1", "1")
            .setSource(GeoJson.toXContent(multiPoint, jsonBuilder().startObject().field("geo"), null).endObject())
            .setRefreshPolicy(IMMEDIATE).get();

        // POINT
        Point point =  GeometryTestUtils.randomPoint(false);
        client().prepareIndex("geo_points_only", "type1", "2")
            .setSource(GeoJson.toXContent(point, jsonBuilder().startObject().field("geo"), null).endObject())
            .setRefreshPolicy(IMMEDIATE).get();

        // test that point was inserted
        SearchResponse response = client().prepareSearch("geo_points_only").setTypes("type1")
            .setQuery(matchAllQuery())
            .get();

        assertHitCount(response, 2);
    }

    public void testIndexedShapeReference() throws Exception {
        String mapping = Strings.toString(createDefaultMapping());
        client().admin().indices().prepareCreate("test").addMapping("type1", mapping, XContentType.JSON).get();
        createIndex("shapes");
        ensureGreen();

        Rectangle shape = new Rectangle(-45, 45, 45, -45);

        client().prepareIndex("shapes", "shape_type", "Big_Rectangle").setSource(
            GeoJson.toXContent(shape, jsonBuilder().startObject().field("shape"), null).endObject()).setRefreshPolicy(IMMEDIATE).get();
        client().prepareIndex("test", "type1", "1").setSource(jsonBuilder().startObject()
            .field("name", "Document 1")
            .startObject("geo")
            .field("type", "point")
            .startArray("coordinates").value(-30).value(-30).endArray()
            .endObject()
            .endObject()).setRefreshPolicy(IMMEDIATE).get();

        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(geoIntersectionQuery("geo", "Big_Rectangle"))
            .get();

        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));

        searchResponse = client().prepareSearch("test")
            .setQuery(geoShapeQuery("geo", "Big_Rectangle"))
            .get();

        assertSearchResponse(searchResponse);
        assertHitCount(searchResponse, 1);
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
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

        MultiPoint multiPoint = GeometryTestUtils.randomMultiPoint(false);
        client().prepareIndex("test", "type", "1")
            .setSource(GeoJson.toXContent(multiPoint, jsonBuilder().startObject().field("location"), null).endObject())
            .setRefreshPolicy(IMMEDIATE).get();

        SearchResponse response = client().prepareSearch("test")
            .setQuery(geoShapeQuery("alias", multiPoint))
            .get();
        assertHitCount(response, 1);
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

        XContentBuilder builder = createRandomMapping();
        client().admin().indices().prepareCreate("test")
            .addMapping("type", builder).get();

        XContentBuilder docSource = GeoJson.toXContent(gcb, jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test", "type", "1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery("geo", polygon);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assumeTrue("Skipping the check for the polygon with a degenerated dimension until "
                +" https://issues.apache.org/jira/browse/LUCENE-8634 is fixed",
            randomPoly.maxLat - randomPoly.minLat > 8.4e-8 &&  randomPoly.maxLon - randomPoly.minLon > 8.4e-8);
        assertHitCount(result, 1);
    }

    public void testShapeFilterWithDefinedGeoCollection() throws Exception {
        createIndex("shapes");
        client().admin().indices().prepareCreate("test").addMapping("type", "geo", "type=geo_shape,tree=quadtree")
            .get();

        XContentBuilder docSource = jsonBuilder().startObject().startObject("geo")
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
                QueryBuilders.geoShapeQuery("geo", new GeometryCollection<>(List.of(polygon1))).relation(ShapeRelation.INTERSECTS);
            SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
            assertSearchResponse(result);
            assertHitCount(result, 1);
        }
        {
            GeoShapeQueryBuilder filter =
                QueryBuilders.geoShapeQuery("geo", new GeometryCollection<>(List.of(polygon2))).relation(ShapeRelation.INTERSECTS);
            SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
            assertSearchResponse(result);
            assertHitCount(result, 0);
        }
        {
            GeoShapeQueryBuilder filter =
                QueryBuilders.geoShapeQuery("geo",
                    new GeometryCollection<>(List.of(polygon1, polygon2))).relation(ShapeRelation.INTERSECTS);
            SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
            assertSearchResponse(result);
            assertHitCount(result, 1);
        }
        {
            // no shape
            GeoShapeQueryBuilder filter = QueryBuilders.geoShapeQuery("geo", GeometryCollection.EMPTY);
            SearchResponse result = client().prepareSearch("test").setQuery(QueryBuilders.matchAllQuery()).setPostFilter(filter).get();
            assertSearchResponse(result);
            assertHitCount(result, 0);
        }
    }

    public void testDistanceQuery() throws Exception {
        String mapping = Strings.toString(createRandomMapping());
        client().admin().indices().prepareCreate("test_distance").addMapping("type1", mapping, XContentType.JSON).get();
        ensureGreen();

        Circle circle = new Circle(1, 0, 350000);

        client().index(new IndexRequest("test_distance")
            .source(jsonBuilder().startObject().field("geo", WellKnownText.toWKT(new Point(2, 2))).endObject())
            .setRefreshPolicy(IMMEDIATE)).actionGet();
        client().index(new IndexRequest("test_distance")
            .source(jsonBuilder().startObject().field("geo", WellKnownText.toWKT(new Point(3, 1))).endObject())
            .setRefreshPolicy(IMMEDIATE)).actionGet();
        client().index(new IndexRequest("test_distance")
            .source(jsonBuilder().startObject().field("geo", WellKnownText.toWKT(new Point(-20, -30))).endObject())
            .setRefreshPolicy(IMMEDIATE)).actionGet();
        client().index(new IndexRequest("test_distance")
            .source(jsonBuilder().startObject().field("geo", WellKnownText.toWKT(new Point(20, 30))).endObject())
            .setRefreshPolicy(IMMEDIATE)).actionGet();

        SearchResponse response = client().prepareSearch("test_distance")
            .setQuery(QueryBuilders.geoShapeQuery("geo", circle).relation(ShapeRelation.WITHIN))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);
        response = client().prepareSearch("test_distance")
            .setQuery(QueryBuilders.geoShapeQuery("geo", circle).relation(ShapeRelation.INTERSECTS))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);
        response = client().prepareSearch("test_distance")
            .setQuery(QueryBuilders.geoShapeQuery("geo", circle).relation(ShapeRelation.DISJOINT))
            .get();
        assertEquals(2, response.getHits().getTotalHits().value);
        response = client().prepareSearch("test_distance")
            .setQuery(QueryBuilders.geoShapeQuery("geo", circle).relation(ShapeRelation.CONTAINS))
            .get();
        assertEquals(0, response.getHits().getTotalHits().value);
    }

    public void testIndexRectangleSpanningDateLine() throws Exception {
        String mapping = Strings.toString(createRandomMapping());

        client().admin().indices().prepareCreate("test").addMapping("type1", mapping, XContentType.JSON).get();
        ensureGreen();

        Rectangle envelope = new Rectangle(178, -178, 10, -10);

        XContentBuilder docSource = GeoJson.toXContent(envelope, jsonBuilder().startObject().field("geo"), null).endObject();
        client().prepareIndex("test", "type1", "1").setSource(docSource).setRefreshPolicy(IMMEDIATE).get();

        Point filterShape = new Point(179, 0);

        GeoShapeQueryBuilder geoShapeQueryBuilder = QueryBuilders.geoShapeQuery("geo", filterShape);
        geoShapeQueryBuilder.relation(ShapeRelation.INTERSECTS);
        SearchResponse result = client().prepareSearch("test").setQuery(geoShapeQueryBuilder).get();
        assertSearchResponse(result);
        assertHitCount(result, 1);
    }
}
