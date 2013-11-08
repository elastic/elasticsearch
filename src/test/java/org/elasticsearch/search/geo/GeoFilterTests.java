/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.exception.InvalidShapeException;
import com.spatial4j.core.shape.Shape;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.query.UnsupportedSpatialOperation;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.*;
import org.elasticsearch.common.geo.ShapeBuilder.MultiPolygonBuilder;
import org.elasticsearch.common.geo.ShapeBuilder.PolygonBuilder;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.geoBoundingBoxFilter;
import static org.elasticsearch.index.query.FilterBuilders.geoDistanceFilter;
import static org.elasticsearch.index.query.QueryBuilders.*;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.*;

/**
 *
 */
public class GeoFilterTests extends ElasticsearchIntegrationTest {

    private static boolean intersectSupport;
    private static boolean disjointSupport;
    private static boolean withinSupport;

    @BeforeClass
    public static void createNodes() throws Exception {
        intersectSupport = testRelationSupport(SpatialOperation.Intersects);
        disjointSupport = testRelationSupport(SpatialOperation.IsDisjointTo);
        withinSupport = testRelationSupport(SpatialOperation.IsWithin);
    }
    
    private static byte[] unZipData(String path) throws IOException {
        InputStream is = Streams.class.getResourceAsStream(path);
        if (is == null) {
            throw new FileNotFoundException("Resource [" + path + "] not found in classpath");
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPInputStream in = new GZIPInputStream(is);
        Streams.copy(in, out);

        is.close();
        out.close();

        return out.toByteArray();
    }

    @Test
    public void testShapeBuilders() {

        try {
            // self intersection polygon
            ShapeBuilder.newPolygon()
                    .point(-10, -10)
                    .point(10, 10)
                    .point(-10, 10)
                    .point(10, -10)
                    .close().build();
            assert false : "Self intersection not detected";
        } catch (InvalidShapeException e) {
        }

        // polygon with hole
        ShapeBuilder.newPolygon()
                .point(-10, -10).point(-10, 10).point(10, 10).point(10, -10)
                .hole()
                .point(-5, -5).point(-5, 5).point(5, 5).point(5, -5)
                .close().close().build();

        try {
            // polygon with overlapping hole
            ShapeBuilder.newPolygon()
                    .point(-10, -10).point(-10, 10).point(10, 10).point(10, -10)
                    .hole()
                    .point(-5, -5).point(-5, 11).point(5, 11).point(5, -5)
                    .close().close().build();

            assert false : "Self intersection not detected";
        } catch (InvalidShapeException e) {
        }

        try {
            // polygon with intersection holes
            ShapeBuilder.newPolygon()
                    .point(-10, -10).point(-10, 10).point(10, 10).point(10, -10)
                    .hole()
                    .point(-5, -5).point(-5, 5).point(5, 5).point(5, -5)
                    .close()
                    .hole()
                    .point(-5, -6).point(5, -6).point(5, -4).point(-5, -4)
                    .close()
                    .close().build();
            assert false : "Intersection of holes not detected";
        } catch (InvalidShapeException e) {
        }

        try {
            // Common line in polygon
            ShapeBuilder.newPolygon()
                    .point(-10, -10)
                    .point(-10, 10)
                    .point(-5, 10)
                    .point(-5, -5)
                    .point(-5, 20)
                    .point(10, 20)
                    .point(10, -10)
                    .close().build();
            assert false : "Self intersection not detected";
        } catch (InvalidShapeException e) {
        }

// Not specified
//        try {
//            // two overlapping polygons within a multipolygon
//            ShapeBuilder.newMultiPolygon()
//                .polygon()
//                    .point(-10, -10)
//                    .point(-10, 10)
//                    .point(10, 10)
//                    .point(10, -10)
//                .close()
//                .polygon()
//                    .point(-5, -5).point(-5, 5).point(5, 5).point(5, -5)
//                .close().build();
//            assert false : "Polygon intersection not detected";
//        } catch (InvalidShapeException e) {}

        // Multipolygon: polygon with hole and polygon within the whole
        ShapeBuilder.newMultiPolygon()
                .polygon()
                .point(-10, -10).point(-10, 10).point(10, 10).point(10, -10)
                .hole()
                .point(-5, -5).point(-5, 5).point(5, 5).point(5, -5)
                .close()
                .close()
                .polygon()
                .point(-4, -4).point(-4, 4).point(4, 4).point(4, -4)
                .close()
                .build();

// Not supported
//        try {
//            // Multipolygon: polygon with hole and polygon within the hole but overlapping
//            ShapeBuilder.newMultiPolygon()
//                .polygon()
//                    .point(-10, -10).point(-10, 10).point(10, 10).point(10, -10)
//                    .hole()
//                        .point(-5, -5).point(-5, 5).point(5, 5).point(5, -5)
//                    .close()
//                .close()
//                .polygon()
//                    .point(-4, -4).point(-4, 6).point(4, 6).point(4, -4)
//                .close()
//                .build();
//            assert false : "Polygon intersection not detected";
//        } catch (InvalidShapeException e) {}

    }

    @Test
    public void testShapeRelations() throws Exception {

        assert intersectSupport : "Intersect relation is not supported";
        assert disjointSupport : "Disjoint relation is not supported";
        assert withinSupport : "within relation is not supported";


        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("polygon")
                .startObject("properties")
                .startObject("area")
                .field("type", "geo_shape")
                .field("tree", "geohash")
                .field("store", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject().string();

        CreateIndexRequestBuilder mappingRequest = client().admin().indices().prepareCreate("shapes").addMapping("polygon", mapping);
        mappingRequest.execute().actionGet();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

        // Create a multipolygon with two polygons. The first is an rectangle of size 10x10
        // with a hole of size 5x5 equidistant from all sides. This hole in turn contains
        // the second polygon of size 4x4 equidistant from all sites
        MultiPolygonBuilder polygon = ShapeBuilder.newMultiPolygon()
                .polygon()
                .point(-10, -10).point(-10, 10).point(10, 10).point(10, -10)
                .hole()
                .point(-5, -5).point(-5, 5).point(5, 5).point(5, -5)
                .close()
                .close()
                .polygon()
                .point(-4, -4).point(-4, 4).point(4, 4).point(4, -4)
                .close();

        BytesReference data = polygon.toXContent("area", jsonBuilder().startObject()).endObject().bytes();
        client().prepareIndex("shapes", "polygon", "1").setSource(data).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        // Point in polygon
        SearchResponse result = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setFilter(FilterBuilders.geoIntersectionFilter("area", ShapeBuilder.newPoint(3, 3)))
                .execute().actionGet();
        assertHitCount(result, 1);
        assertFirstHit(result, hasId("1"));

        // Point in polygon hole
        result = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setFilter(FilterBuilders.geoIntersectionFilter("area", ShapeBuilder.newPoint(4.5, 4.5)))
                .execute().actionGet();
        assertHitCount(result, 0);

        // by definition the border of a polygon belongs to the inner
        // so the border of a polygons hole also belongs to the inner
        // of the polygon NOT the hole

        // Point on polygon border
        result = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setFilter(FilterBuilders.geoIntersectionFilter("area", ShapeBuilder.newPoint(10.0, 5.0)))
                .execute().actionGet();
        assertHitCount(result, 1);
        assertFirstHit(result, hasId("1"));

        // Point on hole border
        result = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setFilter(FilterBuilders.geoIntersectionFilter("area", ShapeBuilder.newPoint(5.0, 2.0)))
                .execute().actionGet();
        assertHitCount(result, 1);
        assertFirstHit(result, hasId("1"));

        if (disjointSupport) {
            // Point not in polygon
            result = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .setFilter(FilterBuilders.geoDisjointFilter("area", ShapeBuilder.newPoint(3, 3)))
                    .execute().actionGet();
            assertHitCount(result, 0);

            // Point in polygon hole
            result = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .setFilter(FilterBuilders.geoDisjointFilter("area", ShapeBuilder.newPoint(4.5, 4.5)))
                    .execute().actionGet();
            assertHitCount(result, 1);
            assertFirstHit(result, hasId("1"));
        }

        // Create a polygon that fills the empty area of the polygon defined above
        PolygonBuilder inverse = ShapeBuilder.newPolygon()
                .point(-5, -5).point(-5, 5).point(5, 5).point(5, -5)
                .hole()
                .point(-4, -4).point(-4, 4).point(4, 4).point(4, -4)
                .close()
                .close();

        data = inverse.toXContent("area", jsonBuilder().startObject()).endObject().bytes();
        client().prepareIndex("shapes", "polygon", "2").setSource(data).execute().actionGet();
        client().admin().indices().prepareRefresh().execute().actionGet();

        // re-check point on polygon hole
        result = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setFilter(FilterBuilders.geoIntersectionFilter("area", ShapeBuilder.newPoint(4.5, 4.5)))
                .execute().actionGet();
        assertHitCount(result, 1);
        assertFirstHit(result, hasId("2"));

        // Create Polygon with hole and common edge
        PolygonBuilder builder = ShapeBuilder.newPolygon()
                .point(-10, -10).point(-10, 10).point(10, 10).point(10, -10)
                .hole()
                .point(-5, -5).point(-5, 5).point(10, 5).point(10, -5)
                .close()
                .close();

        if (withinSupport) {
            // Polygon WithIn Polygon
            builder = ShapeBuilder.newPolygon()
                    .point(-30, -30).point(-30, 30).point(30, 30).point(30, -30).close();

            result = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .setFilter(FilterBuilders.geoWithinFilter("area", builder.build()))
                    .execute().actionGet();
            assertHitCount(result, 2);
        }

/* TODO: fix Polygon builder! It is not possible to cross the lats -180 and 180.
 *       A simple solution is following the path that is currently set up. When
 *       it's crossing the 180° lat set the new point to the intersection of line-
 *       segment and longitude and start building a new Polygon on the other side
 *       of the latitude. When crossing the latitude again continue drawing the
 *       first polygon. This approach can also applied to the holes because the
 *       commonline of hole and polygon will not be recognized as intersection.
 */

//        // Create a polygon crossing longitude 180.
//        builder = ShapeBuilder.newPolygon()
//            .point(170, -10).point(180, 10).point(170, -10).point(10, -10)
//            .close();
//
//        data = builder.toXContent("area", jsonBuilder().startObject()).endObject().bytes();
//        client().prepareIndex("shapes", "polygon", "1").setSource(data).execute().actionGet();
//        client().admin().indices().prepareRefresh().execute().actionGet();
    }

    @Test
    @Slow
    public void bulktest() throws Exception {
        byte[] bulkAction = unZipData("/org/elasticsearch/search/geo/gzippedmap.json");

        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("country")
                .startObject("properties")
                .startObject("pin")
                .field("type", "geo_point")
                .field("lat_lon", true)
                .field("store", true)
                .endObject()
                .startObject("location")
                .field("type", "geo_shape")
                .field("lat_lon", true)
                .field("store", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .string();

        client().admin().indices().prepareCreate("countries").addMapping("country", mapping).execute().actionGet();
        BulkResponse bulk = client().prepareBulk().add(bulkAction, 0, bulkAction.length, false, null, null).execute().actionGet();

        for (BulkItemResponse item : bulk.getItems()) {
            assert !item.isFailed() : "unable to index data";
        }

        client().admin().indices().prepareRefresh().execute().actionGet();
        String key = "DE";

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(fieldQuery("_id", key))
                .execute().actionGet();

        assertHitCount(searchResponse, 1);

        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), equalTo(key));
        }

        SearchResponse world = client().prepareSearch().addField("pin").setQuery(
                filteredQuery(
                        matchAllQuery(),
                        geoBoundingBoxFilter("pin")
                                .topLeft(90, -179.99999)
                                .bottomRight(-90, 179.99999))
        ).execute().actionGet();

        assertHitCount(world, 53);

        SearchResponse distance = client().prepareSearch().addField("pin").setQuery(
                filteredQuery(
                        matchAllQuery(),
                        geoDistanceFilter("pin").distance("425km").point(51.11, 9.851)
                )).execute().actionGet();

        assertHitCount(distance, 5);
        GeoPoint point = new GeoPoint();
        for (SearchHit hit : distance.getHits()) {
            String name = hit.getId();
            point.resetFromString(hit.fields().get("pin").getValue().toString());
            double dist = distance(point.getLat(), point.getLon(), 51.11, 9.851);

            assertThat("distance to '" + name + "'", dist, lessThanOrEqualTo(425000d));
            assertThat(name, anyOf(equalTo("CZ"), equalTo("DE"), equalTo("BE"), equalTo("NL"), equalTo("LU")));
            if (key.equals(name)) {
                assertThat(dist, equalTo(0d));
            }
        }
    }

    @Test
    public void testGeoHashFilter() throws IOException {
        String geohash = randomhash(10);
        logger.info("Testing geohash boundingbox filter for [{}]", geohash);

        List<String> neighbors = GeoHashUtils.neighbors(geohash);
        List<String> parentNeighbors = GeoHashUtils.neighbors(geohash.substring(0, geohash.length() - 1));
       
        logger.info("Neighbors {}", neighbors);
        logger.info("Parent Neighbors {}", parentNeighbors);

        String mapping = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("location")
                .startObject("properties")
                .startObject("pin")
                .field("type", "geo_point")
                .field("geohash_prefix", true)
                .field("latlon", false)
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .string();

        ensureYellow();

        client().admin().indices().prepareCreate("locations").addMapping("location", mapping).execute().actionGet();

        // Index a pin
        client().prepareIndex("locations", "location", "1").setCreate(true).setSource("{\"pin\":\"" + geohash + "\"}").execute().actionGet();

        // index neighbors
        for (int i = 0; i < neighbors.size(); i++) {
            client().prepareIndex("locations", "location", "N" + i).setCreate(true).setSource("{\"pin\":\"" + neighbors.get(i) + "\"}").execute().actionGet();
        }

        // Index parent cell
        client().prepareIndex("locations", "location", "p").setCreate(true).setSource("{\"pin\":\"" + geohash.substring(0, geohash.length() - 1) + "\"}").execute().actionGet();

        // index neighbors
        for (int i = 0; i < parentNeighbors.size(); i++) {
            client().prepareIndex("locations", "location", "p" + i).setCreate(true).setSource("{\"pin\":\"" + parentNeighbors.get(i) + "\"}").execute().actionGet();
        }

        client().admin().indices().prepareRefresh("locations").execute().actionGet();

        // Result of this geohash search should contain the geohash only
        SearchResponse results1 = client().prepareSearch("locations").setQuery(QueryBuilders.matchAllQuery()).setFilter("{\"geohash_cell\": {\"pin\": \"" + geohash + "\", \"neighbors\": false}}").execute().actionGet();
        assertHitCount(results1, 1);

        // Result of the parent query should contain the parent it self, its neighbors, the child and all its neighbors
        SearchResponse results2 = client().prepareSearch("locations").setQuery(QueryBuilders.matchAllQuery()).setFilter("{\"geohash_cell\": {\"pin\": \"" + geohash.substring(0, geohash.length() - 1) + "\", \"neighbors\": true}}").execute().actionGet();
        assertHitCount(results2, 2 + neighbors.size() + parentNeighbors.size());

        // Testing point formats and precision
        GeoPoint point = GeoHashUtils.decode(geohash);
        int precision = geohash.length();

        logger.info("Testing lat/lon format");
        String pointTest1 = "{\"geohash_cell\": {\"pin\": {\"lat\": " + point.lat() + ",\"lon\": " + point.lon() + "},\"precision\": " + precision + ",\"neighbors\": true}}";
        SearchResponse results3 = client().prepareSearch("locations").setQuery(QueryBuilders.matchAllQuery()).setFilter(pointTest1).execute().actionGet();
        assertHitCount(results3, neighbors.size() + 1);

        logger.info("Testing String format");
        String pointTest2 = "{\"geohash_cell\": {\"pin\": \"" + point.lat() + "," + point.lon() + "\",\"precision\": " + precision + ",\"neighbors\": true}}";
        SearchResponse results4 = client().prepareSearch("locations").setQuery(QueryBuilders.matchAllQuery()).setFilter(pointTest2).execute().actionGet();
        assertHitCount(results4, neighbors.size() + 1);

        logger.info("Testing Array format");
        String pointTest3 = "{\"geohash_cell\": {\"pin\": [" + point.lon() + "," + point.lat() + "],\"precision\": " + precision + ",\"neighbors\": true}}";
        SearchResponse results5 = client().prepareSearch("locations").setQuery(QueryBuilders.matchAllQuery()).setFilter(pointTest3).execute().actionGet();
        assertHitCount(results5, neighbors.size() + 1);
    }

    @Test
    public void testNeighbors() {
        // Simple root case
        assertThat(GeoHashUtils.neighbors("7"), containsInAnyOrder("4", "5", "6", "d", "e", "h", "k", "s"));

        // Root cases (Outer cells)
        assertThat(GeoHashUtils.neighbors("0"), containsInAnyOrder("1", "2", "3", "p", "r"));
        assertThat(GeoHashUtils.neighbors("b"), containsInAnyOrder("8", "9", "c", "x", "z"));
        assertThat(GeoHashUtils.neighbors("p"), containsInAnyOrder("n", "q", "r", "0", "2"));
        assertThat(GeoHashUtils.neighbors("z"), containsInAnyOrder("8", "b", "w", "x", "y"));

        // Root crossing dateline
        assertThat(GeoHashUtils.neighbors("2"), containsInAnyOrder("0", "1", "3", "8", "9", "p", "r", "x"));
        assertThat(GeoHashUtils.neighbors("r"), containsInAnyOrder("0", "2", "8", "n", "p", "q", "w", "x"));

        // level1: simple case
        assertThat(GeoHashUtils.neighbors("dk"), containsInAnyOrder("d5", "d7", "de", "dh", "dj", "dm", "ds", "dt"));

        // Level1: crossing cells
        assertThat(GeoHashUtils.neighbors("d5"), containsInAnyOrder("d4", "d6", "d7", "dh", "dk", "9f", "9g", "9u"));
        assertThat(GeoHashUtils.neighbors("d0"), containsInAnyOrder("d1", "d2", "d3", "9b", "9c", "6p", "6r", "3z"));
    }

    public static double distance(double lat1, double lon1, double lat2, double lon2) {
        return GeoUtils.EARTH_SEMI_MAJOR_AXIS * DistanceUtils.distHaversineRAD(
                DistanceUtils.toRadians(lat1),
                DistanceUtils.toRadians(lon1),
                DistanceUtils.toRadians(lat2),
                DistanceUtils.toRadians(lon2)
        );
    }

    protected static boolean testRelationSupport(SpatialOperation relation) {
        try {
            GeohashPrefixTree tree = new GeohashPrefixTree(SpatialContext.GEO, 3);
            RecursivePrefixTreeStrategy strategy = new RecursivePrefixTreeStrategy(tree, "area");
            Shape shape = SpatialContext.GEO.makePoint(0, 0);
            SpatialArgs args = new SpatialArgs(relation, shape);
            strategy.makeFilter(args);
            return true;
        } catch (UnsupportedSpatialOperation e) {
            return false;
        }
    }

    protected static String randomhash(int length) {
        return randomhash(getRandom(), length);
    }

    protected static String randomhash(Random random) {
        return randomhash(random, 2 + random.nextInt(10));
    }

    protected static String randomhash() {
        return randomhash(getRandom());
    }

    protected static String randomhash(Random random, int length) {
        final char[] BASE_32 = {
                '0', '1', '2', '3', '4', '5', '6', '7',
                '8', '9', 'b', 'c', 'd', 'e', 'f', 'g',
                'h', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
                's', 't', 'u', 'v', 'w', 'x', 'y', 'z'};

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(BASE_32[random.nextInt(BASE_32.length)]);
        }

        return sb.toString();
    }
}

