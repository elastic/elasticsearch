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

package org.elasticsearch.test.integration.search.geo;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.query.FilterBuilders.geoBoundingBoxFilter;
import static org.elasticsearch.index.query.FilterBuilders.geoDistanceFilter;
import static org.elasticsearch.index.query.QueryBuilders.fieldQuery;
import static org.elasticsearch.index.query.QueryBuilders.filteredQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.ShapeBuilder;
import org.elasticsearch.common.geo.ShapeBuilder.MultiPolygonBuilder;
import org.elasticsearch.common.geo.ShapeBuilder.PolygonBuilder;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.integration.AbstractNodesTests;

import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.query.UnsupportedSpatialOperation;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.exception.InvalidShapeException;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.Shape;

/**
 *
 */
public class GeoFilterTests extends AbstractNodesTests {

    private Client client;

    private boolean intersectSupport;
    private boolean disjointSupport;
    private boolean withinSupport;

    @BeforeClass
    public void createNodes() throws Exception {
        startNode("server1");
        startNode("server2");

        intersectSupport = testRelationSupport(SpatialOperation.Intersects);
        disjointSupport = testRelationSupport(SpatialOperation.IsDisjointTo);
        withinSupport = testRelationSupport(SpatialOperation.IsWithin);

        client = getClient();
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

    @AfterClass
    public void closeNodes() {
        client.close();
        closeAllNodes();
    }

    protected Client getClient() {
        return client("server1");
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
        } catch (InvalidShapeException e) {}

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
        } catch (InvalidShapeException e) {}

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
        } catch (InvalidShapeException e) {}

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
        } catch (InvalidShapeException e) {}

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

        assert intersectSupport: "Intersect relation is not supported";
//      assert disjointSupport: "Disjoint relation is not supported";
//      assert withinSupport: "within relation is not supported";

        assert !disjointSupport: "Disjoint relation is now supported";
        assert !withinSupport: "within relation is now supported";

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

        CreateIndexRequestBuilder mappingRequest = client.admin().indices().prepareCreate("shapes").addMapping("polygon", mapping);
        mappingRequest.execute().actionGet();
        client.admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().execute().actionGet();

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
        client.prepareIndex("shapes", "polygon", "1").setSource(data).execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        // Point in polygon
        SearchResponse result = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setFilter(FilterBuilders.geoIntersectionFilter("area", ShapeBuilder.newPoint(3, 3)))
                .execute().actionGet();
        assertHitCount(result, 1);
        assertFirstHit(result, hasId("1"));

        // Point in polygon hole
        result = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setFilter(FilterBuilders.geoIntersectionFilter("area", ShapeBuilder.newPoint(4.5, 4.5)))
                .execute().actionGet();
        assertHitCount(result, 0);

        // by definition the border of a polygon belongs to the inner
        // so the border of a polygons hole also belongs to the inner
        // of the polygon NOT the hole

        // Point on polygon border
        result = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setFilter(FilterBuilders.geoIntersectionFilter("area", ShapeBuilder.newPoint(10.0, 5.0)))
                .execute().actionGet();
        assertHitCount(result, 1);
        assertFirstHit(result, hasId("1"));

        // Point on hole border
        result = client.prepareSearch()
                .setQuery(matchAllQuery())
                .setFilter(FilterBuilders.geoIntersectionFilter("area", ShapeBuilder.newPoint(5.0, 2.0)))
                .execute().actionGet();
        assertHitCount(result, 1);
        assertFirstHit(result, hasId("1"));

        if(disjointSupport) {
            // Point not in polygon
            result = client.prepareSearch()
                    .setQuery(matchAllQuery())
                    .setFilter(FilterBuilders.geoDisjointFilter("area", ShapeBuilder.newPoint(3, 3)))
                    .execute().actionGet();
            assertHitCount(result, 0);

            // Point in polygon hole
            result = client.prepareSearch()
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
        client.prepareIndex("shapes", "polygon", "2").setSource(data).execute().actionGet();
        client.admin().indices().prepareRefresh().execute().actionGet();

        // re-check point on polygon hole
        result = client.prepareSearch()
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

        if(withinSupport) {
            // Polygon WithIn Polygon
            builder = ShapeBuilder.newPolygon()
                    .point(-30, -30).point(-30, 30).point(30, 30).point(30, -30).close();

            result = client.prepareSearch()
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
//        client.prepareIndex("shapes", "polygon", "1").setSource(data).execute().actionGet();
//        client.admin().indices().prepareRefresh().execute().actionGet();
    }

    @Test
    public void bulktest() throws Exception {
        byte[] bulkAction = unZipData("/org/elasticsearch/test/integration/search/geo/gzippedmap.json");

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

        client.admin().indices().prepareCreate("countries").addMapping("country", mapping).execute().actionGet();
        BulkResponse bulk = client.prepareBulk().add(bulkAction, 0, bulkAction.length, false, null, null).execute().actionGet();

        for(BulkItemResponse item : bulk.getItems()) {
            assert !item.isFailed(): "unable to index data";
        }

        client.admin().indices().prepareRefresh().execute().actionGet();        
        String key = "DE";

        SearchResponse searchResponse = client.prepareSearch()
                .setQuery(fieldQuery("_id", key))
                .execute().actionGet();

        assertHitCount(searchResponse, 1);

        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), equalTo(key));
        }

        SearchResponse world = client.prepareSearch().addField("pin").setQuery(
                filteredQuery(
                        matchAllQuery(),
                        geoBoundingBoxFilter("pin")
                            .topLeft(90, -179.99999)
                            .bottomRight(-90, 179.99999))
        ).execute().actionGet();

        assertHitCount(world, 246);

        SearchResponse distance = client.prepareSearch().addField("pin").setQuery(
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
            if(key.equals(name)) {
                assertThat(dist, equalTo(0d));
            }
        }
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
}

