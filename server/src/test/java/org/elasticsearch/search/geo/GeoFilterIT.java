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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.query.SpatialArgs;
import org.apache.lucene.spatial.query.SpatialOperation;
import org.apache.lucene.spatial.query.UnsupportedSpatialOperation;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.common.geo.builders.CoordinatesBuilder;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiPolygonBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.Streams;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.VersionUtils;
import org.junit.BeforeClass;
import org.locationtech.spatial4j.context.SpatialContext;
import org.locationtech.spatial4j.distance.DistanceUtils;
import org.locationtech.spatial4j.exception.InvalidShapeException;
import org.locationtech.spatial4j.shape.Shape;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Random;
import java.util.zip.GZIPInputStream;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.geometry.utils.Geohash.addNeighbors;
import static org.elasticsearch.index.query.QueryBuilders.geoBoundingBoxQuery;
import static org.elasticsearch.index.query.QueryBuilders.geoDistanceQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class GeoFilterIT extends ESIntegTestCase {

    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

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

    public void testShapeBuilders() {
        try {
            // self intersection polygon
            new PolygonBuilder(new CoordinatesBuilder()
                    .coordinate(-10, -10)
                    .coordinate(10, 10)
                    .coordinate(-10, 10)
                    .coordinate(10, -10)
                    .close())
                    .buildS4J();
            fail("Self intersection not detected");
        } catch (InvalidShapeException e) {
        }

        // polygon with hole
        new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-10, -10).coordinate(-10, 10).coordinate(10, 10).coordinate(10, -10).close())
                .hole(new LineStringBuilder(new CoordinatesBuilder().coordinate(-5, -5).coordinate(-5, 5).coordinate(5, 5)
                        .coordinate(5, -5).close()))
                .buildS4J();
        try {
            // polygon with overlapping hole
            new PolygonBuilder(new CoordinatesBuilder()
                    .coordinate(-10, -10).coordinate(-10, 10).coordinate(10, 10).coordinate(10, -10).close())
                    .hole(new LineStringBuilder(new CoordinatesBuilder()
                    .coordinate(-5, -5).coordinate(-5, 11).coordinate(5, 11).coordinate(5, -5).close()))
                    .buildS4J();

            fail("Self intersection not detected");
        } catch (InvalidShapeException e) {
        }

        try {
            // polygon with intersection holes
            new PolygonBuilder(new CoordinatesBuilder()
                    .coordinate(-10, -10).coordinate(-10, 10).coordinate(10, 10).coordinate(10, -10).close())
                    .hole(new LineStringBuilder(new CoordinatesBuilder().coordinate(-5, -5).coordinate(-5, 5).coordinate(5, 5)
                            .coordinate(5, -5).close()))
                    .hole(new LineStringBuilder(new CoordinatesBuilder().coordinate(-5, -6).coordinate(5, -6).coordinate(5, -4)
                            .coordinate(-5, -4).close()))
                    .buildS4J();
            fail("Intersection of holes not detected");
        } catch (InvalidShapeException e) {
        }

        try {
            // Common line in polygon
            new PolygonBuilder(new CoordinatesBuilder()
                    .coordinate(-10, -10)
                    .coordinate(-10, 10)
                    .coordinate(-5, 10)
                    .coordinate(-5, -5)
                    .coordinate(-5, 20)
                    .coordinate(10, 20)
                    .coordinate(10, -10)
                    .close())
                    .buildS4J();
            fail("Self intersection not detected");
        } catch (InvalidShapeException e) {
        }

        // Multipolygon: polygon with hole and polygon within the whole
        new MultiPolygonBuilder()
                .polygon(new PolygonBuilder(
                             new CoordinatesBuilder().coordinate(-10, -10)
                             .coordinate(-10, 10)
                             .coordinate(10, 10)
                             .coordinate(10, -10).close())
                        .hole(new LineStringBuilder(
                              new CoordinatesBuilder().coordinate(-5, -5)
                               .coordinate(-5, 5)
                               .coordinate(5, 5)
                               .coordinate(5, -5).close())))
                .polygon(new PolygonBuilder(
                            new CoordinatesBuilder()
                            .coordinate(-4, -4)
                            .coordinate(-4, 4)
                            .coordinate(4, 4)
                            .coordinate(4, -4).close()))
                .buildS4J();
    }

    public void testShapeRelations() throws Exception {
        assertTrue( "Intersect relation is not supported", intersectSupport);
        assertTrue("Disjoint relation is not supported", disjointSupport);
        assertTrue("within relation is not supported", withinSupport);

        String mapping = Strings.toString(XContentFactory.jsonBuilder()
                .startObject()
                .startObject("polygon")
                .startObject("properties")
                .startObject("area")
                .field("type", "geo_shape")
                .field("tree", "geohash")
                .endObject()
                .endObject()
                .endObject()
                .endObject());

        CreateIndexRequestBuilder mappingRequest = client().admin().indices().prepareCreate("shapes")
            .addMapping("polygon", mapping, XContentType.JSON);
        mappingRequest.get();
        client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setWaitForGreenStatus().get();

        // Create a multipolygon with two polygons. The first is an rectangle of size 10x10
        // with a hole of size 5x5 equidistant from all sides. This hole in turn contains
        // the second polygon of size 4x4 equidistant from all sites
        MultiPolygonBuilder polygon = new MultiPolygonBuilder()
                .polygon(new PolygonBuilder(
                                new CoordinatesBuilder().coordinate(-10, -10).coordinate(-10, 10).coordinate(10, 10).coordinate(10, -10)
                                .close())
                        .hole(new LineStringBuilder(new CoordinatesBuilder()
                                    .coordinate(-5, -5).coordinate(-5, 5).coordinate(5, 5).coordinate(5, -5).close())))
                .polygon(new PolygonBuilder(
                                new CoordinatesBuilder().coordinate(-4, -4).coordinate(-4, 4).coordinate(4, 4).coordinate(4, -4).close()));
        BytesReference data = BytesReference.bytes(jsonBuilder().startObject().field("area", polygon).endObject());

        client().prepareIndex("shapes", "polygon", "1").setSource(data, XContentType.JSON).get();
        client().admin().indices().prepareRefresh().get();

        // Point in polygon
        SearchResponse result = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new PointBuilder(3, 3)))
                .get();
        assertHitCount(result, 1);
        assertFirstHit(result, hasId("1"));

        // Point in polygon hole
        result = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new PointBuilder(4.5, 4.5)))
                .get();
        assertHitCount(result, 0);

        // by definition the border of a polygon belongs to the inner
        // so the border of a polygons hole also belongs to the inner
        // of the polygon NOT the hole

        // Point on polygon border
        result = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new PointBuilder(10.0, 5.0)))
                .get();
        assertHitCount(result, 1);
        assertFirstHit(result, hasId("1"));

        // Point on hole border
        result = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new PointBuilder(5.0, 2.0)))
                .get();
        assertHitCount(result, 1);
        assertFirstHit(result, hasId("1"));

        if (disjointSupport) {
            // Point not in polygon
            result = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .setPostFilter(QueryBuilders.geoDisjointQuery("area", new PointBuilder(3, 3)))
                    .get();
            assertHitCount(result, 0);

            // Point in polygon hole
            result = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .setPostFilter(QueryBuilders.geoDisjointQuery("area", new PointBuilder(4.5, 4.5)))
                    .get();
            assertHitCount(result, 1);
            assertFirstHit(result, hasId("1"));
        }

        // Create a polygon that fills the empty area of the polygon defined above
        PolygonBuilder inverse = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-5, -5).coordinate(-5, 5).coordinate(5, 5).coordinate(5, -5).close())
                .hole(new LineStringBuilder(
                            new CoordinatesBuilder().coordinate(-4, -4).coordinate(-4, 4).coordinate(4, 4).coordinate(4, -4).close()));

        data = BytesReference.bytes(jsonBuilder().startObject().field("area", inverse).endObject());
        client().prepareIndex("shapes", "polygon", "2").setSource(data, XContentType.JSON).get();
        client().admin().indices().prepareRefresh().get();

        // re-check point on polygon hole
        result = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new PointBuilder(4.5, 4.5)))
                .get();
        assertHitCount(result, 1);
        assertFirstHit(result, hasId("2"));

        // Create Polygon with hole and common edge
        PolygonBuilder builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(-10, -10).coordinate(-10, 10).coordinate(10, 10).coordinate(10, -10).close())
                .hole(new LineStringBuilder(new CoordinatesBuilder()
                    .coordinate(-5, -5).coordinate(-5, 5).coordinate(10, 5).coordinate(10, -5).close()));

        if (withinSupport) {
            // Polygon WithIn Polygon
            builder = new PolygonBuilder(new CoordinatesBuilder()
                    .coordinate(-30, -30).coordinate(-30, 30).coordinate(30, 30).coordinate(30, -30).close());

            result = client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .setPostFilter(QueryBuilders.geoWithinQuery("area", builder.buildGeometry()))
                    .get();
            assertHitCount(result, 2);
        }

        // Create a polygon crossing longitude 180.
        builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(170, -10).coordinate(190, -10).coordinate(190, 10).coordinate(170, 10).close());

        data = BytesReference.bytes(jsonBuilder().startObject().field("area", builder).endObject());
        client().prepareIndex("shapes", "polygon", "1").setSource(data, XContentType.JSON).get();
        client().admin().indices().prepareRefresh().get();

        // Create a polygon crossing longitude 180 with hole.
        builder = new PolygonBuilder(new CoordinatesBuilder()
                .coordinate(170, -10).coordinate(190, -10).coordinate(190, 10).coordinate(170, 10).close())
                    .hole(new LineStringBuilder(new CoordinatesBuilder().coordinate(175, -5).coordinate(185, -5).coordinate(185, 5)
                            .coordinate(175, 5).close()));

        data = BytesReference.bytes(jsonBuilder().startObject().field("area", builder).endObject());
        client().prepareIndex("shapes", "polygon", "1").setSource(data, XContentType.JSON).get();
        client().admin().indices().prepareRefresh().get();

        result = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new PointBuilder(174, -4).buildGeometry()))
                .get();
        assertHitCount(result, 1);

        result = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new PointBuilder(-174, -4).buildGeometry()))
                .get();
        assertHitCount(result, 1);

        result = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new PointBuilder(180, -4).buildGeometry()))
                .get();
        assertHitCount(result, 0);

        result = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setPostFilter(QueryBuilders.geoIntersectionQuery("area", new PointBuilder(180, -6).buildGeometry()))
                .get();
        assertHitCount(result, 1);
    }

    public void testBulk() throws Exception {
        byte[] bulkAction = unZipData("/org/elasticsearch/search/geo/gzippedmap.gz");
        Version version = VersionUtils.randomIndexCompatibleVersion(random());
        Settings settings = Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, version).build();
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder()
                .startObject()
                .startObject("country")
                .startObject("properties")
                .startObject("pin")
                .field("type", "geo_point");
        xContentBuilder.field("store", true)
                .endObject()
                .startObject("location")
                .field("type", "geo_shape")
                .field("ignore_malformed", true)
                .endObject()
                .endObject()
                .endObject()
                .endObject();

        client().admin().indices().prepareCreate("countries").setSettings(settings)
                .addMapping("country", xContentBuilder).get();
        BulkResponse bulk = client().prepareBulk().add(bulkAction, 0, bulkAction.length, null, null, xContentBuilder.contentType()).get();

        for (BulkItemResponse item : bulk.getItems()) {
            assertFalse("unable to index data", item.isFailed());
        }

        client().admin().indices().prepareRefresh().get();
        String key = "DE";

        SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchQuery("_id", key))
                .get();

        assertHitCount(searchResponse, 1);

        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getId(), equalTo(key));
        }

        SearchResponse world = client().prepareSearch().addStoredField("pin").setQuery(
                geoBoundingBoxQuery("pin").setCorners(90, -179.99999, -90, 179.99999)
        ).get();

        assertHitCount(world, 53);

        SearchResponse distance = client().prepareSearch().addStoredField("pin").setQuery(
                geoDistanceQuery("pin").distance("425km").point(51.11, 9.851)
                ).get();

        assertHitCount(distance, 5);
        GeoPoint point = new GeoPoint();
        for (SearchHit hit : distance.getHits()) {
            String name = hit.getId();
            point.resetFromString(hit.getFields().get("pin").getValue());
            double dist = distance(point.getLat(), point.getLon(), 51.11, 9.851);

            assertThat("distance to '" + name + "'", dist, lessThanOrEqualTo(425000d));
            assertThat(name, anyOf(equalTo("CZ"), equalTo("DE"), equalTo("BE"), equalTo("NL"), equalTo("LU")));
            if (key.equals(name)) {
                assertThat(dist, closeTo(0d, 0.1d));
            }
        }
    }

    public void testNeighbors() {
        // Simple root case
        assertThat(addNeighbors("7", new ArrayList<>()), containsInAnyOrder("4", "5", "6", "d", "e", "h", "k", "s"));

        // Root cases (Outer cells)
        assertThat(addNeighbors("0", new ArrayList<>()), containsInAnyOrder("1", "2", "3", "p", "r"));
        assertThat(addNeighbors("b", new ArrayList<>()), containsInAnyOrder("8", "9", "c", "x", "z"));
        assertThat(addNeighbors("p", new ArrayList<>()), containsInAnyOrder("n", "q", "r", "0", "2"));
        assertThat(addNeighbors("z", new ArrayList<>()), containsInAnyOrder("8", "b", "w", "x", "y"));

        // Root crossing dateline
        assertThat(addNeighbors("2", new ArrayList<>()), containsInAnyOrder("0", "1", "3", "8", "9", "p", "r", "x"));
        assertThat(addNeighbors("r", new ArrayList<>()), containsInAnyOrder("0", "2", "8", "n", "p", "q", "w", "x"));

        // level1: simple case
        assertThat(addNeighbors("dk", new ArrayList<>()),
                containsInAnyOrder("d5", "d7", "de", "dh", "dj", "dm", "ds", "dt"));

        // Level1: crossing cells
        assertThat(addNeighbors("d5", new ArrayList<>()),
                containsInAnyOrder("d4", "d6", "d7", "dh", "dk", "9f", "9g", "9u"));
        assertThat(addNeighbors("d0", new ArrayList<>()),
                containsInAnyOrder("d1", "d2", "d3", "9b", "9c", "6p", "6r", "3z"));
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
        if (relation == SpatialOperation.IsDisjointTo) {
            // disjoint works in terms of intersection
            relation = SpatialOperation.Intersects;
        }
        try {
            GeohashPrefixTree tree = new GeohashPrefixTree(SpatialContext.GEO, 3);
            RecursivePrefixTreeStrategy strategy = new RecursivePrefixTreeStrategy(tree, "area");
            Shape shape = SpatialContext.GEO.makePoint(0, 0);
            SpatialArgs args = new SpatialArgs(relation, shape);
            strategy.makeQuery(args);
            return true;
        } catch (UnsupportedSpatialOperation e) {
            final SpatialOperation finalRelation = relation;
            LogManager.getLogger(GeoFilterIT.class)
                .info(() -> new ParameterizedMessage("Unsupported spatial operation {}", finalRelation), e);
            return false;
        }
    }

    protected static String randomhash(int length) {
        return randomhash(random(), length);
    }

    protected static String randomhash(Random random) {
        return randomhash(random, 2 + random.nextInt(10));
    }

    protected static String randomhash() {
        return randomhash(random());
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

