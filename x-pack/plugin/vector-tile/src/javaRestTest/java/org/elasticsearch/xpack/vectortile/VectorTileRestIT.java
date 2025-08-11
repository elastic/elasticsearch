/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile;

import com.wdtinc.mapbox_vector_tile.VectorTile;

import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;

/**
 * Rest test for _mvt end point. The test only check that the structure of the vector tiles is sound in
 * respect to the number of layers returned and the number of features abd tags in each layer.
 */
public class VectorTileRestIT extends ESRestTestCase {

    private static final String INDEX_POINTS = "index-points";
    private static final String INDEX_POLYGON = "index-polygon";
    private static final String INDEX_COLLECTION = "index-collection";
    private static final String INDEX_POINTS_SHAPES = INDEX_POINTS + "," + INDEX_POLYGON;
    private static final String INDEX_ALL = "index*";
    private static final String META_LAYER = "meta";
    private static final String HITS_LAYER = "hits";
    private static final String AGGS_LAYER = "aggs";

    private static boolean oneTimeSetup = false;
    private static int x, y, z;

    @Before
    public void indexDocuments() throws IOException {
        if (oneTimeSetup == false) {
            z = randomIntBetween(1, GeoTileUtils.MAX_ZOOM - 10);
            x = randomIntBetween(0, (1 << z) - 1);
            y = randomIntBetween(0, (1 << z) - 1);
            indexPoints();
            indexShapes();
            indexCollection();
            oneTimeSetup = true;
        }
    }

    private void indexPoints() throws IOException {
        final Request createRequest = new Request(HttpPut.METHOD_NAME, INDEX_POINTS);
        Response response = client().performRequest(createRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        final Request mappingRequest = new Request(HttpPut.METHOD_NAME, INDEX_POINTS + "/_mapping");
        mappingRequest.setJsonEntity(
            "{\n"
                + "  \"properties\": {\n"
                + "    \"location\": {\n"
                + "      \"type\": \"geo_point\"\n"
                + "    },\n"
                + "    \"name\": {\n"
                + "      \"type\": \"keyword\"\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        response = client().performRequest(mappingRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        final Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
        double x = (r.getMaxX() + r.getMinX()) / 2;
        double y = (r.getMaxY() + r.getMinY()) / 2;
        for (int i = 0; i < 30; i += 10) {
            for (int j = 0; j <= i; j++) {
                final Request putRequest = new Request(HttpPost.METHOD_NAME, INDEX_POINTS + "/_doc/");
                putRequest.setJsonEntity(
                    "{\n"
                        + "  \"location\": \"POINT("
                        + x
                        + " "
                        + y
                        + ")\", \"name\": \"point"
                        + i
                        + "\""
                        + ", \"value1\": "
                        + i
                        + ", \"value2\": "
                        + (i + 1)
                        + "\n"
                        + "}"
                );
                response = client().performRequest(putRequest);
                assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_CREATED));
            }
        }

        final Request flushRequest = new Request(HttpPost.METHOD_NAME, INDEX_POINTS + "/_refresh");
        response = client().performRequest(flushRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
    }

    private void indexShapes() throws IOException {
        final Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
        createIndexAndPutGeometry(INDEX_POLYGON, toPolygon(r), "polygon");
    }

    private void createIndexAndPutGeometry(String indexName, Geometry geometry, String id) throws IOException {
        final Request createRequest = new Request(HttpPut.METHOD_NAME, indexName);
        Response response = client().performRequest(createRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        final Request mappingRequest = new Request(HttpPut.METHOD_NAME, indexName + "/_mapping");
        mappingRequest.setJsonEntity(
            "{\n"
                + "  \"properties\": {\n"
                + "    \"location\": {\n"
                + "      \"type\": \"geo_shape\"\n"
                + "    },\n"
                + "    \"name\": {\n"
                + "      \"type\": \"keyword\"\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        response = client().performRequest(mappingRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));

        final Request putRequest = new Request(HttpPost.METHOD_NAME, indexName + "/_doc/" + id);
        putRequest.setJsonEntity(
            "{\n"
                + "  \"location\": \""
                + WellKnownText.toWKT(geometry)
                + "\""
                + ", \"name\": \"geometry\""
                + ", \"value1\": "
                + 1
                + ", \"value2\": "
                + 2
                + "\n"
                + "}"
        );
        response = client().performRequest(putRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_CREATED));

        final Request flushRequest = new Request(HttpPost.METHOD_NAME, indexName + "/_refresh");
        response = client().performRequest(flushRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
    }

    private Polygon toPolygon(Rectangle r) {
        return new Polygon(
            new LinearRing(
                new double[] { r.getMinX(), r.getMaxX(), r.getMaxX(), r.getMinX(), r.getMinX() },
                new double[] { r.getMinY(), r.getMinY(), r.getMaxY(), r.getMaxY(), r.getMinY() }
            )
        );
    }

    private void indexCollection() throws IOException {
        final Request createRequest = new Request(HttpPut.METHOD_NAME, INDEX_COLLECTION);
        Response response = client().performRequest(createRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        final Request mappingRequest = new Request(HttpPut.METHOD_NAME, INDEX_COLLECTION + "/_mapping");
        mappingRequest.setJsonEntity(
            "{\n"
                + "  \"properties\": {\n"
                + "    \"location\": {\n"
                + "      \"type\": \"geo_shape\"\n"
                + "    },\n"
                + "    \"name\": {\n"
                + "      \"type\": \"keyword\"\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        response = client().performRequest(mappingRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));

        final Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
        double x = (r.getMaxX() + r.getMinX()) / 2;
        double y = (r.getMaxY() + r.getMinY()) / 2;
        final Request putRequest = new Request(HttpPost.METHOD_NAME, INDEX_COLLECTION + "/_doc");
        String collection = "GEOMETRYCOLLECTION (BBOX ("
            + r.getMinLon()
            + ", "
            + r.getMaxLon()
            + ","
            + r.getMaxLat()
            + ","
            + r.getMinLat()
            + "), POINT("
            + x
            + " "
            + y
            + "))";
        putRequest.setJsonEntity(
            "{\n"
                + "  \"location\": \""
                + collection
                + "\""
                + ", \"name\": \"collection\""
                + ", \"value1\": "
                + 1
                + ", \"value2\": "
                + 2
                + "\n"
                + "}"
        );
        response = client().performRequest(putRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_CREATED));

        final Request flushRequest = new Request(HttpPost.METHOD_NAME, INDEX_COLLECTION + "/_refresh");
        response = client().performRequest(flushRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
    }

    @AfterClass
    public static void deleteData() throws IOException {
        try {
            wipeAllIndices();
        } finally {
            // Clear the setup state
            oneTimeSetup = false;
        }
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    public void testBasicGet() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\"size\" : 100}");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 4096, 33, 2);
        assertLayer(tile, AGGS_LAYER, 4096, 1, 2);
        assertLayer(tile, META_LAYER, 4096, 1, 13);
    }

    public void testIndexAllGet() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_ALL + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\"size\" : 100}");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        // 33 points, 1 polygon and two from geometry collection
        assertLayer(tile, HITS_LAYER, 4096, 36, 2);
        assertLayer(tile, AGGS_LAYER, 4096, 256 * 256, 2);
        assertLayer(tile, META_LAYER, 4096, 1, 13);
    }

    public void testExtent() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\"size\" : 100, \"extent\" : 256}");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 256, 33, 2);
        assertLayer(tile, AGGS_LAYER, 256, 1, 2);
        assertLayer(tile, META_LAYER, 256, 1, 13);
    }

    public void testExtentURL() throws Exception {
        final Request mvtRequest = new Request(
            getHttpMethod(),
            INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y + "?extent=" + 512
        );
        mvtRequest.setJsonEntity("{\"size\" : 100, \"extent\" : 256}");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 512, 33, 2);
        assertLayer(tile, AGGS_LAYER, 512, 1, 2);
        assertLayer(tile, META_LAYER, 512, 1, 13);
    }

    public void testExactBounds() throws Exception {
        {
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
            mvtRequest.setJsonEntity("{\"size\" : 0, \"grid_precision\" : 0}");
            final VectorTile.Tile tile = execute(mvtRequest);
            assertThat(tile.getLayersCount(), Matchers.equalTo(1));
            assertLayer(tile, META_LAYER, 4096, 1, 8);
            final VectorTile.Tile.Layer layer = getLayer(tile, META_LAYER);
            assertThat(layer.getFeatures(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));

        }
        {
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
            mvtRequest.setJsonEntity("{\"size\" : 0, \"grid_precision\" : 0, \"exact_bounds\" : true}");
            final VectorTile.Tile tile = execute(mvtRequest);
            assertThat(tile.getLayersCount(), Matchers.equalTo(1));
            assertLayer(tile, META_LAYER, 4096, 1, 8);
            final VectorTile.Tile.Layer layer = getLayer(tile, META_LAYER);
            // edge case: because all points are the same, the bounding box is a point
            assertThat(layer.getFeatures(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POINT));
        }
        {
            final Request mvtRequest = new Request(
                getHttpMethod(),
                INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y + "?exact_bounds=false"
            );
            mvtRequest.setJsonEntity("{\"size\" : 0, \"grid_precision\" : 0, \"exact_bounds\" : true}");
            final VectorTile.Tile tile = execute(mvtRequest);
            assertThat(tile.getLayersCount(), Matchers.equalTo(1));
            assertLayer(tile, META_LAYER, 4096, 1, 8);
            final VectorTile.Tile.Layer layer = getLayer(tile, META_LAYER);
            assertThat(layer.getFeatures(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));

        }
    }

    public void testEmpty() throws Exception {
        final int newY = (1 << z) - 1 == y ? y - 1 : y + 1;
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + newY);
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(1));
        assertLayer(tile, META_LAYER, 4096, 1, 10);
    }

    public void testGridPrecision() throws Exception {
        {
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
            mvtRequest.setJsonEntity("{\"size\" : 100, \"grid_precision\": 7 }");
            final VectorTile.Tile tile = execute(mvtRequest);
            assertThat(tile.getLayersCount(), Matchers.equalTo(3));
            assertLayer(tile, HITS_LAYER, 4096, 33, 2);
            assertLayer(tile, AGGS_LAYER, 4096, 1, 2);
            assertLayer(tile, META_LAYER, 4096, 1, 13);
        }
        {
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
            mvtRequest.setJsonEntity("{\"grid_precision\": 9 }");
            final ResponseException ex = expectThrows(ResponseException.class, () -> execute(mvtRequest));
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_BAD_REQUEST));
        }
    }

    public void testGridType() throws Exception {
        {
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
            mvtRequest.setJsonEntity("{\"size\" : 100, \"grid_type\": \"point\" }");
            final VectorTile.Tile tile = execute(mvtRequest);
            assertThat(tile.getLayersCount(), Matchers.equalTo(3));
            assertLayer(tile, HITS_LAYER, 4096, 33, 2);
            assertLayer(tile, AGGS_LAYER, 4096, 1, 2);
            assertLayer(tile, META_LAYER, 4096, 1, 13);
            assertFeatureType(tile, AGGS_LAYER, VectorTile.Tile.GeomType.POINT);
        }
        {
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
            mvtRequest.setJsonEntity("{\"size\" : 100, \"grid_type\": \"grid\" }");
            final VectorTile.Tile tile = execute(mvtRequest);
            assertThat(tile.getLayersCount(), Matchers.equalTo(3));
            assertLayer(tile, HITS_LAYER, 4096, 33, 2);
            assertLayer(tile, AGGS_LAYER, 4096, 1, 2);
            assertLayer(tile, META_LAYER, 4096, 1, 13);
            assertFeatureType(tile, AGGS_LAYER, VectorTile.Tile.GeomType.POLYGON);
        }
        {
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
            mvtRequest.setJsonEntity("{\"size\" : 100, \"grid_type\": \"centroid\" }");
            final VectorTile.Tile tile = execute(mvtRequest);
            assertThat(tile.getLayersCount(), Matchers.equalTo(3));
            assertLayer(tile, HITS_LAYER, 4096, 33, 2);
            assertLayer(tile, AGGS_LAYER, 4096, 1, 2);
            assertLayer(tile, META_LAYER, 4096, 1, 13);
            assertFeatureType(tile, AGGS_LAYER, VectorTile.Tile.GeomType.POINT);
        }
        {
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
            mvtRequest.setJsonEntity("{\"grid_type\": \"invalid_type\" }");
            final ResponseException ex = expectThrows(ResponseException.class, () -> execute(mvtRequest));
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_BAD_REQUEST));
        }
    }

    public void testInvalidAggName() {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity(
            "{\"size\" : 0,"
                + "  \"aggs\": {\n"
                + "    \"_mvt_name\": {\n"
                + "      \"min\": {\n"
                + "         \"field\": \"value1\"\n"
                + "        }\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        ResponseException ex = expectThrows(ResponseException.class, () -> execute(mvtRequest));
        // the prefix '_mvt_' is reserved for internal aggregations
        assertThat(ex.getMessage(), Matchers.containsString("Invalid aggregation name [_mvt_name]"));
    }

    public void testCentroidGridTypeOnPolygon() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POLYGON + "/_mvt/location/" + (z + 2) + "/" + 4 * x + "/" + 4 * y);
        mvtRequest.setJsonEntity("{\"size\" : 0, \"grid_type\": \"centroid\",  \"grid_precision\": 2}");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(2));
        assertLayer(tile, AGGS_LAYER, 4096, 4 * 4, 2);
        assertLayer(tile, META_LAYER, 4096, 1, 13);
        assertFeatureType(tile, AGGS_LAYER, VectorTile.Tile.GeomType.POINT);
    }

    public void testTrackTotalHitsAsBoolean() throws Exception {
        {
            final Request mvtRequest = new Request(
                getHttpMethod(),
                INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y + "?track_total_hits=true"
            );
            final VectorTile.Tile tile = execute(mvtRequest);
            assertThat(tile.getLayersCount(), Matchers.equalTo(3));
            assertLayer(tile, HITS_LAYER, 4096, 33, 2);
            assertLayer(tile, AGGS_LAYER, 4096, 1, 2);
            assertLayer(tile, META_LAYER, 4096, 1, 13);
            assertStringTag(getLayer(tile, META_LAYER), getLayer(tile, META_LAYER).getFeatures(0), "hits.total.relation", "eq");
            assertSintTag(getLayer(tile, META_LAYER), getLayer(tile, META_LAYER).getFeatures(0), "hits.total.value", 33);
        }
        {
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
            mvtRequest.setJsonEntity("{\"track_total_hits\": false }");
            final VectorTile.Tile tile = execute(mvtRequest);
            assertThat(tile.getLayersCount(), Matchers.equalTo(3));
            assertLayer(tile, HITS_LAYER, 4096, 33, 2);
            assertLayer(tile, AGGS_LAYER, 4096, 1, 2);
            assertLayer(tile, META_LAYER, 4096, 1, 11);
        }
    }

    public void testTrackTotalHitsAsInt() throws Exception {
        {
            final Request mvtRequest = new Request(
                getHttpMethod(),
                INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y + "?track_total_hits=100"
            );
            final VectorTile.Tile tile = execute(mvtRequest);
            assertThat(tile.getLayersCount(), Matchers.equalTo(3));
            assertLayer(tile, HITS_LAYER, 4096, 33, 2);
            assertLayer(tile, AGGS_LAYER, 4096, 1, 2);
            assertLayer(tile, META_LAYER, 4096, 1, 13);
            assertStringTag(getLayer(tile, META_LAYER), getLayer(tile, META_LAYER).getFeatures(0), "hits.total.relation", "eq");
            assertSintTag(getLayer(tile, META_LAYER), getLayer(tile, META_LAYER).getFeatures(0), "hits.total.value", 33);
        }
        {
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
            mvtRequest.setJsonEntity("{\"track_total_hits\": 1 }");
            final VectorTile.Tile tile = execute(mvtRequest);
            assertThat(tile.getLayersCount(), Matchers.equalTo(3));
            assertLayer(tile, HITS_LAYER, 4096, 33, 2);
            assertLayer(tile, AGGS_LAYER, 4096, 1, 2);
            assertLayer(tile, META_LAYER, 4096, 1, 13);
            assertStringTag(getLayer(tile, META_LAYER), getLayer(tile, META_LAYER).getFeatures(0), "hits.total.relation", "gte");
            assertSintTag(getLayer(tile, META_LAYER), getLayer(tile, META_LAYER).getFeatures(0), "hits.total.value", 1);
        }
    }

    public void testGridTypeURL() throws Exception {
        final Request mvtRequest = new Request(
            getHttpMethod(),
            INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y + "?grid_type=grid"
        );
        mvtRequest.setJsonEntity("{\"size\" : 100, \"grid_type\": \"point\" }");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 4096, 33, 2);
        assertLayer(tile, AGGS_LAYER, 4096, 1, 2);
        assertLayer(tile, META_LAYER, 4096, 1, 13);
        assertFeatureType(tile, AGGS_LAYER, VectorTile.Tile.GeomType.POLYGON);
    }

    public void testNoAggLayer() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\"size\" : 100, \"grid_precision\": 0 }");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(2));
        assertLayer(tile, HITS_LAYER, 4096, 33, 2);
        assertLayer(tile, META_LAYER, 4096, 1, 8);
    }

    public void testNoAggLayerURL() throws Exception {
        final Request mvtRequest = new Request(
            getHttpMethod(),
            INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y + "?grid_precision=" + 0
        );
        mvtRequest.setJsonEntity("{\"size\" : 100, \"grid_precision\": 4 }");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(2));
        assertLayer(tile, HITS_LAYER, 4096, 33, 2);
        assertLayer(tile, META_LAYER, 4096, 1, 8);
    }

    public void testNoHitsLayer() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\"size\": 0 }");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(2));
        assertLayer(tile, AGGS_LAYER, 4096, 1, 2);
        assertLayer(tile, META_LAYER, 4096, 1, 13);
    }

    public void testNoHitsLayerURL() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y + "?size=" + 0);
        mvtRequest.setJsonEntity("{\"size\": 100 }");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(2));
        assertLayer(tile, AGGS_LAYER, 4096, 1, 2);
        assertLayer(tile, META_LAYER, 4096, 1, 13);
    }

    public void testDefaultSort() throws Exception {
        {
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS_SHAPES + "/_mvt/location/" + z + "/" + x + "/" + y);
            mvtRequest.setJsonEntity("{\"size\": 100 }");
            final VectorTile.Tile tile = execute(mvtRequest);
            assertThat(tile.getLayersCount(), Matchers.equalTo(3));
            assertLayer(tile, HITS_LAYER, 4096, 34, 2);
            final VectorTile.Tile.Layer layer = getLayer(tile, HITS_LAYER);
            assertThat(layer.getFeatures(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
            assertLayer(tile, AGGS_LAYER, 4096, 256 * 256, 2);
            assertLayer(tile, META_LAYER, 4096, 1, 13);
        }
        {
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS_SHAPES + "/_mvt/location/" + z + "/" + x + "/" + y);
            mvtRequest.setJsonEntity("{\"size\": 100, \"sort\" : []}"); // override default sort
            final VectorTile.Tile tile = execute(mvtRequest);
            assertThat(tile.getLayersCount(), Matchers.equalTo(3));
            assertLayer(tile, HITS_LAYER, 4096, 34, 2);
            final VectorTile.Tile.Layer layer = getLayer(tile, HITS_LAYER);
            assertThat(layer.getFeatures(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POINT));
            assertLayer(tile, AGGS_LAYER, 4096, 256 * 256, 2);
            assertLayer(tile, META_LAYER, 4096, 1, 14);
        }
    }

    public void testRuntimeFieldWithSort() throws Exception {
        String runtimeMapping = "\"runtime_mappings\": {\n"
            + "  \"width\": {\n"
            + "    \"script\": "
            + "\"emit(doc['location'].getBoundingBox().bottomRight().getLon() - doc['location'].getBoundingBox().topLeft().getLon())\",\n"
            + "    \"type\": \"double\"\n"
            + "  }\n"
            + "}\n";
        {
            // desc order, polygon should be the first hit
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS_SHAPES + "/_mvt/location/" + z + "/" + x + "/" + y);
            mvtRequest.setJsonEntity(
                "{\n"
                    + "  \"size\" : 100,\n"
                    + "  \"grid_precision\" : 0,\n"
                    + runtimeMapping
                    + ","
                    + "  \"sort\" : [\n"
                    + "    {\n"
                    + "      \"width\": {\n"
                    + "        \"order\": \"desc\"\n"
                    + "      }\n"
                    + "    }\n"
                    + "  ]"
                    + "}"
            );

            final VectorTile.Tile tile = execute(mvtRequest);
            assertThat(tile.getLayersCount(), Matchers.equalTo(2));
            assertLayer(tile, HITS_LAYER, 4096, 34, 2);
            final VectorTile.Tile.Layer layer = getLayer(tile, HITS_LAYER);
            assertThat(layer.getFeatures(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
            assertLayer(tile, META_LAYER, 4096, 1, 8);
        }
        {
            // asc order, polygon should be the last hit
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS_SHAPES + "/_mvt/location/" + z + "/" + x + "/" + y);
            mvtRequest.setJsonEntity(
                "{\n"
                    + "  \"size\" : 100,\n"
                    + "  \"grid_precision\" : 0,\n"
                    + runtimeMapping
                    + ","
                    + "  \"sort\" : [\n"
                    + "    {\n"
                    + "      \"width\": {\n"
                    + "        \"order\": \"asc\"\n"
                    + "      }\n"
                    + "    }\n"
                    + "  ]"
                    + "}"
            );

            final VectorTile.Tile tile = execute(mvtRequest);
            assertThat(tile.getLayersCount(), Matchers.equalTo(2));
            assertLayer(tile, HITS_LAYER, 4096, 34, 2);
            final VectorTile.Tile.Layer layer = getLayer(tile, HITS_LAYER);
            assertThat(layer.getFeatures(33).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
            assertLayer(tile, META_LAYER, 4096, 1, 8);
        }
    }

    public void testBasicQueryGet() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity(
            "{\n"
                + "  \"query\": {\n"
                + "    \"term\": {\n"
                + "      \"name\": {\n"
                + "         \"value\": \"point0\"\n"
                + "        }\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 4096, 1, 2);
        assertLayer(tile, AGGS_LAYER, 4096, 1, 2);
        assertLayer(tile, META_LAYER, 4096, 1, 13);
        assertStringTag(getLayer(tile, HITS_LAYER), getLayer(tile, HITS_LAYER).getFeatures(0), "_index", INDEX_POINTS);
    }

    public void testBasicShape() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POLYGON + "/_mvt/location/" + z + "/" + x + "/" + y);
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 4096, 1, 2);
        assertLayer(tile, AGGS_LAYER, 4096, 256 * 256, 2);
        assertLayer(tile, META_LAYER, 4096, 1, 13);
        assertStringTag(getLayer(tile, HITS_LAYER), getLayer(tile, HITS_LAYER).getFeatures(0), "_index", INDEX_POLYGON);
        assertStringTag(getLayer(tile, HITS_LAYER), getLayer(tile, HITS_LAYER).getFeatures(0), "_id", "polygon");
        // check we add right values to the aggs layer
        final VectorTile.Tile.Layer aggsLayer = getLayer(tile, AGGS_LAYER);
        for (int i = 0; i < 256 * 256; i++) {
            final VectorTile.Tile.Feature feature = aggsLayer.getFeatures(i);
            assertSintTag(aggsLayer, feature, "_count", 1);
            assertBucketKeyTag(aggsLayer, feature);
        }
    }

    public void testWithFields() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POLYGON + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\"fields\": [\"name\", \"value1\"] }");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 4096, 1, 4);
        assertLayer(tile, AGGS_LAYER, 4096, 256 * 256, 2);
        assertLayer(tile, META_LAYER, 4096, 1, 13);
    }

    public void testWithNoExistingFields() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POLYGON + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\"fields\": [\"otherField\"] }");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 4096, 1, 2);
        assertLayer(tile, AGGS_LAYER, 4096, 256 * 256, 2);
        assertLayer(tile, META_LAYER, 4096, 1, 13);
    }

    public void testWithNullFields() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POLYGON + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\"fields\": [\"nullField\"] }");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 4096, 1, 2);
        assertLayer(tile, AGGS_LAYER, 4096, 256 * 256, 2);
        assertLayer(tile, META_LAYER, 4096, 1, 13);
    }

    public void testWithIgnoreMalformedValueFields() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POLYGON + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\"fields\": [ \"ignore_value\"] }");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 4096, 1, 2);
        assertLayer(tile, AGGS_LAYER, 4096, 256 * 256, 2);
        assertLayer(tile, META_LAYER, 4096, 1, 13);
    }

    public void testWithFieldsWildCard() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POLYGON + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\"fields\": [\"*\"] }");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 4096, 1, 5);
        assertLayer(tile, AGGS_LAYER, 4096, 256 * 256, 2);
        assertLayer(tile, META_LAYER, 4096, 1, 13);
    }

    public void testSingleValueAgg() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POLYGON + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity(
            "{\n"
                + "  \"aggs\": {\n"
                + "    \"minVal\": {\n"
                + "      \"min\": {\n"
                + "         \"field\": \"value1\"\n"
                + "        }\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 4096, 1, 2);
        assertLayer(tile, AGGS_LAYER, 4096, 256 * 256, 3);
        assertLayer(tile, META_LAYER, 4096, 1, 18);
        // check pipeline aggregation values
        final VectorTile.Tile.Layer metaLayer = getLayer(tile, META_LAYER);
        assertDoubleTag(metaLayer, metaLayer.getFeatures(0), "aggregations.minVal.min", 1.0);
        assertDoubleTag(metaLayer, metaLayer.getFeatures(0), "aggregations.minVal.max", 1.0);
    }

    public void testMultiValueAgg() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POLYGON + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity(
            "{\n"
                + "  \"aggs\": {\n"
                + "    \"percentilesAgg\": {\n"
                + "      \"percentiles\": {\n"
                + "         \"field\": \"value1\",\n"
                + "         \"percents\": [95, 99, 99.9]\n"
                + "        }\n"
                + "    }\n"
                + "  }\n"
                + "}"
        );
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 4096, 1, 2);
        assertLayer(tile, AGGS_LAYER, 4096, 256 * 256, 5);
        assertLayer(tile, META_LAYER, 4096, 1, 28);
        // check pipeline aggregation values
        final VectorTile.Tile.Layer metaLayer = getLayer(tile, META_LAYER);
        assertDoubleTag(metaLayer, metaLayer.getFeatures(0), "aggregations.percentilesAgg.95.0.min", 1.0);
        assertDoubleTag(metaLayer, metaLayer.getFeatures(0), "aggregations.percentilesAgg.95.0.max", 1.0);
        assertDoubleTag(metaLayer, metaLayer.getFeatures(0), "aggregations.percentilesAgg.99.0.min", 1.0);
        assertDoubleTag(metaLayer, metaLayer.getFeatures(0), "aggregations.percentilesAgg.99.0.max", 1.0);
        assertDoubleTag(metaLayer, metaLayer.getFeatures(0), "aggregations.percentilesAgg.99.9.min", 1.0);
        assertDoubleTag(metaLayer, metaLayer.getFeatures(0), "aggregations.percentilesAgg.99.9.max", 1.0);
    }

    public void testOverlappingMultipolygon() throws Exception {
        // Overlapping multipolygon are accepted by Elasticsearch but is invalid for JTS. This
        // causes and error in the mvt library that gets logged using slf4j
        final String index = "overlapping_multipolygon";
        final Rectangle r1 = new Rectangle(-160, 160, 80, -80);
        final Rectangle r2 = new Rectangle(-159, 161, 79, -81);
        createIndexAndPutGeometry(index, new MultiPolygon(org.elasticsearch.core.List.of(toPolygon(r1), toPolygon(r2))), "multi_polygon");
        final Request mvtRequest = new Request(getHttpMethod(), index + "/_mvt/location/0/0/0?grid_precision=0");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(2));
        assertLayer(tile, HITS_LAYER, 4096, 0, 0);
        assertLayer(tile, META_LAYER, 4096, 1, 8);
        final Response response = client().performRequest(new Request(HttpDelete.METHOD_NAME, index));
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
    }

    private String getHttpMethod() {
        return random().nextBoolean() ? HttpGet.METHOD_NAME : HttpPost.METHOD_NAME;
    }

    private void assertFeatureType(VectorTile.Tile tile, String name, VectorTile.Tile.GeomType type) {
        final VectorTile.Tile.Layer layer = getLayer(tile, name);
        for (int i = 0; i < layer.getFeaturesCount(); i++) {
            final VectorTile.Tile.Feature feature = layer.getFeatures(i);
            assertThat(feature.getType(), Matchers.equalTo(type));
        }
    }

    private void assertLayer(VectorTile.Tile tile, String name, int extent, int numFeatures, int numTags) {
        final VectorTile.Tile.Layer layer = getLayer(tile, name);
        assertThat(layer.getExtent(), Matchers.equalTo(extent));
        assertThat(layer.getFeaturesCount(), Matchers.equalTo(numFeatures));
        assertThat(layer.getKeysCount(), Matchers.equalTo(numTags));
    }

    private void assertSintTag(VectorTile.Tile.Layer layer, VectorTile.Tile.Feature feature, String tag, long value) {
        for (int i = 0; i < feature.getTagsCount(); i += 2) {
            String thisTag = layer.getKeys(feature.getTags(i));
            if (tag.equals(thisTag)) {
                VectorTile.Tile.Value thisValue = layer.getValues(feature.getTags(i + 1));
                assertThat(value, Matchers.equalTo(thisValue.getSintValue()));
                return;
            }
        }
        fail("Could not find tag [" + tag + "]");
    }

    private void assertDoubleTag(VectorTile.Tile.Layer layer, VectorTile.Tile.Feature feature, String tag, double value) {
        for (int i = 0; i < feature.getTagsCount(); i += 2) {
            String thisTag = layer.getKeys(feature.getTags(i));
            if (tag.equals(thisTag)) {
                VectorTile.Tile.Value thisValue = layer.getValues(feature.getTags(i + 1));
                assertThat(value, Matchers.equalTo(thisValue.getDoubleValue()));
                return;
            }
        }
        fail("Could not find tag [" + tag + "]");
    }

    private void assertStringTag(VectorTile.Tile.Layer layer, VectorTile.Tile.Feature feature, String tag, String value) {
        for (int i = 0; i < feature.getTagsCount(); i += 2) {
            String thisTag = layer.getKeys(feature.getTags(i));
            if (tag.equals(thisTag)) {
                VectorTile.Tile.Value thisValue = layer.getValues(feature.getTags(i + 1));
                assertEquals(thisValue.getStringValue(), value);
                return;
            }
        }
        fail("Could not find tag [" + tag + "]");
    }

    private void assertBucketKeyTag(VectorTile.Tile.Layer layer, VectorTile.Tile.Feature feature) {
        for (int i = 0; i < feature.getTagsCount(); i += 2) {
            String thisTag = layer.getKeys(feature.getTags(i));
            if ("_key".equals(thisTag)) {
                VectorTile.Tile.Value thisValue = layer.getValues(feature.getTags(i + 1));
                // just make sure it can be parsed
                GeoTileUtils.longEncode(thisValue.getStringValue());
                return;
            }
        }
        fail("Could not find tag [|_key]");
    }

    private VectorTile.Tile execute(Request mvtRequest) throws IOException {
        final Response response = client().performRequest(mvtRequest);
        final InputStream inputStream = response.getEntity().getContent();
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        return VectorTile.Tile.parseFrom(inputStream);
    }

    private VectorTile.Tile.Layer getLayer(VectorTile.Tile tile, String layerName) {
        for (int i = 0; i < tile.getLayersCount(); i++) {
            final VectorTile.Tile.Layer layer = tile.getLayers(i);
            if (layerName.equals(layer.getName())) {
                return layer;
            }
        }
        fail("Could not find layer " + layerName);
        return null;
    }
}
