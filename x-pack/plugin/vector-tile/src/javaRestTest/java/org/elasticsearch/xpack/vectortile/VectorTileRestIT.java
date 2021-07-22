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
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;

/**
 * Rest test for _mvt end point. The test only check that the structure of the vector tiles is sound in
 * respect to the number of layers returned and the number of features abd tags in each layer.
 */
public class VectorTileRestIT extends ESRestTestCase {

    private static final String INDEX_POINTS = "index-points";
    private static final String INDEX_SHAPES = "index-shapes";
    private static final String INDEX_ALL = "index*";
    private static final String META_LAYER = "meta";
    private static final String HITS_LAYER = "hits";
    private static final String AGGS_LAYER = "aggs";

    private int x, y, z;

    @Before
    public void indexDocuments() throws IOException {
        z = randomIntBetween(1, GeoTileUtils.MAX_ZOOM - 10);
        x = randomIntBetween(0, (1 << z) - 1);
        y = randomIntBetween(0, (1 << z) - 1);
        indexPoints();
        indexShapes();
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
                final Request putRequest = new Request(HttpPost.METHOD_NAME, INDEX_POINTS + "/_doc");
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
        final Request createRequest = new Request(HttpPut.METHOD_NAME, INDEX_SHAPES);
        Response response = client().performRequest(createRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        final Request mappingRequest = new Request(HttpPut.METHOD_NAME, INDEX_SHAPES + "/_mapping");
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
        final Request putRequest = new Request(HttpPost.METHOD_NAME, INDEX_SHAPES + "/_doc");
        putRequest.setJsonEntity(
            "{\n"
                + "  \"location\": \"BBOX ("
                + r.getMinLon()
                + ", "
                + r.getMaxLon()
                + ","
                + r.getMaxLat()
                + ","
                + r.getMinLat()
                + ")\""
                + ", \"name\": \"rectangle\""
                + ", \"value1\": "
                + 1
                + ", \"value2\": "
                + 2
                + "\n"
                + "}"
        );
        response = client().performRequest(putRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_CREATED));

        final Request flushRequest = new Request(HttpPost.METHOD_NAME, INDEX_SHAPES + "/_refresh");
        response = client().performRequest(flushRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
    }

    @After
    public void deleteData() throws IOException {
        final Request deleteRequest = new Request(HttpDelete.METHOD_NAME, INDEX_POINTS);
        final Response response = client().performRequest(deleteRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
    }

    public void testBasicGet() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\"size\" : 100}");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 4096, 33, 1);
        assertLayer(tile, AGGS_LAYER, 4096, 1, 1);
        assertLayer(tile, META_LAYER, 4096, 1, 14);
    }

    public void testExtent() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\"size\" : 100, \"extent\" : 256}");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 256, 33, 1);
        assertLayer(tile, AGGS_LAYER, 256, 1, 1);
        assertLayer(tile, META_LAYER, 256, 1, 14);
    }

    public void testExtentURL() throws Exception {
        final Request mvtRequest = new Request(
            getHttpMethod(),
            INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y + "?extent=" + 512
        );
        mvtRequest.setJsonEntity("{\"size\" : 100, \"extent\" : 256}");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 512, 33, 1);
        assertLayer(tile, AGGS_LAYER, 512, 1, 1);
        assertLayer(tile, META_LAYER, 512, 1, 14);
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
            // edge case: because all points are the same, the bounding box is a point and cannot be expressed as a polygon.
            // Therefore the feature ends-up without a geometry.
            assertThat(layer.getFeatures(0).hasType(), Matchers.equalTo(false));
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
            assertLayer(tile, HITS_LAYER, 4096, 33, 1);
            assertLayer(tile, AGGS_LAYER, 4096, 1, 1);
            assertLayer(tile, META_LAYER, 4096, 1, 14);
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
            assertLayer(tile, HITS_LAYER, 4096, 33, 1);
            assertLayer(tile, AGGS_LAYER, 4096, 1, 1);
            assertLayer(tile, META_LAYER, 4096, 1, 14);
            assertFeatureType(tile, AGGS_LAYER, VectorTile.Tile.GeomType.POINT);
        }
        {
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
            mvtRequest.setJsonEntity("{\"size\" : 100, \"grid_type\": \"grid\" }");
            final VectorTile.Tile tile = execute(mvtRequest);
            assertThat(tile.getLayersCount(), Matchers.equalTo(3));
            assertLayer(tile, HITS_LAYER, 4096, 33, 1);
            assertLayer(tile, AGGS_LAYER, 4096, 1, 1);
            assertLayer(tile, META_LAYER, 4096, 1, 14);
            assertFeatureType(tile, AGGS_LAYER, VectorTile.Tile.GeomType.POLYGON);
        }
        {
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
            mvtRequest.setJsonEntity("{\"grid_type\": \"invalid_type\" }");
            final ResponseException ex = expectThrows(ResponseException.class, () -> execute(mvtRequest));
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_BAD_REQUEST));
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
        assertLayer(tile, HITS_LAYER, 4096, 33, 1);
        assertLayer(tile, AGGS_LAYER, 4096, 1, 1);
        assertLayer(tile, META_LAYER, 4096, 1, 14);
        assertFeatureType(tile, AGGS_LAYER, VectorTile.Tile.GeomType.POLYGON);
    }

    public void testNoAggLayer() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\"size\" : 100, \"grid_precision\": 0 }");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(2));
        assertLayer(tile, HITS_LAYER, 4096, 33, 1);
        assertLayer(tile, META_LAYER, 4096, 1, 9);
    }

    public void testNoAggLayerURL() throws Exception {
        final Request mvtRequest = new Request(
            getHttpMethod(),
            INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y + "?grid_precision=" + 0
        );
        mvtRequest.setJsonEntity("{\"size\" : 100, \"grid_precision\": 4 }");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(2));
        assertLayer(tile, HITS_LAYER, 4096, 33, 1);
        assertLayer(tile, META_LAYER, 4096, 1, 9);
    }

    public void testNoHitsLayer() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\"size\": 0 }");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(2));
        assertLayer(tile, AGGS_LAYER, 4096, 1, 1);
        assertLayer(tile, META_LAYER, 4096, 1, 13);
    }

    public void testNoHitsLayerURL() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_POINTS + "/_mvt/location/" + z + "/" + x + "/" + y + "?size=" + 0);
        mvtRequest.setJsonEntity("{\"size\": 100 }");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(2));
        assertLayer(tile, AGGS_LAYER, 4096, 1, 1);
        assertLayer(tile, META_LAYER, 4096, 1, 13);
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
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_ALL + "/_mvt/location/" + z + "/" + x + "/" + y);
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
            assertLayer(tile, HITS_LAYER, 4096, 34, 1);
            final VectorTile.Tile.Layer layer = getLayer(tile, HITS_LAYER);
            assertThat(layer.getFeatures(0).getType(), Matchers.equalTo(VectorTile.Tile.GeomType.POLYGON));
            assertLayer(tile, META_LAYER, 4096, 1, 8);
        }
        {
            // asc order, polygon should be the last hit
            final Request mvtRequest = new Request(getHttpMethod(), INDEX_ALL + "/_mvt/location/" + z + "/" + x + "/" + y);
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
            assertLayer(tile, HITS_LAYER, 4096, 34, 1);
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
        assertLayer(tile, HITS_LAYER, 4096, 1, 1);
        assertLayer(tile, AGGS_LAYER, 4096, 1, 1);
        assertLayer(tile, META_LAYER, 4096, 1, 14);
    }

    public void testBasicShape() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_SHAPES + "/_mvt/location/" + z + "/" + x + "/" + y);
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 4096, 1, 1);
        assertLayer(tile, AGGS_LAYER, 4096, 256 * 256, 1);
        assertLayer(tile, META_LAYER, 4096, 1, 14);
    }

    public void testWithFields() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_SHAPES + "/_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\"fields\": [\"name\", \"value1\"] }");
        final VectorTile.Tile tile = execute(mvtRequest);
        assertThat(tile.getLayersCount(), Matchers.equalTo(3));
        assertLayer(tile, HITS_LAYER, 4096, 1, 3);
        assertLayer(tile, AGGS_LAYER, 4096, 256 * 256, 1);
        assertLayer(tile, META_LAYER, 4096, 1, 14);
    }

    public void testMinAgg() throws Exception {
        final Request mvtRequest = new Request(getHttpMethod(), INDEX_SHAPES + "/_mvt/location/" + z + "/" + x + "/" + y);
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
        assertLayer(tile, HITS_LAYER, 4096, 1, 1);
        assertLayer(tile, AGGS_LAYER, 4096, 256 * 256, 2);
        assertLayer(tile, META_LAYER, 4096, 1, 19);
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
