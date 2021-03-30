/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;

public class VectorTileGridRestIT extends ESRestTestCase {

    private static String INDEX_POINTS = "index-points";
    private static String INDEX_SHAPES = "index-shapes";

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
        mappingRequest.setJsonEntity("{\n" +
            "  \"properties\": {\n" +
            "    \"location\": {\n" +
            "      \"type\": \"geo_point\"\n" +
            "    },\n" +
            "    \"name\": {\n" +
            "      \"type\": \"keyword\"\n" +
            "    }\n" +
            "  }\n" +
            "}");
        response = client().performRequest(mappingRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);
        double x = (r.getMaxX() + r.getMinX()) / 2;
        double y = (r.getMaxY() + r.getMinY()) / 2;
        for (int i = 0; i < 30; i+=10) {
            for (int j = 0; j <= i; j++) {
                final Request putRequest = new Request(HttpPost.METHOD_NAME, INDEX_POINTS + "/_doc");
                putRequest.setJsonEntity("{\n" +
                    "  \"location\": \"POINT(" + x + " " + y + ")\", \"name\": \"point" + i + "\"" +
                    ", \"value1\": " + i + ", \"value2\": " + (i + 1) + "\n" +
                    "}");
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
        mappingRequest.setJsonEntity("{\n" +
            "  \"properties\": {\n" +
            "    \"location\": {\n" +
            "      \"type\": \"geo_shape\"\n" +
            "    },\n" +
            "    \"name\": {\n" +
            "      \"type\": \"keyword\"\n" +
            "    }\n" +
            "  }\n" +
            "}");
        response = client().performRequest(mappingRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));

        Rectangle r = GeoTileUtils.toBoundingBox(x, y, z);

        final Request putRequest = new Request(HttpPost.METHOD_NAME, INDEX_SHAPES + "/_doc");
        putRequest.setJsonEntity("{\n" +
            "  \"location\": \"BBOX (" + r.getMinLon() + ", " + r.getMaxLon() + "," + r.getMaxLat() + "," + r.getMinLat() + ")\"" +
            ", \"name\": \"rectangle\"" +
            ", \"value1\": " + 1 + ", \"value2\": " + 2 + "\n" +
            "}");
        response = client().performRequest(putRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_CREATED));


        final Request flushRequest = new Request(HttpPost.METHOD_NAME, INDEX_SHAPES + "/_refresh");
        response = client().performRequest(flushRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
    }

    @After
    public void deleteData() throws IOException {
        final Request deleteRequest = new Request(HttpDelete.METHOD_NAME, INDEX_POINTS);
        Response response = client().performRequest(deleteRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
    }

    public void testBasicGet() throws Exception {
        final Request mvtRequest = new Request(HttpGet.METHOD_NAME, INDEX_POINTS + "/_agg_mvt/location/" + z + "/" + x + "/" + y);
        Response response = client().performRequest(mvtRequest);
        InputStream inputStream = response.getEntity().getContent();
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        VectorTile.Tile tile = VectorTile.Tile.parseFrom(inputStream);
        assertThat(tile.getLayersCount(), Matchers.equalTo(2));
        {
            VectorTile.Tile.Layer layer = getLayer(tile, "AGG");
            assertThat(layer.getValuesCount(), Matchers.equalTo(1));
            assertThat(layer.getExtent(), Matchers.equalTo(256));
            assertThat(layer.getFeaturesCount(), Matchers.equalTo(1));
        }
        {
            VectorTile.Tile.Layer layer = getLayer(tile, "META");
            assertThat(layer.getFeaturesCount(), Matchers.equalTo(1));
            assertThat(layer.getExtent(), Matchers.equalTo(256));
        }
    }

    public void testEmpty() throws Exception {
        final int newY = (1 << z) - 1 == y ? y - 1 : y + 1;
        final Request mvtRequest = new Request(HttpGet.METHOD_NAME, INDEX_POINTS + "/_agg_mvt/location/" + z + "/" + x + "/" + newY);
        Response response = client().performRequest(mvtRequest);
        InputStream inputStream = response.getEntity().getContent();
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        VectorTile.Tile tile = VectorTile.Tile.parseFrom(inputStream);
        assertThat(tile.getLayersCount(), Matchers.equalTo(2));
        {
            VectorTile.Tile.Layer layer = getLayer(tile, "AGG");
            assertThat(layer.getValuesCount(), Matchers.equalTo(0));
            assertThat(layer.getExtent(), Matchers.equalTo(256));
            assertThat(layer.getFeaturesCount(), Matchers.equalTo(0));
        }
        {
            VectorTile.Tile.Layer layer = getLayer(tile, "META");
            assertThat(layer.getFeaturesCount(), Matchers.equalTo(0));
            assertThat(layer.getExtent(), Matchers.equalTo(256));
        }
    }

    public void testBasicScaling() throws Exception {
        final Request mvtRequest = new Request(HttpGet.METHOD_NAME, INDEX_POINTS + "/_agg_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\"scaling\": 7 }");
        Response response = client().performRequest(mvtRequest);
        InputStream inputStream = response.getEntity().getContent();
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        VectorTile.Tile tile = VectorTile.Tile.parseFrom(inputStream);
        assertThat(tile.getLayersCount(), Matchers.equalTo(2));
        {
            VectorTile.Tile.Layer layer = getLayer(tile, "AGG");
            assertThat(layer.getValuesCount(), Matchers.equalTo(1));
            assertThat(layer.getFeaturesCount(), Matchers.equalTo(1));
            assertThat(layer.getExtent(), Matchers.equalTo(128));
        }
        {
            VectorTile.Tile.Layer layer = getLayer(tile, "META");
            assertThat(layer.getFeaturesCount(), Matchers.equalTo(1));
            assertThat(layer.getExtent(), Matchers.equalTo(128));
        }
    }

    private VectorTile.Tile.Layer getLayer(VectorTile.Tile tile, String layerName) {
        for (int i = 0; i < tile.getLayersCount(); i++) {
            VectorTile.Tile.Layer layer = tile.getLayers(i);
            if (layerName.equals(layer.getName())) {
                return layer;
            }
        }
        fail("Could not find layer " + layerName);
        return null;
    }

    @AwaitsFix(bugUrl = "doesn't work yet")
    public void testBasicQueryGet() throws Exception {
        final Request mvtRequest = new Request(HttpGet.METHOD_NAME, INDEX_POINTS + "/_agg_mvt/location/" + z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\n" +
            "  \"query\": {\n" +
            "    \"term\": {\n" +
            "      \"name\": {\n" +
            "         \"value\": \"point2\"\n" +
            "        }\n" +
            "    }\n" +
            "  }\n" +
            "}");
        Response response = client().performRequest(mvtRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        InputStream inputStream = response.getEntity().getContent();
        VectorTile.Tile tile = VectorTile.Tile.parseFrom(inputStream);
        assertThat(tile.getLayersCount(), Matchers.equalTo(2));
        {
            VectorTile.Tile.Layer layer = getLayer(tile, "AGG");
            assertThat(layer.getValuesCount(), Matchers.equalTo(1));
            assertThat(layer.getFeaturesCount(), Matchers.equalTo(1));
            assertThat(layer.getExtent(), Matchers.equalTo(256));
        }
        {
            VectorTile.Tile.Layer layer = getLayer(tile, "META");
            assertThat(layer.getFeaturesCount(), Matchers.equalTo(1));
            assertThat(layer.getExtent(), Matchers.equalTo(256));
        }
    }

    public void testBasicShape() throws Exception {
        final Request mvtRequest = new Request(HttpGet.METHOD_NAME, INDEX_SHAPES + "/_agg_mvt/location/"+ z + "/" + x + "/" + y);
        Response response = client().performRequest(mvtRequest);
        InputStream inputStream = response.getEntity().getContent();
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        VectorTile.Tile tile = VectorTile.Tile.parseFrom(inputStream);
        assertThat(tile.getLayersCount(), Matchers.equalTo(2));
        {
            VectorTile.Tile.Layer layer = getLayer(tile, "AGG");
            assertThat(layer.getValuesCount(), Matchers.equalTo(1));
            assertThat(layer.getExtent(), Matchers.equalTo(256));
            assertThat(layer.getFeaturesCount(), Matchers.equalTo(256 * 256));
        }
        {
            VectorTile.Tile.Layer layer = getLayer(tile, "META");
            assertThat(layer.getFeaturesCount(), Matchers.equalTo(1));
            assertThat(layer.getExtent(), Matchers.equalTo(256));
        }
    }

    public void testMinAgg() throws Exception {
        final Request mvtRequest = new Request(HttpGet.METHOD_NAME, INDEX_SHAPES + "/_agg_mvt/location/"+ z + "/" + x + "/" + y);
        mvtRequest.setJsonEntity("{\n" +
            "  \"aggs\": {\n" +
            "    \"minVal\": {\n" +
            "      \"min\": {\n" +
            "         \"field\": \"value1\"\n" +
            "        }\n" +
            "    }\n" +
            "  }\n" +
            "}");
        Response response = client().performRequest(mvtRequest);
        InputStream inputStream = response.getEntity().getContent();
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        VectorTile.Tile tile = VectorTile.Tile.parseFrom(inputStream);
        assertThat(tile.getLayersCount(), Matchers.equalTo(2));
        {
            VectorTile.Tile.Layer layer = getLayer(tile, "AGG");
            assertThat(layer.getValuesCount(), Matchers.equalTo(1));
            assertThat(layer.getExtent(), Matchers.equalTo(256));
            assertThat(layer.getFeaturesCount(), Matchers.equalTo(256 * 256));
        }
        {
            VectorTile.Tile.Layer layer = getLayer(tile, "META");
            assertThat(layer.getFeaturesCount(), Matchers.equalTo(1));
            assertThat(layer.getExtent(), Matchers.equalTo(256));
        }
    }
}
