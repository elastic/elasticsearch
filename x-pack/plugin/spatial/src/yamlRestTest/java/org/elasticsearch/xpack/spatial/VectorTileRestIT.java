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
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;

public class VectorTileRestIT extends ESRestTestCase {

    private static String INDEX_NAME = "my-index";

    @Before
    public void indexDocuments() throws IOException {

        final Request createRequest = new Request(HttpPut.METHOD_NAME, INDEX_NAME);
        Response response = client().performRequest(createRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));

        final Request mappingRequest = new Request(HttpPut.METHOD_NAME, INDEX_NAME + "/_mapping");
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

        final Request putRequest1 = new Request(HttpPost.METHOD_NAME, INDEX_NAME + "/_doc");
        putRequest1.setJsonEntity("{\n" +
            "  \"location\": \"POINT(0 0)\", \"name\": \"point1\"\n" +
            "}");

        response = client().performRequest(putRequest1);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_CREATED));

        final Request putRequest2 = new Request(HttpPost.METHOD_NAME, INDEX_NAME + "/_doc");
        putRequest2.setJsonEntity("{\n" +
            "  \"location\": \"POINT(1 1)\", \"name\": \"point2\"\n" +
            "}");

        response = client().performRequest(putRequest2);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_CREATED));

        final Request flushRequest = new Request(HttpPost.METHOD_NAME, INDEX_NAME + "/_refresh");
        response = client().performRequest(flushRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
    }

    @After
    public void deleteData() throws IOException {
        final Request deleteRequest = new Request(HttpDelete.METHOD_NAME, INDEX_NAME);
        Response response = client().performRequest(deleteRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
    }

    public void testBasicGet() throws Exception {
        final Request mvtRequest = new Request(HttpGet.METHOD_NAME, INDEX_NAME + "/_mvt/location/0/0/0");
        Response response = client().performRequest(mvtRequest);
        InputStream inputStream = response.getEntity().getContent();
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        VectorTile.Tile.Builder builder = VectorTile.Tile.newBuilder().mergeFrom(inputStream);
        assertThat(builder.getLayers(0).getFeaturesCount(), Matchers.equalTo(2));
    }

    public void testBasicQueryGet() throws Exception {
        final Request mvtRequest = new Request(HttpGet.METHOD_NAME, INDEX_NAME + "/_mvt/location/0/0/0");
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
        VectorTile.Tile.Builder builder = VectorTile.Tile.newBuilder().mergeFrom(inputStream);
        assertThat(builder.getLayers(0).getFeaturesCount(), Matchers.equalTo(1));
    }
}
