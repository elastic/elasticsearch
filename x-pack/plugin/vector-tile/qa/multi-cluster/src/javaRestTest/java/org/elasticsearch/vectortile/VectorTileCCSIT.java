/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.vectortile;

import com.wdtinc.mapbox_vector_tile.VectorTile;

import org.apache.http.HttpHost;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.io.InputStream;

public class VectorTileCCSIT extends ESRestTestCase {

    @Override
    protected Settings restClientSettings() {
        final String token = basicAuthHeaderValue("admin", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private int createIndex(RestClient client, String indexName) throws IOException {
        final Request createRequest = new Request(HttpPut.METHOD_NAME, indexName);
        Response response = client.performRequest(createRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        final Request mappingRequest = new Request(HttpPut.METHOD_NAME, indexName + "/_mapping");
        mappingRequest.setJsonEntity("""
            {
              "properties": {
                "location": {
                  "type": "geo_shape"
                }
              }
            }""");
        response = client.performRequest(mappingRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));

        final Request putRequest = new Request(HttpPost.METHOD_NAME, indexName + "/_doc");
        putRequest.setJsonEntity("{\"location\": \"POINT(0 0)\"}");

        // just add the shape geometry n times
        final int numGeometries = randomIntBetween(1, 10);
        for (int i = 0; i < numGeometries; i++) {
            response = client.performRequest(putRequest);
            assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_CREATED));
        }

        final Request flushRequest = new Request(HttpPost.METHOD_NAME, indexName + "/_refresh");
        response = client.performRequest(flushRequest);
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        return numGeometries;
    }

    public void testBasic() throws IOException {
        try (RestClient local = buildLocalClusterClient(); RestClient remote = buildRemoteClusterClient()) {
            final int localGeometries = createIndex(local, "test");
            final int remoteGeometries = createIndex(remote, "test");
            // check call in each cluster
            final Request mvtRequest = new Request(HttpPost.METHOD_NAME, "test/_mvt/location/0/0/0");
            final VectorTile.Tile localTile = execute(local, mvtRequest);
            assertThat(getLayer(localTile, "hits").getFeaturesCount(), Matchers.equalTo(localGeometries));
            final VectorTile.Tile remoteTile = execute(remote, mvtRequest);
            assertThat(getLayer(remoteTile, "hits").getFeaturesCount(), Matchers.equalTo(remoteGeometries));
            // call to both clusters
            final Request mvtCCSRequest = new Request(HttpPost.METHOD_NAME, "/test,other:test/_mvt/location/0/0/0");
            final VectorTile.Tile ccsTile = execute(local, mvtCCSRequest);
            assertThat(getLayer(ccsTile, "hits").getFeaturesCount(), Matchers.equalTo(localGeometries + remoteGeometries));
        }
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

    private VectorTile.Tile execute(RestClient client, Request mvtRequest) throws IOException {
        final Response response = client.performRequest(mvtRequest);
        final InputStream inputStream = response.getEntity().getContent();
        assertThat(response.getStatusLine().getStatusCode(), Matchers.equalTo(HttpStatus.SC_OK));
        return VectorTile.Tile.parseFrom(inputStream);
    }

    private RestClient buildLocalClusterClient() throws IOException {
        return buildClient(System.getProperty("tests.local"));
    }

    private RestClient buildRemoteClusterClient() throws IOException {
        return buildClient(System.getProperty("tests.remote"));
    }

    private RestClient buildClient(final String url) throws IOException {
        final int portSeparator = url.lastIndexOf(':');
        final HttpHost httpHost = new HttpHost(
            url.substring(0, portSeparator),
            Integer.parseInt(url.substring(portSeparator + 1)),
            getProtocol()
        );
        return buildClient(restAdminSettings(), new HttpHost[] { httpHost });
    }
}
