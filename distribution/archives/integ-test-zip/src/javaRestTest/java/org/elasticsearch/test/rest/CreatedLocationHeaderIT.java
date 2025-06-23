/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;

import java.io.IOException;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for the "Location" header returned when returning {@code 201 CREATED}.
 */
public class CreatedLocationHeaderIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local().build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testCreate() throws IOException {
        locationTestCase("PUT", "test/_doc/1");
    }

    public void testIndexWithId() throws IOException {
        locationTestCase("PUT", "test/_doc/1");
    }

    public void testIndexWithoutId() throws IOException {
        locationTestCase("POST", "test/_doc");
    }

    public void testUpsert() throws IOException {
        Request request = new Request("POST", "test/_update/1");
        request.setJsonEntity("""
            {"doc": {"test": "test"},"doc_as_upsert": true}
            """);
        locationTestCase(client().performRequest(request));
    }

    private void locationTestCase(String method, String url) throws IOException {
        final Request request = new Request(method, url);
        request.setJsonEntity("{\"test\": \"test\"}");
        locationTestCase(client().performRequest(request));
        // we have to delete the index otherwise the second indexing request will route to the single shard and not produce a 201
        final Response response = client().performRequest(new Request("DELETE", "test"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
        final Request withRouting = new Request(method, url);
        withRouting.addParameter("routing", "cat");
        withRouting.setJsonEntity("{\"test\": \"test\"}");
        locationTestCase(client().performRequest(withRouting));
    }

    private void locationTestCase(Response response) throws IOException {
        assertEquals(201, response.getStatusLine().getStatusCode());
        String location = response.getHeader("Location");
        assertThat(location, startsWith("/test/_doc/"));
        Response getResponse = client().performRequest(new Request("GET", location));
        assertEquals(singletonMap("test", "test"), entityAsMap(getResponse).get("_source"));
    }

}
