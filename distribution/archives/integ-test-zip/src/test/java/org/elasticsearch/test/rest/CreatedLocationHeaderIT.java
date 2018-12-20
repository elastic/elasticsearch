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

package org.elasticsearch.test.rest;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;

import java.io.IOException;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for the "Location" header returned when returning {@code 201 CREATED}.
 */
public class CreatedLocationHeaderIT extends ESRestTestCase {

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
        request.setJsonEntity("{"
            + "\"doc\": {\"test\": \"test\"},"
            + "\"doc_as_upsert\": true}");
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
