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

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;

import java.io.IOException;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.startsWith;

/**
 * Tests for the "Location" header returned when returning {@code 201 CREATED}.
 */
public class CreatedLocationHeaderIT extends ESRestTestCase {
    public void testCreate() throws IOException {
        locationTestCase("PUT", "test/test/1");
    }

    public void testIndexWithId() throws IOException {
        locationTestCase("PUT", "test/test/1");
    }

    public void testIndexWithoutId() throws IOException {
        locationTestCase("POST", "test/test");
    }

    public void testUpsert() throws IOException {
        locationTestCase(client().performRequest("POST", "test/test/1/_update", emptyMap(), new StringEntity("{"
                + "\"doc\": {\"test\": \"test\"},"
                + "\"doc_as_upsert\": true}", ContentType.APPLICATION_JSON)));
    }

    private void locationTestCase(String method, String url) throws IOException {
        locationTestCase(client().performRequest(method, url, emptyMap(),
            new StringEntity("{\"test\": \"test\"}", ContentType.APPLICATION_JSON)));
        locationTestCase(client().performRequest(method, url + "?routing=cat", emptyMap(),
            new StringEntity("{\"test\": \"test\"}", ContentType.APPLICATION_JSON)));
    }

    private void locationTestCase(Response response) throws IOException {
        assertEquals(201, response.getStatusLine().getStatusCode());
        String location = response.getHeader("Location");
        assertThat(location, startsWith("/test/test/"));
        Response getResponse = client().performRequest("GET", location);
        assertEquals(singletonMap("test", "test"), entityAsMap(getResponse).get("_source"));
    }
}
