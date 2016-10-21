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

import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;

/**
 * Tests that HTTP HEAD requests don't respond with a body.
 */
public class HeadBodyIsEmptyIT extends ESRestTestCase {
    public void testHeadRoot() throws IOException {
        headTestCase("/", emptyMap());
        headTestCase("/", singletonMap("pretty", ""));
        headTestCase("/", singletonMap("pretty", "true"));
    }

    private void createTestDoc() throws UnsupportedEncodingException, IOException {
        client().performRequest("PUT", "test/test/1", emptyMap(), new StringEntity("{\"test\": \"test\"}"));
    }

    public void testDocumentExists() throws IOException {
        createTestDoc();
        headTestCase("test/test/1", emptyMap());
        headTestCase("test/test/1", singletonMap("pretty", "true"));
    }

    public void testIndexExists() throws IOException {
        createTestDoc();
        headTestCase("test", emptyMap());
        headTestCase("test", singletonMap("pretty", "true"));
    }

    public void testTypeExists() throws IOException {
        createTestDoc();
        headTestCase("test/test", emptyMap());
        headTestCase("test/test", singletonMap("pretty", "true"));
    }

    private void headTestCase(String url, Map<String, String> params) throws IOException {
        Response response = client().performRequest("HEAD", url, params);
        assertEquals(200, response.getStatusLine().getStatusCode());
        /* Check that the content-length header is always 0. This isn't what we should be doing in the long run but it is what we expect
         * that we are *actually* doing. */
        assertEquals("We expect HEAD requests to have 0 Content-Length but " + url + " didn't", "0", response.getHeader("Content-Length"));
        assertNull("HEAD requests shouldn't have a response body but " + url + " did", response.getEntity());
    }
}
