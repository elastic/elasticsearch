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

package org.elasticsearch.rest;

import org.apache.http.entity.StringEntity;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Tests that HTTP HEAD requests don't respond with a body.
 */
public class HeadBodyIsEmptyIntegTestCase extends ESRestTestCase {
    public void testHeadRoot() throws IOException {
        headTestCase("/", emptyMap(), greaterThan(0));
        headTestCase("/", singletonMap("pretty", ""), greaterThan(0));
        headTestCase("/", singletonMap("pretty", "true"), greaterThan(0));
    }

    private void createTestDoc() throws UnsupportedEncodingException, IOException {
        client().performRequest("PUT", "test/test/1", emptyMap(), new StringEntity("{\"test\": \"test\"}"));
    }

    public void testDocumentExists() throws IOException {
        createTestDoc();
        headTestCase("test/test/1", emptyMap(), equalTo(0));
        headTestCase("test/test/1", singletonMap("pretty", "true"), equalTo(0));
    }

    public void testIndexExists() throws IOException {
        createTestDoc();
        headTestCase("test", emptyMap(), equalTo(0));
        headTestCase("test", singletonMap("pretty", "true"), equalTo(0));
    }

    public void testTypeExists() throws IOException {
        createTestDoc();
        headTestCase("test/test", emptyMap(), equalTo(0));
        headTestCase("test/test", singletonMap("pretty", "true"), equalTo(0));
    }

    private void headTestCase(String url, Map<String, String> params, Matcher<Integer> matcher) throws IOException {
        Response response = client().performRequest("HEAD", url, params);
        assertEquals(200, response.getStatusLine().getStatusCode());
        assertThat(Integer.valueOf(response.getHeader("Content-Length")), matcher);
        assertNull("HEAD requests shouldn't have a response body but " + url + " did", response.getEntity());
    }
}
