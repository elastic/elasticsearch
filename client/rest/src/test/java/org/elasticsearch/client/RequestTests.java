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

package org.elasticsearch.client;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class RequestTests extends RestClientTestCase {
    public void testConstructor() {
        final String method = randomFrom(new String[] {"GET", "PUT", "POST", "HEAD", "DELETE"});
        final String endpoint = randomAsciiLettersOfLengthBetween(1, 10);

        try {
            new Request(null, endpoint);
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("method cannot be null", e.getMessage());
        }

        try {
            new Request(method, null);
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("endpoint cannot be null", e.getMessage());
        }

        final Request request = new Request(method, endpoint);
        assertEquals(method, request.getMethod());
        assertEquals(endpoint, request.getEndpoint());
    }

    public void testAddParameters() {
        final String method = randomFrom(new String[] {"GET", "PUT", "POST", "HEAD", "DELETE"});
        final String endpoint = randomAsciiLettersOfLengthBetween(1, 10);
        int parametersCount = between(1, 3);
        final Map<String, String> parameters = new HashMap<>(parametersCount);
        while (parameters.size() < parametersCount) {
            parameters.put(randomAsciiLettersOfLength(5), randomAsciiLettersOfLength(5));
        }
        Request request = new Request(method, endpoint);

        try {
            request.addParameter(null, "value");
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("url parameter name cannot be null", e.getMessage());
        }

        for (Map.Entry<String, String> entry : parameters.entrySet()) {
            request.addParameter(entry.getKey(), entry.getValue());
        }
        assertEquals(parameters, request.getParameters());

        // Test that adding parameters with a null value is ok.
        request.addParameter("is_null", null);
        parameters.put("is_null", null);
        assertEquals(parameters, request.getParameters());

        // Test that adding a duplicate parameter fails
        String firstValue = randomBoolean() ? null : "value";
        request.addParameter("name", firstValue);
        try {
            request.addParameter("name", randomBoolean() ? firstValue : "second_value");
            fail("expected failure");
        } catch (IllegalArgumentException e) {
            assertEquals("url parameter [name] has already been set to [" + firstValue + "]", e.getMessage());
        }
    }

    public void testSetEntity() {
        final String method = randomFrom(new String[] {"GET", "PUT", "POST", "HEAD", "DELETE"});
        final String endpoint = randomAsciiLettersOfLengthBetween(1, 10);
        final HttpEntity entity =
                randomBoolean() ? new StringEntity(randomAsciiLettersOfLengthBetween(1, 100), ContentType.TEXT_PLAIN) : null;
        Request request = new Request(method, endpoint);

        request.setEntity(entity);
        assertEquals(entity, request.getEntity());
    }

    public void testSetHeaders() {
        final String method = randomFrom(new String[] {"GET", "PUT", "POST", "HEAD", "DELETE"});
        final String endpoint = randomAsciiLettersOfLengthBetween(1, 10);
        Request request = new Request(method, endpoint);

        try {
            request.setHeaders((Header[]) null);
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("headers cannot be null", e.getMessage());
        }

        try {
            request.setHeaders(new Header [] {null});
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("header cannot be null", e.getMessage());
        }

        Header[] headers = new Header[between(0, 5)];
        for (int i = 0; i < headers.length; i++) {
            headers[i] = new BasicHeader(randomAsciiAlphanumOfLength(3), randomAsciiAlphanumOfLength(3));
        }
        request.setHeaders(headers);
        assertArrayEquals(headers, request.getHeaders());
    }

    // TODO equals and hashcode

}
