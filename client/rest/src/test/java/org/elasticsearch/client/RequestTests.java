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

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;

public class RequestTests extends RestClientTestCase {

    public void testConstructor() {
        final String method = randomFrom(new String[] {"GET", "PUT", "POST", "HEAD", "DELETE"});
        final String endpoint = randomAsciiLettersOfLengthBetween(1, 10);
        final Map<String, String> parameters = singletonMap(randomAsciiLettersOfLength(5), randomAsciiLettersOfLength(5));
        final HttpEntity entity =
                randomBoolean() ? new StringEntity(randomAsciiLettersOfLengthBetween(1, 100), ContentType.TEXT_PLAIN) : null;

        try {
            new Request(null, endpoint, parameters, entity);
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("method cannot be null", e.getMessage());
        }

        try {
            new Request(method, null, parameters, entity);
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("endpoint cannot be null", e.getMessage());
        }

        try {
            new Request(method, endpoint, null, entity);
            fail("expected failure");
        } catch (NullPointerException e) {
            assertEquals("parameters cannot be null", e.getMessage());
        }

        final Request request = new Request(method, endpoint, parameters, entity);
        assertEquals(method, request.getMethod());
        assertEquals(endpoint, request.getEndpoint());
        assertEquals(parameters, request.getParameters());
        assertEquals(entity, request.getEntity());
    }

}
