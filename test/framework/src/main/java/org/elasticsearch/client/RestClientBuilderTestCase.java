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

import joptsimple.internal.Strings;
import org.apache.http.Header;
import org.elasticsearch.test.ESTestCase;

/**
 * A test case with access to internals of a RestClient.
 */
public abstract class RestClientBuilderTestCase extends ESTestCase {
    /** Checks the given rest client has the provided default headers. */
    public void assertHeaders(RestClient client, Map<String, String> expectedHeaders) {
        expectedHeaders = new HashMap<>(expectedHeaders); // copy so we can remove as we check
        for (Header header : client.defaultHeaders) {
            String name = header.getName();
            String expectedValue = expectedHeaders.remove(name);
            if (expectedValue == null) {
                fail("Found unexpected header in rest client: " + name);
            }
            assertEquals(expectedValue, header.getValue());
        }
        if (expectedHeaders.isEmpty() == false) {
            fail("Missing expected headers in rest client: " + Strings.join(expectedHeaders.keySet(), ", "));
        }
    }
}
