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

import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.RequestConvertersTests.assertToXContentBody;

public class SecurityRequestConvertersTests extends ESTestCase {

    public void testPutUser() throws IOException {
        final String username = randomAlphaOfLengthBetween(4, 12);
        final char[] password = randomBoolean() ? randomAlphaOfLengthBetween(8, 12).toCharArray() : null;
        final List<String> roles = Arrays.asList(generateRandomStringArray(randomIntBetween(2, 8), randomIntBetween(8, 16), false, true));
        final String email = randomBoolean() ? null : randomAlphaOfLengthBetween(12, 24);
        final String fullName = randomBoolean() ? null : randomAlphaOfLengthBetween(7, 14);
        final boolean enabled = randomBoolean();
        final Map<String, Object> metadata;
        if (randomBoolean()) {
            metadata = new HashMap<>();
            for (int i = 0; i < randomIntBetween(0, 10); i++) {
                metadata.put(String.valueOf(i), randomAlphaOfLengthBetween(1, 12));
            }
        } else {
            metadata = null;
        }

        final RefreshPolicy refreshPolicy = randomFrom(RefreshPolicy.values());
        final Map<String, String> expectedParams;
        if (refreshPolicy != RefreshPolicy.NONE) {
            expectedParams = Collections.singletonMap("refresh", refreshPolicy.getValue());
        } else {
            expectedParams = Collections.emptyMap();
        }

        PutUserRequest putUserRequest = new PutUserRequest(username, password, roles, fullName, email, enabled, metadata, refreshPolicy);
        Request request = SecurityRequestConverters.putUser(putUserRequest);
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertEquals("/_xpack/security/user/" + putUserRequest.getUsername(), request.getEndpoint());
        assertEquals(expectedParams, request.getParameters());
        assertToXContentBody(putUserRequest, request.getEntity());
    }
}
