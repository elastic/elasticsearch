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

import org.elasticsearch.client.security.AuthenticateResponse;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.PutUserResponse;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.common.CharArrays;

import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class SecurityIT extends ESRestHighLevelClientTestCase {

    public void testAuthenticate() throws Exception {
        final SecurityClient securityClient = highLevelClient().security();
        final PutUserRequest putUserRequest = randomPutUserRequest();
        final PutUserResponse putUserResponse = execute(putUserRequest, securityClient::putUser, securityClient::putUserAsync);
        assertThat(putUserResponse.isCreated(), is(true));

        // correct password authenticate
        final String correctBasicAuthHeader = basicAuthHeader(putUserRequest.getUsername(), putUserRequest.getPassword());
        final AuthenticateResponse correctAuthenticateResponse = execute(securityClient::authenticate, securityClient::authenticateAsync,
                authorizationRequestOptions(correctBasicAuthHeader));

        assertThat(correctAuthenticateResponse.getUser().username(), is(putUserRequest.getUsername()));
        if (putUserRequest.getRoles().isEmpty()) {
            assertThat(correctAuthenticateResponse.getUser().roles(), is(empty()));
        } else {
            assertThat(correctAuthenticateResponse.getUser().roles(), contains(putUserRequest.getRoles().toArray()));
        }
        assertThat(correctAuthenticateResponse.getUser().metadata(), is(putUserRequest.getMetadata()));
        assertThat(correctAuthenticateResponse.getUser().fullName(), is(putUserRequest.getFullName()));
        assertThat(correctAuthenticateResponse.getUser().email(), is(putUserRequest.getEmail()));
    }
    
    // run as

    private static PutUserRequest randomPutUserRequest() {
        final String username = randomAlphaOfLengthBetween(1, 4);
        final char[] password = randomAlphaOfLengthBetween(6, 10).toCharArray();
        final List<String> roles = Arrays.asList(generateRandomStringArray(3, 3, false, true));
        final String fullName = randomFrom(random(), null, randomAlphaOfLengthBetween(0, 3));
        final String email = randomFrom(random(), null, randomAlphaOfLengthBetween(0, 3));
        final boolean enabled = randomBoolean();
        final Map<String, Object> metadata;
        metadata = new HashMap<>();
        if (randomBoolean()) {
            metadata.put("string", null);
        } else {
            metadata.put("string", randomAlphaOfLengthBetween(0, 4));
        }
        if (randomBoolean()) {
            metadata.put("string_list", null);
        } else {
            metadata.put("string_list", Arrays.asList(generateRandomStringArray(4, 4, false, true)));
        }
        return new PutUserRequest(username, password, roles, fullName, email, enabled, metadata, RefreshPolicy.IMMEDIATE);
    }
    
    private static String basicAuthHeader(String username, char[] password) {
        final String concat = new StringBuilder().append(username).append(':').append(password).toString();
        final byte[] concatBytes = CharArrays.toUtf8Bytes(concat.toCharArray());
        return "Basic " + Base64.getEncoder().encodeToString(concatBytes);
    }
    
    private static RequestOptions authorizationRequestOptions(String authorizationHeader) {
        final RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        builder.addHeader("Authorization", authorizationHeader);
        return builder.build();
    }
}
