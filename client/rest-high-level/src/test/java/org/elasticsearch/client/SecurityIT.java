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

import org.apache.http.client.methods.HttpDelete;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.client.security.AuthenticateResponse;
import org.elasticsearch.client.security.DeleteUserRequest;
import org.elasticsearch.client.security.DeleteUserResponse;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.PutUserResponse;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.CharArrays;

import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.containsString;

public class SecurityIT extends ESRestHighLevelClientTestCase {

    public void testPutUser() throws Exception {
        final SecurityClient securityClient = highLevelClient().security();
        // create user
        final PutUserRequest putUserRequest = randomPutUserRequest(randomBoolean());
        final PutUserResponse putUserResponse = execute(putUserRequest, securityClient::putUser, securityClient::putUserAsync);
        // assert user created
        assertThat(putUserResponse.isCreated(), is(true));
        // update user
        final User updatedUser = randomUser(putUserRequest.getUser().getUsername());
        final PutUserRequest updateUserRequest = randomPutUserRequest(updatedUser, randomBoolean());
        final PutUserResponse updateUserResponse = execute(updateUserRequest, securityClient::putUser, securityClient::putUserAsync);
        // assert user not created
        assertThat(updateUserResponse.isCreated(), is(false));
        // delete user
        final Request deleteUserRequest = new Request(HttpDelete.METHOD_NAME,
                "/_xpack/security/user/" + putUserRequest.getUser().getUsername());
        highLevelClient().getLowLevelClient().performRequest(deleteUserRequest);
    }

    public void testAuthenticate() throws Exception {
        final SecurityClient securityClient = highLevelClient().security();
        // test fixture: put enabled user
        final PutUserRequest putUserRequest = randomPutUserRequest(true);
        final PutUserResponse putUserResponse = execute(putUserRequest, securityClient::putUser, securityClient::putUserAsync);
        assertThat(putUserResponse.isCreated(), is(true));

        // authenticate correctly
        final String basicAuthHeader = basicAuthHeader(putUserRequest.getUser().getUsername(), putUserRequest.getPassword());
        final AuthenticateResponse authenticateResponse = execute(securityClient::authenticate, securityClient::authenticateAsync,
                authorizationRequestOptions(basicAuthHeader));

        assertThat(authenticateResponse.getUser(), is(putUserRequest.getUser()));
        assertThat(authenticateResponse.enabled(), is(true));

        // delete user
        final DeleteUserRequest deleteUserRequest =
            new DeleteUserRequest(putUserRequest.getUser().getUsername(), putUserRequest.getRefreshPolicy());

        final DeleteUserResponse deleteUserResponse =
            execute(deleteUserRequest, securityClient::deleteUser, securityClient::deleteUserAsync);
        assertThat(deleteUserResponse.isAcknowledged(), is(true));

        // authentication no longer works
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> execute(securityClient::authenticate,
                securityClient::authenticateAsync, authorizationRequestOptions(basicAuthHeader)));
        assertThat(e.getMessage(), containsString("unable to authenticate user [" + putUserRequest.getUser().getUsername() + "]"));

        // delete non-existing user
        final DeleteUserResponse deleteUserResponse2 =
            execute(deleteUserRequest, securityClient::deleteUser, securityClient::deleteUserAsync);
        assertThat(deleteUserResponse2.isAcknowledged(), is(false));
    }

    private static User randomUser() {
        final String username = randomAlphaOfLengthBetween(1, 4);
        return randomUser(username);
    }

    private static User randomUser(String username) {
        final List<String> roles = Arrays.asList(generateRandomStringArray(3, 3, false, true));
        final String fullName = randomFrom(random(), null, randomAlphaOfLengthBetween(0, 3));
        final String email = randomFrom(random(), null, randomAlphaOfLengthBetween(0, 3));
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
        return new User(username, roles, metadata, fullName, email);
    }

    private static PutUserRequest randomPutUserRequest(boolean enabled) {
        final User user = randomUser();
        return randomPutUserRequest(user, enabled);
    }

    private static PutUserRequest randomPutUserRequest(User user, boolean enabled) {
        final char[] password = randomAlphaOfLengthBetween(6, 10).toCharArray();
        return new PutUserRequest(user, password, enabled, RefreshPolicy.IMMEDIATE);
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
