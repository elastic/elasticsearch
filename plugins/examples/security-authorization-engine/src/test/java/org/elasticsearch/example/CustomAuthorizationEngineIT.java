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

package org.elasticsearch.example;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for the custom authorization engine. These tests are meant to be run against
 * an external cluster with the custom authorization plugin installed to validate the functionality
 * when running as a plugin
 */
public class CustomAuthorizationEngineIT extends ESRestTestCase {

    @Override
    protected Settings restClientSettings() {
        final String token = "Basic " +
            Base64.getEncoder().encodeToString(("test_user:x-pack-test-password").getBytes(StandardCharsets.UTF_8));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    public void testClusterAction() throws IOException {
        RestHighLevelClient restClient = new TestRestHighLevelClient();
        restClient.security().putUser(PutUserRequest.withPassword(new User("custom_user", List.of("custom_superuser")),
            "x-pack-test-password".toCharArray(), true, RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);

        {
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                basicAuthHeaderValue("custom_user", new SecureString("x-pack-test-password".toCharArray())));
            Request request = new Request("GET", "_cluster/health");
            request.setOptions(options);
            Response response = client().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        }

        {
            restClient.security().putUser(PutUserRequest.withPassword(new User("custom_user2", List.of("not_superuser")),
                "x-pack-test-password".toCharArray(), true, RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                basicAuthHeaderValue("custom_user2", new SecureString("x-pack-test-password".toCharArray())));
            Request request = new Request("GET", "_cluster/health");
            request.setOptions(options);
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }
    }

    public void testIndexAction() throws IOException {
        RestHighLevelClient restClient = new TestRestHighLevelClient();
        restClient.security().putUser(PutUserRequest.withPassword(new User("custom_user", List.of("custom_superuser")),
            "x-pack-test-password".toCharArray(), true, RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);

        {
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                basicAuthHeaderValue("custom_user", new SecureString("x-pack-test-password".toCharArray())));
            Request request = new Request("PUT", "/index");
            request.setOptions(options);
            Response response = client().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        }

        {
            restClient.security().putUser(PutUserRequest.withPassword(new User("custom_user2", List.of("not_superuser")),
                "x-pack-test-password".toCharArray(), true, RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                basicAuthHeaderValue("custom_user2", new SecureString("x-pack-test-password".toCharArray())));
            Request request = new Request("PUT", "/index");
            request.setOptions(options);
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }
    }

    public void testRunAs() throws IOException {
        RestHighLevelClient restClient = new TestRestHighLevelClient();
        restClient.security().putUser(PutUserRequest.withPassword(new User("custom_user", List.of("custom_superuser")),
            "x-pack-test-password".toCharArray(), true, RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        restClient.security().putUser(PutUserRequest.withPassword(new User("custom_user2", List.of("custom_superuser")),
            "x-pack-test-password".toCharArray(), true, RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        restClient.security().putUser(PutUserRequest.withPassword(new User("custom_user3", List.of("not_superuser")),
            "x-pack-test-password".toCharArray(), true, RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);

        {
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                basicAuthHeaderValue("custom_user", new SecureString("x-pack-test-password".toCharArray())));
            options.addHeader("es-security-runas-user", "custom_user2");
            Request request = new Request("GET", "/_security/_authenticate");
            request.setOptions(options);
            Response response = client().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
            String responseStr = EntityUtils.toString(response.getEntity());
            assertThat(responseStr, containsString("custom_user2"));
        }

        {
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                basicAuthHeaderValue("custom_user", new SecureString("x-pack-test-password".toCharArray())));
            options.addHeader("es-security-runas-user", "custom_user3");
            Request request = new Request("PUT", "/index");
            request.setOptions(options);
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }

        {
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                basicAuthHeaderValue("custom_user3", new SecureString("x-pack-test-password".toCharArray())));
            options.addHeader("es-security-runas-user", "custom_user2");
            Request request = new Request("PUT", "/index");
            request.setOptions(options);
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(request));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }
    }

    private class TestRestHighLevelClient extends RestHighLevelClient {
        TestRestHighLevelClient() {
            super(client(), restClient -> {}, Collections.emptyList());
        }
    }
}
