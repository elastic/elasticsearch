/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

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
        ElasticsearchClient restClient = new ElasticsearchClient(new RestClientTransport(client(), new JacksonJsonpMapper()));
        restClient.security().putUser(req -> req
            .username("custom_user")
            .roles("custom_superuser")
            .password("x-pack-test-password")
            .enabled(true)
        );

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
            restClient.security().putUser(req -> req
                .username("custom_user2")
                .roles("not_superuser")
                .password("x-pack-test-password")
                .enabled(true)
            );
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
        ElasticsearchClient restClient = new ElasticsearchClient(new RestClientTransport(client(), new JacksonJsonpMapper()));
        restClient.security().putUser(req -> req
            .username("custom_user")
            .roles("custom_superuser")
            .password("x-pack-test-password")
            .enabled(true)
        );

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
            restClient.security().putUser(req -> req
                .username("custom_user2")
                .roles("not_superuser")
                .password("x-pack-test-password")
                .enabled(true)
            );
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
        ElasticsearchClient restClient = new ElasticsearchClient(new RestClientTransport(client(), new JacksonJsonpMapper()));
        restClient.security().putUser(req -> req
            .username("custom_user")
            .roles("custom_superuser")
            .password("x-pack-test-password")
            .enabled(true)
        );
        restClient.security().putUser(req -> req
            .username("custom_user2")
            .roles("custom_superuser")
            .password("x-pack-test-password")
            .enabled(true)
        );
        restClient.security().putUser(req -> req
            .username("custom_user3")
            .roles("not_superuser")
            .password("x-pack-test-password")
            .enabled(true)
        );

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
}
