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
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.XPackClientPlugin;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.client.SecurityClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Integration tests for the custom authorization engine. These tests are meant to be run against
 * an external cluster with the custom authorization plugin installed to validate the functionality
 * when running as a plugin
 */
public class CustomAuthorizationEngineIT extends ESIntegTestCase {

    @Override
    protected Settings externalClusterClientSettings() {
        final String token = "Basic " +
            Base64.getEncoder().encodeToString(("test_user:x-pack-test-password").getBytes(StandardCharsets.UTF_8));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(NetworkModule.TRANSPORT_TYPE_KEY, "security4")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return Collections.singleton(XPackClientPlugin.class);
    }

    public void testClusterAction() throws IOException {
        SecurityClient securityClient = new SecurityClient(client());
        securityClient.preparePutUser("custom_user", "x-pack-test-password".toCharArray(), Hasher.BCRYPT, "custom_superuser").get();

        {
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                basicAuthHeaderValue("custom_user", new SecureString("x-pack-test-password".toCharArray())));
            Request request = new Request("GET", "_cluster/health");
            request.setOptions(options);
            Response response = getRestClient().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        }

        {
            securityClient.preparePutUser("custom_user2", "x-pack-test-password".toCharArray(), Hasher.BCRYPT, "not_superuser").get();
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                basicAuthHeaderValue("custom_user2", new SecureString("x-pack-test-password".toCharArray())));
            Request request = new Request("GET", "_cluster/health");
            request.setOptions(options);
            ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }
    }

    public void testIndexAction() throws IOException {
        SecurityClient securityClient = new SecurityClient(client());
        securityClient.preparePutUser("custom_user", "x-pack-test-password".toCharArray(), Hasher.BCRYPT, "custom_superuser").get();

        {
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                basicAuthHeaderValue("custom_user", new SecureString("x-pack-test-password".toCharArray())));
            Request request = new Request("PUT", "/index");
            request.setOptions(options);
            Response response = getRestClient().performRequest(request);
            assertThat(response.getStatusLine().getStatusCode(), is(200));
        }

        {
            securityClient.preparePutUser("custom_user2", "x-pack-test-password".toCharArray(), Hasher.BCRYPT, "not_superuser").get();
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                basicAuthHeaderValue("custom_user2", new SecureString("x-pack-test-password".toCharArray())));
            Request request = new Request("PUT", "/index");
            request.setOptions(options);
            ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }
    }

    public void testRunAs() throws IOException {
        SecurityClient securityClient = new SecurityClient(client());
        securityClient.preparePutUser("custom_user", "x-pack-test-password".toCharArray(), Hasher.BCRYPT, "custom_superuser").get();
        securityClient.preparePutUser("custom_user2", "x-pack-test-password".toCharArray(), Hasher.BCRYPT, "custom_superuser").get();
        securityClient.preparePutUser("custom_user3", "x-pack-test-password".toCharArray(), Hasher.BCRYPT, "not_superuser").get();

        {
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                basicAuthHeaderValue("custom_user", new SecureString("x-pack-test-password".toCharArray())));
            options.addHeader("es-security-runas-user", "custom_user2");
            Request request = new Request("GET", "/_security/_authenticate");
            request.setOptions(options);
            Response response = getRestClient().performRequest(request);
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
            ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }

        {
            RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
            options.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                basicAuthHeaderValue("custom_user3", new SecureString("x-pack-test-password".toCharArray())));
            options.addHeader("es-security-runas-user", "custom_user2");
            Request request = new Request("PUT", "/index");
            request.setOptions(options);
            ResponseException e = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(403));
        }
    }
}
