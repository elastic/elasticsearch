/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.test.rest;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.RestTestCandidate;
import org.elasticsearch.test.rest.parser.RestTestParseException;
import org.elasticsearch.xpack.common.xcontent.XContentUtils;
import org.junit.After;
import org.junit.Before;

import static org.elasticsearch.shield.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.is;

public abstract class XPackRestTestCase extends ESRestTestCase {

    private static final String BASIC_AUTH_VALUE = basicAuthHeaderValue("test_user", new SecuredString("changeme".toCharArray()));

    public XPackRestTestCase(@Name("yaml") RestTestCandidate testCandidate) {
        super(testCandidate);
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() throws IOException, RestTestParseException {
        return ESRestTestCase.createParameters(0, 1);
    }

    @Before
    public void startWatcher() throws Exception {
        try (CloseableHttpClient client = HttpClients.createMinimal(new BasicHttpClientConnectionManager())) {
            URL url = getClusterUrls()[0];
            HttpPut request = new HttpPut(new URI("http",
                                                  null,
                                                  url.getHost(),
                                                  url.getPort(),
                                                  "/_watcher/_start", null, null));
            request.addHeader("Authorization", BASIC_AUTH_VALUE);
            try (CloseableHttpResponse response = client.execute(request)) {
            }
        }
    }

    @After
    public void stopWatcher() throws Exception {
        try(CloseableHttpClient client = HttpClients.createMinimal(new BasicHttpClientConnectionManager())) {
            URL url = getClusterUrls()[0];
            HttpPut request = new HttpPut(new URI("http",
                                                  null,
                                                  url.getHost(),
                                                  url.getPort(),
                                                  "/_watcher/_stop", null, null));
            request.addHeader("Authorization", BASIC_AUTH_VALUE);
            try (CloseableHttpResponse response = client.execute(request)) {
            }
        }
    }

    @After
    public void clearShieldUsersAndRoles() throws Exception {
        // we cannot delete the .security index from a rest test since we aren't the internal user, lets wipe the data
        // TODO remove this once the built-in SUPERUSER role is added that can delete the index and we use the built in admin user here
        try (CloseableHttpClient client = HttpClients.createMinimal(new BasicHttpClientConnectionManager())) {
            final URL url = getClusterUrls()[0];
            HttpGet getUsersRequest = new HttpGet(new URI("http", null, url.getHost(), url.getPort(), "/_shield/user", null, null));
            getUsersRequest.addHeader("Authorization", BASIC_AUTH_VALUE);
            try (CloseableHttpResponse closeableHttpResponse = client.execute(getUsersRequest)) {
                assertThat(closeableHttpResponse.getStatusLine().getStatusCode(), is(200));
                String response = Streams.copyToString(
                        new InputStreamReader(closeableHttpResponse.getEntity().getContent(), StandardCharsets.UTF_8));
                Map<String, Object> responseMap = XContentFactory.xContent(response).createParser(response).map();

                // in the structure of this API, the users are the keyset
                for (String user : responseMap.keySet()) {
                    HttpDelete delete = new HttpDelete(new URI("http", null, url.getHost(), url.getPort(),
                            "/_shield/user/" + user, null, null));
                    delete.addHeader("Authorization", BASIC_AUTH_VALUE);
                    try (CloseableHttpResponse deleteResponse = client.execute(delete)) {
                    }
                }
            }

            HttpGet getRolesRequest = new HttpGet(new URI("http", null, url.getHost(), url.getPort(), "/_shield/role",
                    null, null));
            getRolesRequest.addHeader("Authorization", BASIC_AUTH_VALUE);
            try (CloseableHttpResponse closeableHttpResponse = client.execute(getRolesRequest)) {
                assertThat(closeableHttpResponse.getStatusLine().getStatusCode(), is(200));
                String response = Streams.copyToString(
                        new InputStreamReader(closeableHttpResponse.getEntity().getContent(), StandardCharsets.UTF_8));
                Map<String, Object> responseMap = XContentFactory.xContent(response).createParser(response).map();

                // in the structure of this API, the users are the keyset
                for (String role : responseMap.keySet()) {
                    HttpDelete delete = new HttpDelete(new URI("http", null, url.getHost(), url.getPort(),
                            "/_shield/role/" + role, null, null));
                    delete.addHeader("Authorization", BASIC_AUTH_VALUE);
                    try (CloseableHttpResponse deleteResponse = client.execute(delete)) {
                    }
                }
            }
        }
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", BASIC_AUTH_VALUE)
                .build();
    }
}
