/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.elasticsearch.http.HttpServerTransport;
import org.elasticsearch.shield.authc.support.Hasher;
import org.elasticsearch.shield.authc.support.SecuredString;
import org.elasticsearch.shield.authc.support.UsernamePasswordToken;
import org.elasticsearch.test.ShieldIntegTestCase;
import org.elasticsearch.test.rest.client.http.HttpRequestBuilder;
import org.elasticsearch.test.rest.client.http.HttpResponse;
import org.junit.After;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * a helper class that contains a couple of HTTP helper methods
 */
public abstract class AbstractPrivilegeTestCase extends ShieldIntegTestCase {

    protected static final String USERS_PASSWD_HASHED = new String(Hasher.BCRYPT.hash(new SecuredString("passwd".toCharArray())));

    private CloseableHttpClient httpClient = HttpClients.createDefault();

    @After
    public void cleanup() throws IOException {
        httpClient.close();
    }

    protected void assertAccessIsAllowed(String user, String method, String uri, String body,
                                         Map<String, String> params) throws IOException {
        HttpResponse response = executeRequest(user, method, uri, body, params);
        String message = String.format(Locale.ROOT, "%s %s: Expected no error got %s %s with body %s", method, uri,
                response.getStatusCode(), response.getReasonPhrase(), response.getBody());
        assertThat(message, response.getStatusCode(), is(not(greaterThanOrEqualTo(400))));
    }

    protected void assertAccessIsAllowed(String user, String method, String uri, String body) throws IOException {
        assertAccessIsAllowed(user, method, uri, body, new HashMap<>());
    }

    protected void assertAccessIsAllowed(String user, String method, String uri) throws IOException {
        assertAccessIsAllowed(user, method, uri, null, new HashMap<>());
    }

    protected void assertAccessIsDenied(String user, String method, String uri, String body) throws IOException {
        assertAccessIsDenied(user, method, uri, body, new HashMap<>());
    }

    protected void assertAccessIsDenied(String user, String method, String uri) throws IOException {
        assertAccessIsDenied(user, method, uri, null, new HashMap<>());
    }

    protected void assertAccessIsDenied(String user, String method, String uri, String body,
                                        Map<String, String> params) throws IOException {
        HttpResponse response = executeRequest(user, method, uri, body, params);
        String message = String.format(Locale.ROOT, "%s %s body %s: Expected 403, got %s %s with body %s", method, uri, body,
                response.getStatusCode(), response.getReasonPhrase(), response.getBody());
        assertThat(message, response.getStatusCode(), is(403));
    }

    protected HttpResponse executeRequest(String user, String method, String uri, String body,
                                          Map<String, String> params) throws IOException {
        HttpServerTransport httpServerTransport = internalCluster().getDataNodeInstance(HttpServerTransport.class);

        HttpRequestBuilder requestBuilder = new HttpRequestBuilder(httpClient).httpTransport(httpServerTransport);
        requestBuilder.path(uri);
        requestBuilder.method(method);
        for (Map.Entry<String, String> entry : params.entrySet()) {
            requestBuilder.addParam(entry.getKey(), entry.getValue());
        }
        if (body != null) {
            requestBuilder.body(body);
        }
        requestBuilder.addHeader(UsernamePasswordToken.BASIC_AUTH_HEADER, UsernamePasswordToken.basicAuthHeaderValue(user,
                new SecuredString("passwd".toCharArray())));
        return requestBuilder.execute();
    }

}
