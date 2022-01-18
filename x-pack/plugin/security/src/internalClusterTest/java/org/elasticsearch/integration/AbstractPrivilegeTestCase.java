/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.io.IOException;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * A helper class that contains a couple of HTTP helper methods.
 */
public abstract class AbstractPrivilegeTestCase extends SecuritySingleNodeTestCase {

    protected Response assertAccessIsAllowed(String user, Request request) throws IOException {
        setUser(request, user);
        Response response = getRestClient().performRequest(request);
        StatusLine statusLine = response.getStatusLine();
        String message = String.format(
            Locale.ROOT,
            "%s %s: Expected no error got %s %s with body %s",
            request.getMethod(),
            request.getEndpoint(),
            statusLine.getStatusCode(),
            statusLine.getReasonPhrase(),
            EntityUtils.toString(response.getEntity())
        );
        assertThat(message, statusLine.getStatusCode(), is(not(greaterThanOrEqualTo(400))));
        return response;
    }

    protected Response assertAccessIsAllowed(String user, String method, String uri, String body) throws IOException {
        Request request = new Request(method, uri);
        request.setJsonEntity(body);
        return assertAccessIsAllowed(user, request);
    }

    protected void assertAccessIsAllowed(String user, String method, String uri) throws IOException {
        assertAccessIsAllowed(user, new Request(method, uri));
    }

    protected void assertAccessIsDenied(String user, Request request) throws IOException {
        setUser(request, user);
        ResponseException responseException = expectThrows(ResponseException.class, () -> getRestClient().performRequest(request));
        StatusLine statusLine = responseException.getResponse().getStatusLine();
        String requestBody = request.getEntity() == null ? "" : "with body " + EntityUtils.toString(request.getEntity());
        String message = String.format(
            Locale.ROOT,
            "%s %s body %s: Expected 403, got %s %s with body %s",
            request.getMethod(),
            request.getEndpoint(),
            requestBody,
            statusLine.getStatusCode(),
            statusLine.getReasonPhrase(),
            EntityUtils.toString(responseException.getResponse().getEntity())
        );
        assertThat(message, statusLine.getStatusCode(), is(403));
    }

    protected void assertAccessIsDenied(String user, String method, String uri, String body) throws IOException {
        Request request = new Request(method, uri);
        request.setJsonEntity(body);
        assertAccessIsDenied(user, request);
    }

    protected void assertAccessIsDenied(String user, String method, String uri) throws IOException {
        assertAccessIsDenied(user, new Request(method, uri));
    }

    /**
     * Like {@code assertAcessIsDenied}, but for _bulk requests since the entire
     * request will not be failed, just the individual ones
     */
    protected void assertBodyHasAccessIsDenied(String user, Request request) throws IOException {
        setUser(request, user);
        Response resp = getRestClient().performRequest(request);
        StatusLine statusLine = resp.getStatusLine();
        assertThat(statusLine.getStatusCode(), is(200));
        HttpEntity bodyEntity = resp.getEntity();
        String bodyStr = EntityUtils.toString(bodyEntity);
        assertThat(bodyStr, containsString("unauthorized for user [" + user + "]"));
    }

    protected void assertBodyHasAccessIsDenied(String user, String method, String uri, String body) throws IOException {
        Request request = new Request(method, uri);
        request.setJsonEntity(body);
        assertBodyHasAccessIsDenied(user, request);
    }

    private void setUser(Request request, String user) {
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.addHeader(
            "Authorization",
            UsernamePasswordToken.basicAuthHeaderValue(user, SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING)
        );
        request.setOptions(options);
    }
}
