/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.test.SecuritySingleNodeTestCase;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.authc.support.HasherFactory;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

/**
 * a helper class that contains a couple of HTTP helper methods
 */
public abstract class AbstractPrivilegeTestCase extends SecuritySingleNodeTestCase {

    protected void assertAccessIsAllowed(String user, String method, String uri, String body,
                                         Map<String, String> params) throws IOException {
        Response response = getRestClient().performRequest(method, uri, params, entityOrNull(body),
                new BasicHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                        UsernamePasswordToken.basicAuthHeaderValue(user, new SecureString("passwd".toCharArray()))));
        StatusLine statusLine = response.getStatusLine();
        String message = String.format(Locale.ROOT, "%s %s: Expected no error got %s %s with body %s", method, uri,
                statusLine.getStatusCode(), statusLine.getReasonPhrase(), EntityUtils.toString(response.getEntity()));
        assertThat(message, statusLine.getStatusCode(), is(not(greaterThanOrEqualTo(400))));
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
        ResponseException responseException = expectThrows(ResponseException.class,
                () -> getRestClient().performRequest(method, uri, params, entityOrNull(body),
                        new BasicHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                                UsernamePasswordToken.basicAuthHeaderValue(user, new SecureString("passwd".toCharArray())))));
        StatusLine statusLine = responseException.getResponse().getStatusLine();
        String message = String.format(Locale.ROOT, "%s %s body %s: Expected 403, got %s %s with body %s", method, uri, body,
                statusLine.getStatusCode(), statusLine.getReasonPhrase(),
                EntityUtils.toString(responseException.getResponse().getEntity()));
        assertThat(message, statusLine.getStatusCode(), is(403));
    }


    protected void assertBodyHasAccessIsDenied(String user, String method, String uri, String body) throws IOException {
        assertBodyHasAccessIsDenied(user, method, uri, body, new HashMap<>());
    }

    /**
     * Like {@code assertAcessIsDenied}, but for _bulk requests since the entire
     * request will not be failed, just the individual ones
     */
    protected void assertBodyHasAccessIsDenied(String user, String method, String uri, String body,
                                               Map<String, String> params) throws IOException {
        Response resp = getRestClient().performRequest(method, uri, params, entityOrNull(body),
                new BasicHeader(UsernamePasswordToken.BASIC_AUTH_HEADER,
                        UsernamePasswordToken.basicAuthHeaderValue(user, new SecureString("passwd".toCharArray()))));
        StatusLine statusLine = resp.getStatusLine();
        assertThat(statusLine.getStatusCode(), is(200));
        HttpEntity bodyEntity = resp.getEntity();
        String bodyStr = EntityUtils.toString(bodyEntity);
        assertThat(bodyStr, containsString("unauthorized for user [" + user + "]"));
    }

    private static HttpEntity entityOrNull(String body) {
        HttpEntity entity = null;
        if (body != null) {
            entity = new StringEntity(body, ContentType.APPLICATION_JSON);
        }
        return entity;
    }
}
