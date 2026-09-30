/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;

import java.net.URI;
import java.net.URISyntaxException;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.apiKey;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.bearerToken;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthApiKeyHeader;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.decorateWithAuthHeader;
import static org.hamcrest.Matchers.is;

public class RequestUtilsTests extends ESTestCase {
    private static final String SECRET = "abc";
    private static final String BEARER_PREFIX = "Bearer ";
    private static final String APIKEY_PREFIX = "ApiKey ";
    private static final String AUTHORIZATION_HEADER = "Authorization";

    public void testCreateAuthBearerHeader() {
        var header = createAuthBearerHeader(new SecureString(SECRET.toCharArray()));

        assertThat(header.getName(), is(AUTHORIZATION_HEADER));
        assertThat(header.getValue(), is(BEARER_PREFIX + SECRET));
    }

    public void testBearerToken() {
        assertThat(bearerToken(SECRET), is(BEARER_PREFIX + SECRET));
    }

    public void testCreateAuthApiKeyHeader() {
        var header = createAuthApiKeyHeader(new SecureString(SECRET.toCharArray()));

        assertThat(header.getName(), is(AUTHORIZATION_HEADER));
        assertThat(header.getValue(), is(APIKEY_PREFIX + SECRET));
    }

    public void testApiKey() {
        assertThat(apiKey(SECRET), is(APIKEY_PREFIX + SECRET));
    }

    public void testDecorateWithAuthHeader() throws URISyntaxException {
        var httpPost = new HttpPost(new URI("https://example.com/v1/embeddings"));
        decorateWithAuthHeader(httpPost, new SecureString(SECRET.toCharArray()));

        assertThat(httpPost.getFirstHeader(HttpHeaders.CONTENT_TYPE).getValue(), is(XContentType.JSON.mediaType()));
        assertThat(httpPost.getFirstHeader(HttpHeaders.AUTHORIZATION).getValue(), is(Strings.format("Bearer %s", SECRET)));
    }
}
