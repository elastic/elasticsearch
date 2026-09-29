/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.request;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContentObject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.xpack.inference.external.request.RequestUtils.apiKey;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.bearerToken;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthApiKeyHeader;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.createAuthBearerHeader;
import static org.elasticsearch.xpack.inference.external.request.RequestUtils.jsonEntity;
import static org.hamcrest.Matchers.containsString;
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

    public void testJsonEntity_WritesTheSerializedObject() throws IOException {
        ToXContentObject object = (builder, params) -> builder.startObject().field("key", "value").endObject();

        var entity = jsonEntity(object);

        assertThat(EntityUtils.toString(entity, StandardCharsets.UTF_8), is("""
            {"key":"value"}"""));
        assertTrue("entity must be repeatable so the request can be retried", entity.isRepeatable());
    }

    public void testJsonEntity_WhenSerializationFails_Throws() {
        ToXContentObject object = (builder, params) -> { throw new IOException("boom"); };

        var exception = expectThrows(UncheckedIOException.class, () -> jsonEntity(object));

        assertThat(exception.getMessage(), containsString("Failed to serialize ["));
        assertThat(exception.getCause().getMessage(), is("boom"));
    }
}
