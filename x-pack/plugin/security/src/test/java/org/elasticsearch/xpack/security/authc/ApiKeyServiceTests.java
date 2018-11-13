/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;

public class ApiKeyServiceTests extends ESTestCase {

    public void testGetCredentialsFromThreadContext() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        assertNull(ApiKeyService.getCredentialsFromHeader(threadContext));

        final String apiKeyAuthScheme = randomFrom("apikey", "apiKey", "ApiKey", "APikey", "APIKEY");
        final String id = randomAlphaOfLength(12);
        final String key = randomAlphaOfLength(16);
        String headerValue = apiKeyAuthScheme + " " + Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", headerValue);
            ApiKeyService.ApiKeyCredentials creds = ApiKeyService.getCredentialsFromHeader(threadContext);
            assertNotNull(creds);
            assertEquals(id, creds.getId());
            assertEquals(key, creds.getKey().toString());
        }

        // missing space
        headerValue = apiKeyAuthScheme + Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", headerValue);
            ApiKeyService.ApiKeyCredentials creds = ApiKeyService.getCredentialsFromHeader(threadContext);
            assertNull(creds);
        }

        // missing colon
        headerValue = apiKeyAuthScheme + " " + Base64.getEncoder().encodeToString((id + key).getBytes(StandardCharsets.UTF_8));
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.putHeader("Authorization", headerValue);
            IllegalArgumentException e =
                expectThrows(IllegalArgumentException.class, () -> ApiKeyService.getCredentialsFromHeader(threadContext));
            assertEquals("invalid ApiKey value", e.getMessage());
        }
    }

    public void testValidateApiKey() throws Exception {
        final String apiKey = randomAlphaOfLength(16);
        Hasher hasher = randomFrom(Hasher.PBKDF2, Hasher.BCRYPT4, Hasher.BCRYPT);
        final char[] hash = hasher.hash(new SecureString(apiKey.toCharArray()));

        Map<String, Object> sourceMap = new HashMap<>();
        sourceMap.put("api_key_hash", new String(hash));
        sourceMap.put("principal", "test_user");
        sourceMap.put("metadata", Collections.emptyMap());
        sourceMap.put("roles", Collections.singletonList("a role"));


        ApiKeyService.ApiKeyCredentials creds =
            new ApiKeyService.ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(apiKey.toCharArray()));
        PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        ApiKeyService.validateApiKeyCredentials(sourceMap, creds, Clock.systemUTC(), future);
        AuthenticationResult result = future.get();
        assertNotNull(result);
        assertTrue(result.isAuthenticated());
        assertThat(result.getUser().principal(), is("test_user"));
        assertThat(result.getUser().roles(), arrayContaining("a role"));
        assertThat(result.getUser().metadata(), is(Collections.emptyMap()));

        sourceMap.put("expiration_time", Clock.systemUTC().instant().plus(1L, ChronoUnit.HOURS).toEpochMilli());
        future = new PlainActionFuture<>();
        ApiKeyService.validateApiKeyCredentials(sourceMap, creds, Clock.systemUTC(), future);
        result = future.get();
        assertNotNull(result);
        assertTrue(result.isAuthenticated());
        assertThat(result.getUser().principal(), is("test_user"));
        assertThat(result.getUser().roles(), arrayContaining("a role"));
        assertThat(result.getUser().metadata(), is(Collections.emptyMap()));

        sourceMap.put("expiration_time", Clock.systemUTC().instant().minus(1L, ChronoUnit.HOURS).toEpochMilli());
        future = new PlainActionFuture<>();
        ApiKeyService.validateApiKeyCredentials(sourceMap, creds, Clock.systemUTC(), future);
        result = future.get();
        assertNotNull(result);
        assertFalse(result.isAuthenticated());

        sourceMap.remove("expiration_time");
        creds = new ApiKeyService.ApiKeyCredentials(randomAlphaOfLength(12), new SecureString(randomAlphaOfLength(15).toCharArray()));
        future = new PlainActionFuture<>();
        ApiKeyService.validateApiKeyCredentials(sourceMap, creds, Clock.systemUTC(), future);
        result = future.get();
        assertNotNull(result);
        assertFalse(result.isAuthenticated());
    }
}
