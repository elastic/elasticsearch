/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests.randomUniquelyNamedRoleDescriptors;
import static org.elasticsearch.xpack.security.authc.RemoteAccessHeaders.REMOTE_CLUSTER_AUTHORIZATION_HEADER_KEY;
import static org.hamcrest.Matchers.equalTo;

public class RemoteAccessHeadersTests extends ESTestCase {

    public void testWriteReadContextRoundtrip() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        final var expected = new RemoteAccessHeaders(
            randomEncodedApiKeyHeader(),
            AuthenticationTestHelper.randomRemoteAccessAuthentication(randomRoleDescriptorsIntersection())
        );

        expected.writeToContext(ctx);
        final RemoteAccessHeaders actual = RemoteAccessHeaders.readFromContext(ctx);

        assertThat(actual.remoteAccessAuthentication(), equalTo(expected.remoteAccessAuthentication()));
        assertThat(actual.clusterCredentials().getId(), equalTo(expected.clusterCredentials().getId()));
        assertThat(actual.clusterCredentials().getKey().toString(), equalTo(expected.clusterCredentials().getKey().toString()));
    }

    public void testClusterCredentialsReturnsValidApiKey() {
        final String id = UUIDs.randomBase64UUID();
        final String key = UUIDs.randomBase64UUID();
        final String encodedApiKey = encodedApiKeyWithPrefix(id, key);
        final var headers = new RemoteAccessHeaders(
            encodedApiKey,
            AuthenticationTestHelper.randomRemoteAccessAuthentication(randomRoleDescriptorsIntersection())
        );

        final ApiKeyService.ApiKeyCredentials actual = headers.clusterCredentials();

        assertThat(actual.getId(), equalTo(id));
        assertThat(actual.getKey().toString(), equalTo(key));
    }

    public void testReadOnInvalidApiKeyValueThrows() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        final var expected = new RemoteAccessHeaders(
            randomFrom("ApiKey abc", "ApiKey id:key", "ApiKey ", "ApiKey  "),
            AuthenticationTestHelper.randomRemoteAccessAuthentication(randomRoleDescriptorsIntersection())
        );

        expected.writeToContext(ctx);
        var actual = expectThrows(IllegalArgumentException.class, () -> RemoteAccessHeaders.readFromContext(ctx));

        assertThat(
            actual.getMessage(),
            equalTo("remote access header [" + REMOTE_CLUSTER_AUTHORIZATION_HEADER_KEY + "] value must be a valid API key credential")
        );
    }

    public void testReadOnHeaderWithMalformedPrefixThrows() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        AuthenticationTestHelper.randomRemoteAccessAuthentication(randomRoleDescriptorsIntersection()).writeToContext(ctx);
        final String encodedApiKey = encodedApiKey(UUIDs.randomBase64UUID(), UUIDs.randomBase64UUID());
        ctx.putHeader(
            REMOTE_CLUSTER_AUTHORIZATION_HEADER_KEY,
            randomFrom(
                // missing space
                "ApiKey" + encodedApiKey,
                // no prefix
                encodedApiKey,
                // wrong prefix
                "Bearer " + encodedApiKey
            )
        );

        var actual = expectThrows(IllegalArgumentException.class, () -> RemoteAccessHeaders.readFromContext(ctx));

        assertThat(
            actual.getMessage(),
            equalTo("remote access header [" + REMOTE_CLUSTER_AUTHORIZATION_HEADER_KEY + "] value must be a valid API key credential")
        );
    }

    private static RoleDescriptorsIntersection randomRoleDescriptorsIntersection() {
        return new RoleDescriptorsIntersection(randomList(0, 3, () -> Set.copyOf(randomUniquelyNamedRoleDescriptors(0, 1))));
    }

    // TODO centralize common usage of this across all tests
    static String randomEncodedApiKeyHeader() {
        return encodedApiKeyWithPrefix(UUIDs.randomBase64UUID(), UUIDs.randomBase64UUID());
    }

    private static String encodedApiKeyWithPrefix(String id, String key) {
        return "ApiKey " + encodedApiKey(id, key);
    }

    private static String encodedApiKey(String id, String key) {
        return Base64.getEncoder().encodeToString((id + ":" + key).getBytes(StandardCharsets.UTF_8));
    }
}
