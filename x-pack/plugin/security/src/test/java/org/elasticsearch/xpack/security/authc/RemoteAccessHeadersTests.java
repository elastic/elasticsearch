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
import static org.elasticsearch.xpack.security.authc.RemoteAccessHeaders.REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY;
import static org.hamcrest.Matchers.equalTo;

public class RemoteAccessHeadersTests extends ESTestCase {

    public void testWriteAndReadContext() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        final var expected = new RemoteAccessHeaders(
            randomEncodedApiKey(),
            AuthenticationTestHelper.randomRemoteAccessAuthentication(randomRoleDescriptorsIntersection())
        );

        expected.writeToContext(ctx);
        final RemoteAccessHeaders actual = RemoteAccessHeaders.readFromContext(ctx);

        assertThat(actual, equalTo(expected));
    }

    public void testReadInvalidThrows() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        final var expected = new RemoteAccessHeaders(
            randomFrom("abc", "id:key", "", randomEncodedApiKey() + "suffix"),
            AuthenticationTestHelper.randomRemoteAccessAuthentication(randomRoleDescriptorsIntersection())
        );

        expected.writeToContext(ctx);
        var actual = expectThrows(IllegalArgumentException.class, () -> RemoteAccessHeaders.readFromContext(ctx));

        assertThat(
            actual.getMessage(),
            equalTo("remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] value must be a valid API key credential")
        );
    }

    private static RoleDescriptorsIntersection randomRoleDescriptorsIntersection() {
        return new RoleDescriptorsIntersection(randomList(0, 3, () -> Set.copyOf(randomUniquelyNamedRoleDescriptors(0, 1))));
    }

    // TODO centralize common usage of this across all tests
    private static String randomEncodedApiKey() {
        return Base64.getEncoder().encodeToString((UUIDs.base64UUID() + ":" + UUIDs.base64UUID()).getBytes(StandardCharsets.UTF_8));
    }
}
