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
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests.randomUniquelyNamedRoleDescriptors;
import static org.hamcrest.Matchers.equalTo;

public class RemoteAccessHeadersTests extends ESTestCase {

    public void testWriteAndReadContext() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        final RoleDescriptorsIntersection rds = randomRoleDescriptorsIntersection();
        final var expected = new RemoteAccessHeaders(
            new ApiKeyService.ApiKeyCredentials(UUIDs.base64UUID(), UUIDs.randomBase64UUIDSecureString()),
            AuthenticationTestHelper.randomRemoteAccessAuthentication(rds)
        );

        expected.writeToContext(ctx);
        final var actual = RemoteAccessHeaders.readFromContext(ctx);

        assertThat(actual, equalTo(expected));
    }

    private RoleDescriptorsIntersection randomRoleDescriptorsIntersection() {
        return new RoleDescriptorsIntersection(randomList(0, 3, () -> Set.copyOf(randomUniquelyNamedRoleDescriptors(0, 1))));
    }
}
