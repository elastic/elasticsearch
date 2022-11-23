/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests.randomUniquelyNamedRoleDescriptors;
import static org.hamcrest.Matchers.equalTo;

public class RemoteAccessAuthenticationTests extends ESTestCase {

    public void testWriteReadContextRoundtrip() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        final Authentication expectedAuthentication = AuthenticationTestHelper.builder().build();
        final RoleDescriptorsIntersection expectedRoleDescriptorsIntersection = randomRoleDescriptorIntersection();

        RemoteAccessAuthentication.writeToContextAsRemoteAccessAuthentication(
            ctx,
            expectedAuthentication,
            expectedRoleDescriptorsIntersection
        );
        final RemoteAccessAuthentication actual = RemoteAccessAuthentication.readFromContext(ctx);

        assertThat(actual.authentication(), equalTo(expectedAuthentication));
        final var actualRoleDescriptorIntersection = new RoleDescriptorsIntersection(
            actual.roleDescriptorsBytesIntersection().stream().map(RemoteAccessAuthentication::parseRoleDescriptorBytes).toList()
        );
        assertThat(actualRoleDescriptorIntersection, equalTo(expectedRoleDescriptorsIntersection));
    }

    public void testWriteToContextThrowsIfHeaderAlreadyPresent() throws IOException {
        final ThreadContext ctx = new ThreadContext(Settings.EMPTY);
        RemoteAccessAuthentication.writeToContextAsRemoteAccessAuthentication(
            ctx,
            AuthenticationTestHelper.builder().build(),
            randomRoleDescriptorIntersection()
        );
        final IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> RemoteAccessAuthentication.writeToContextAsRemoteAccessAuthentication(
                ctx,
                AuthenticationTestHelper.builder().build(),
                randomRoleDescriptorIntersection()
            )
        );
        assertThat(
            ex.getMessage(),
            equalTo(
                "remote access authentication ["
                    + RemoteAccessAuthentication.REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY
                    + "] is already present in the context"
            )
        );
    }

    private RoleDescriptorsIntersection randomRoleDescriptorIntersection() {
        return new RoleDescriptorsIntersection(randomList(0, 3, () -> Set.copyOf(randomUniquelyNamedRoleDescriptors(0, 1))));
    }
}
