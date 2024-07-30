/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.is;

public class AsyncSearchSecurityTests extends ESSingleNodeTestCase {

    public void testEnsuredAuthenticatedUserIsSame() throws IOException {
        final ThreadContext threadContext = client().threadPool().getThreadContext();
        final AsyncSearchSecurity security = new AsyncSearchSecurity(
            ".async-search",
            new SecurityContext(Settings.EMPTY, threadContext),
            client(),
            "async_origin"
        );

        Authentication original = AuthenticationTestHelper.builder()
            .user(new User("test", "role"))
            .realmRef(new Authentication.RealmRef("realm", "file", "node"))
            .build(false);
        Authentication current = randomBoolean()
            ? original
            : AuthenticationTestHelper.builder()
                .user(new User("test", "role"))
                .realmRef(new Authentication.RealmRef("realm", "file", "node"))
                .build(false);
        current.writeToContext(threadContext);
        assertThat(security.currentUserHasAccessToTaskWithHeaders(getAuthenticationAsHeaders(original)), is(true));

        // "original" search was unauthenticated (e.g. security was turned off when it was performed)
        assertThat(security.currentUserHasAccessToTaskWithHeaders(Collections.emptyMap()), is(true));

        // current is not authenticated
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            assertThat(security.currentUserHasAccessToTaskWithHeaders(getAuthenticationAsHeaders(original)), is(false));
            assertThat(security.currentUserHasAccessToTaskWithHeaders(Map.of()), is(true));
        }

        // original user being run as
        final User authenticatingUser = new User("authenticated", "runas");
        final User effectiveUser = new User("test", "role");
        assertThat(
            security.currentUserHasAccessToTaskWithHeaders(
                getAuthenticationAsHeaders(
                    AuthenticationTestHelper.builder()
                        .user(authenticatingUser)
                        .realmRef(new Authentication.RealmRef(randomAlphaOfLengthBetween(1, 16), "file", "node"))
                        .runAs()
                        .user(effectiveUser)
                        .realmRef(new Authentication.RealmRef("realm", "file", "node"))
                        .build()
                )
            ),
            is(true)
        );

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // current user being run as
            current = AuthenticationTestHelper.builder()
                .user(authenticatingUser)
                .realmRef(new Authentication.RealmRef(randomAlphaOfLengthBetween(1, 16), "file", "node"))
                .runAs()
                .user(effectiveUser)
                .realmRef(new Authentication.RealmRef("realm", "file", "node"))
                .build();
            current.writeToContext(threadContext);
            assertThat(security.currentUserHasAccessToTaskWithHeaders(getAuthenticationAsHeaders(original)), is(true));

            // both users are run as
            assertThat(
                security.currentUserHasAccessToTaskWithHeaders(
                    getAuthenticationAsHeaders(
                        AuthenticationTestHelper.builder()
                            .user(authenticatingUser)
                            .realmRef(new Authentication.RealmRef(randomAlphaOfLengthBetween(1, 16), "file", "node"))
                            .runAs()
                            .user(effectiveUser)
                            .realmRef(new Authentication.RealmRef("realm", "file", "node"))
                            .build()
                    )
                ),
                is(true)
            );
        }

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // different authenticated by type
            final Authentication differentRealmType = AuthenticationTestHelper.builder()
                .user(new User("test", "role"))
                .realmRef(new Authentication.RealmRef("realm", randomAlphaOfLength(10), "node"))
                .build(false);
            differentRealmType.writeToContext(threadContext);
            assertFalse(security.currentUserHasAccessToTaskWithHeaders(getAuthenticationAsHeaders(original)));
        }

        // different user
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            final Authentication differentUser = AuthenticationTestHelper.builder()
                .user(new User("test2", "role"))
                .realmRef(new Authentication.RealmRef("realm", "file", "node"))
                .build(false);
            differentUser.writeToContext(threadContext);
            assertFalse(security.currentUserHasAccessToTaskWithHeaders(getAuthenticationAsHeaders(original)));
        }

        // run as different user
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            final Authentication differentRunAs = AuthenticationTestHelper.builder()
                .user(new User("authenticated", "runas"))
                .realmRef(new Authentication.RealmRef("realm_runas", "file", "node1"))
                .runAs()
                .user(new User("test2", "role"))
                .realmRef(new Authentication.RealmRef("realm", "file", "node1"))
                .build();
            differentRunAs.writeToContext(threadContext);
            assertFalse(security.currentUserHasAccessToTaskWithHeaders(getAuthenticationAsHeaders(original)));
        }

        // run as different looked up by type
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            final Authentication runAsDiffType = AuthenticationTestHelper.builder()
                .user(authenticatingUser)
                .realmRef(new Authentication.RealmRef("realm", "file", "node"))
                .runAs()
                .user(effectiveUser)
                .realmRef(new Authentication.RealmRef(randomAlphaOfLengthBetween(1, 16), randomAlphaOfLengthBetween(5, 12), "node"))
                .build();
            runAsDiffType.writeToContext(threadContext);
            assertFalse(security.currentUserHasAccessToTaskWithHeaders(getAuthenticationAsHeaders(original)));
        }
    }

    private Map<String, String> getAuthenticationAsHeaders(Authentication authentication) throws IOException {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        authentication.writeToContext(threadContext);
        return threadContext.getHeaders();
    }

}
