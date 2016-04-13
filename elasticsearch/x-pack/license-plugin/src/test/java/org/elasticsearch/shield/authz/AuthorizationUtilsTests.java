/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.shield.user.SystemUser;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.authc.InternalAuthenticationService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import static org.hamcrest.Matchers.is;

/**
 * Unit tests for the AuthorizationUtils class
 */
public class AuthorizationUtilsTests extends ESTestCase {

    private ThreadContext threadContext;

    @Before
    public void setupContext() {
        threadContext = new ThreadContext(Settings.EMPTY);
    }

    public void testSystemUserSwitchNonInternalAction() {
        assertThat(AuthorizationUtils.shouldReplaceUserWithSystem(threadContext, randomFrom("indices:foo", "cluster:bar")), is(false));
    }

    public void testSystemUserSwitchWithNullorSystemUser() {
        if (randomBoolean()) {
            threadContext.putTransient(InternalAuthenticationService.USER_KEY, SystemUser.INSTANCE);
        }
        assertThat(AuthorizationUtils.shouldReplaceUserWithSystem(threadContext, "internal:something"), is(true));
    }

    public void testSystemUserSwitchWithNonSystemUser() {
        User user = new User(randomAsciiOfLength(6), new String[] {});
        threadContext.putTransient(InternalAuthenticationService.USER_KEY, user);
        threadContext.putTransient(InternalAuthorizationService.ORIGINATING_ACTION_KEY, randomFrom("indices:foo", "cluster:bar"));
        assertThat(AuthorizationUtils.shouldReplaceUserWithSystem(threadContext, "internal:something"), is(true));
    }

    public void testSystemUserSwitchWithNonSystemUserAndInternalAction() {
        User user = new User(randomAsciiOfLength(6), new String[] {});
        threadContext.putTransient(InternalAuthenticationService.USER_KEY, user);
        threadContext.putTransient(InternalAuthorizationService.ORIGINATING_ACTION_KEY, randomFrom("internal:foo/bar"));
        assertThat(AuthorizationUtils.shouldReplaceUserWithSystem(threadContext, "internal:something"), is(false));
    }
}