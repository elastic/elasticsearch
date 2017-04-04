/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.Authentication.RealmRef;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
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

    public void testSystemUserSwitchWithSystemUser() {
        threadContext.putTransient(Authentication.AUTHENTICATION_KEY,
                new Authentication(SystemUser.INSTANCE, new RealmRef("test", "test", "foo"), null));
        assertThat(AuthorizationUtils.shouldReplaceUserWithSystem(threadContext, "internal:something"), is(false));
    }

    public void testSystemUserSwitchWithNullUser() {
        assertThat(AuthorizationUtils.shouldReplaceUserWithSystem(threadContext, "internal:something"), is(true));
    }

    public void testSystemUserSwitchWithNonSystemUser() {
        User user = new User(randomAlphaOfLength(6), new String[] {});
        Authentication authentication =  new Authentication(user, new RealmRef("test", "test", "foo"), null);
        threadContext.putTransient(Authentication.AUTHENTICATION_KEY, authentication);
        threadContext.putTransient(AuthorizationService.ORIGINATING_ACTION_KEY, randomFrom("indices:foo", "cluster:bar"));
        assertThat(AuthorizationUtils.shouldReplaceUserWithSystem(threadContext, "internal:something"), is(true));
    }

    public void testSystemUserSwitchWithNonSystemUserAndInternalAction() {
        User user = new User(randomAlphaOfLength(6), new String[] {});
        Authentication authentication =  new Authentication(user, new RealmRef("test", "test", "foo"), null);
        threadContext.putTransient(Authentication.AUTHENTICATION_KEY, authentication);
        threadContext.putTransient(AuthorizationService.ORIGINATING_ACTION_KEY, randomFrom("internal:foo/bar"));
        assertThat(AuthorizationUtils.shouldReplaceUserWithSystem(threadContext, "internal:something"), is(false));
    }
}
