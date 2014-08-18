/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.shield.User;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;

public class CachingUsernamePasswordRealmTests {
    public static class AlwaysAuthenticateCachingRealm extends CachingUsernamePasswordRealm {
        public AlwaysAuthenticateCachingRealm() {
            super(ImmutableSettings.EMPTY);
        }
        public final AtomicInteger INVOCATION_COUNTER = new AtomicInteger(0);
        @Override protected User doAuthenticate(UsernamePasswordToken token) {
            INVOCATION_COUNTER.incrementAndGet();
            return new User.Simple(token.principal(), "testRole1", "testRole2");
        }

        @Override public String type() { return "test"; };
    }


    @Test
    public void testCache(){
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm();
        char[] pass = "pass".toCharArray();
        realm.authenticate(new UsernamePasswordToken("a", pass));
        realm.authenticate(new UsernamePasswordToken("b", pass));
        realm.authenticate(new UsernamePasswordToken("c", pass));

        assertThat(realm.INVOCATION_COUNTER.intValue(), is(3));
        realm.authenticate(new UsernamePasswordToken("a", pass));
        realm.authenticate(new UsernamePasswordToken("b", pass));
        realm.authenticate(new UsernamePasswordToken("c", pass));

        assertThat(realm.INVOCATION_COUNTER.intValue(), is(3));
    }

    @Test
    public void testCache_changePassword(){
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm();

        String user = "testUser";
        char[] pass1 = "pass".toCharArray();
        char[] pass2 = "password".toCharArray();

        realm.authenticate(new UsernamePasswordToken(user, pass1));
        realm.authenticate(new UsernamePasswordToken(user, pass1));

        assertThat(realm.INVOCATION_COUNTER.intValue(), is(1));

        realm.authenticate(new UsernamePasswordToken(user, pass2));
        realm.authenticate(new UsernamePasswordToken(user, pass2));

        assertThat(realm.INVOCATION_COUNTER.intValue(), is(2));
    }
}
