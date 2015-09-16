/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authc.support;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.shield.User;
import org.elasticsearch.shield.authc.Realm;
import org.elasticsearch.shield.authc.RealmConfig;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.*;

public class CachingUsernamePasswordRealmTests extends ESTestCase {

    private Settings globalSettings;

    @Before
    public void setup() {
        globalSettings = Settings.builder().put("path.home", createTempDir()).build();
    }

    @Test
    public void testSettings() throws Exception {

        String hashAlgo = randomFrom("bcrypt", "bcrypt4", "bcrypt5", "bcrypt6", "bcrypt7", "bcrypt8", "bcrypt9", "sha1", "ssha256", "md5", "clear_text", "noop");
        int maxUsers = randomIntBetween(10, 100);
        TimeValue ttl = TimeValue.timeValueMinutes(randomIntBetween(10, 20));
        Settings settings = Settings.builder()
                .put(CachingUsernamePasswordRealm.CACHE_HASH_ALGO_SETTING, hashAlgo)
                .put(CachingUsernamePasswordRealm.CACHE_MAX_USERS_SETTING, maxUsers)
                .put(CachingUsernamePasswordRealm.CACHE_TTL_SETTING, ttl)
                .build();

        RealmConfig config = new RealmConfig("test_realm", settings, globalSettings);
        CachingUsernamePasswordRealm realm = new CachingUsernamePasswordRealm("test", config) {
            @Override
            protected User doAuthenticate(UsernamePasswordToken token) {
                return new User.Simple("username", new String[] { "r1", "r2", "r3" });
            }

            @Override
            protected User doLookupUser(String username) {
                throw new UnsupportedOperationException("this method should not be called");
            }

            @Override
            public boolean userLookupSupported() {
                return false;
            }
        };

        assertThat(realm.hasher, sameInstance(Hasher.resolve(hashAlgo)));
    }

    @Test
    public void testAuthCache() {
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(globalSettings);
        SecuredString pass = SecuredStringTests.build("pass");
        realm.authenticate(new UsernamePasswordToken("a", pass));
        realm.authenticate(new UsernamePasswordToken("b", pass));
        realm.authenticate(new UsernamePasswordToken("c", pass));

        assertThat(realm.authInvocationCounter.intValue(), is(3));
        realm.authenticate(new UsernamePasswordToken("a", pass));
        realm.authenticate(new UsernamePasswordToken("b", pass));
        realm.authenticate(new UsernamePasswordToken("c", pass));

        assertThat(realm.authInvocationCounter.intValue(), is(3));
        assertThat(realm.lookupInvocationCounter.intValue(), is(0));
    }

    @Test
    public void testLookupCache() {
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(globalSettings);
        realm.lookupUser("a");
        realm.lookupUser("b");
        realm.lookupUser("c");

        assertThat(realm.lookupInvocationCounter.intValue(), is(3));
        realm.lookupUser("a");
        realm.lookupUser("b");
        realm.lookupUser("c");

        assertThat(realm.authInvocationCounter.intValue(), is(0));
        assertThat(realm.lookupInvocationCounter.intValue(), is(3));
    }

    @Test
    public void testLookupAndAuthCache() {
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(globalSettings);
        // lookup first
        User lookedUp = realm.lookupUser("a");
        assertThat(realm.lookupInvocationCounter.intValue(), is(1));
        assertThat(realm.authInvocationCounter.intValue(), is(0));
        assertThat(lookedUp.roles(), arrayContaining("lookupRole1", "lookupRole2"));

        // now authenticate
        User user = realm.authenticate(new UsernamePasswordToken("a", SecuredStringTests.build("pass")));
        assertThat(realm.lookupInvocationCounter.intValue(), is(1));
        assertThat(realm.authInvocationCounter.intValue(), is(1));
        assertThat(user.roles(), arrayContaining("testRole1", "testRole2"));
        assertThat(user, not(sameInstance(lookedUp)));

        // authenticate a different user first
        user = realm.authenticate(new UsernamePasswordToken("b", SecuredStringTests.build("pass")));
        assertThat(realm.lookupInvocationCounter.intValue(), is(1));
        assertThat(realm.authInvocationCounter.intValue(), is(2));
        assertThat(user.roles(), arrayContaining("testRole1", "testRole2"));
        //now lookup b
        lookedUp = realm.lookupUser("b");
        assertThat(realm.lookupInvocationCounter.intValue(), is(1));
        assertThat(realm.authInvocationCounter.intValue(), is(2));
        assertThat(user, sameInstance(lookedUp));
    }

    @Test
    public void testCache_changePassword(){
        AlwaysAuthenticateCachingRealm realm = new AlwaysAuthenticateCachingRealm(globalSettings);

        String user = "testUser";
        SecuredString pass1 = SecuredStringTests.build("pass");
        SecuredString pass2 = SecuredStringTests.build("password");

        realm.authenticate(new UsernamePasswordToken(user, pass1));
        realm.authenticate(new UsernamePasswordToken(user, pass1));

        assertThat(realm.authInvocationCounter.intValue(), is(1));

        realm.authenticate(new UsernamePasswordToken(user, pass2));
        realm.authenticate(new UsernamePasswordToken(user, pass2));

        assertThat(realm.authInvocationCounter.intValue(), is(2));
    }

    @Test
    public void testAuthenticateContract() throws Exception {
        Realm<UsernamePasswordToken> realm = new FailingAuthenticationRealm(Settings.EMPTY, globalSettings);
        User user = realm.authenticate(new UsernamePasswordToken("user", SecuredStringTests.build("pass")));
        assertThat(user , nullValue());

        realm = new ThrowingAuthenticationRealm(Settings.EMPTY, globalSettings);
        user = realm.authenticate(new UsernamePasswordToken("user", SecuredStringTests.build("pass")));
        assertThat(user , nullValue());
    }

    @Test
    public void testLookupContract() throws Exception {
        Realm<UsernamePasswordToken> realm = new FailingAuthenticationRealm(Settings.EMPTY, globalSettings);
        User user = realm.lookupUser("user");
        assertThat(user , nullValue());

        realm = new ThrowingAuthenticationRealm(Settings.EMPTY, globalSettings);
        user = realm.lookupUser("user");
        assertThat(user , nullValue());
    }

    @Test
    public void testThatLookupIsNotCalledIfNotSupported() throws Exception {
        LookupNotSupportedRealm realm = new LookupNotSupportedRealm(globalSettings);
        assertThat(realm.userLookupSupported(), is(false));
        User user = realm.lookupUser("a");
        assertThat(user, is(nullValue()));
        assertThat(realm.lookupInvocationCounter.intValue(), is(0));

        // try to lookup more
        realm.lookupUser("b");
        realm.lookupUser("c");

        assertThat(realm.lookupInvocationCounter.intValue(), is(0));
    }

    static class FailingAuthenticationRealm extends CachingUsernamePasswordRealm {

        FailingAuthenticationRealm(Settings settings, Settings global) {
            super("failing", new RealmConfig("failing-test", settings, global));
        }

        @Override
        protected User doAuthenticate(UsernamePasswordToken token) {
            return null;
        }

        @Override
        protected User doLookupUser(String username) {
            return null;
        }

        @Override
        public boolean userLookupSupported() {
            return true;
        }
    }

    static class ThrowingAuthenticationRealm extends CachingUsernamePasswordRealm {

        ThrowingAuthenticationRealm(Settings settings, Settings globalSettings) {
            super("throwing", new RealmConfig("throwing-test", settings, globalSettings));
        }

        @Override
        protected User doAuthenticate(UsernamePasswordToken token) {
            throw new RuntimeException("whatever exception");
        }

        @Override
        protected User doLookupUser(String username) {
            throw new RuntimeException("lookup exception");
        }

        @Override
        public boolean userLookupSupported() {
            return true;
        }
    }

    static class AlwaysAuthenticateCachingRealm extends CachingUsernamePasswordRealm {

        public final AtomicInteger authInvocationCounter = new AtomicInteger(0);
        public final AtomicInteger lookupInvocationCounter = new AtomicInteger(0);

        AlwaysAuthenticateCachingRealm(Settings globalSettings) {
            super("always", new RealmConfig("always-test", Settings.EMPTY, globalSettings));
        }

        @Override
        protected User doAuthenticate(UsernamePasswordToken token) {
            authInvocationCounter.incrementAndGet();
            return new User.Simple(token.principal(), new String[] { "testRole1", "testRole2" });
        }

        @Override
        protected User doLookupUser(String username) {
            lookupInvocationCounter.incrementAndGet();
            return new User.Simple(username, new String[] { "lookupRole1", "lookupRole2" });
        }

        @Override
        public boolean userLookupSupported() {
            return true;
        }
    }

    static class LookupNotSupportedRealm extends CachingUsernamePasswordRealm {

        public final AtomicInteger authInvocationCounter = new AtomicInteger(0);
        public final AtomicInteger lookupInvocationCounter = new AtomicInteger(0);

        LookupNotSupportedRealm(Settings globalSettings) {
            super("lookup", new RealmConfig("lookup-notsupported-test", Settings.EMPTY, globalSettings));
        }

        @Override
        protected User doAuthenticate(UsernamePasswordToken token) {
            authInvocationCounter.incrementAndGet();
            return new User.Simple(token.principal(), new String[] { "testRole1", "testRole2" });
        }

        @Override
        protected User doLookupUser(String username) {
            lookupInvocationCounter.incrementAndGet();
            throw new UnsupportedOperationException("don't call lookup if lookup isn't supported!!!");
        }

        @Override
        public boolean userLookupSupported() {
            return false;
        }
    }
}
