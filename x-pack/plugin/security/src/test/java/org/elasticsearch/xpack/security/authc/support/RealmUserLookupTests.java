/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmConfig.RealmIdentifier;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class RealmUserLookupTests extends ESTestCase {

    private Settings globalSettings;
    private ThreadContext threadContext;
    private Environment env;

    @Before
    public void setup() {
        globalSettings = Settings.builder()
            .put("path.home", createTempDir())
            .build();
        env = TestEnvironment.newEnvironment(globalSettings);
        threadContext = new ThreadContext(globalSettings);
    }

    public void testNoRealms() throws Exception {
        final RealmUserLookup lookup = new RealmUserLookup(Collections.emptyList(), threadContext);
        final PlainActionFuture<Tuple<User, Realm>> listener = new PlainActionFuture<>();
        lookup.lookup(randomAlphaOfLengthBetween(3, 12), listener);
        final Tuple<User, Realm> tuple = listener.get();
        assertThat(tuple, nullValue());
    }

    public void testUserFound() throws Exception {
        final List<MockLookupRealm> realms = buildRealms(randomIntBetween(5, 9));
        final RealmUserLookup lookup = new RealmUserLookup(realms, threadContext);

        final MockLookupRealm matchRealm = randomFrom(realms);
        final User user = new User(randomAlphaOfLength(5));
        matchRealm.registerUser(user);

        final PlainActionFuture<Tuple<User, Realm>> listener = new PlainActionFuture<>();
        lookup.lookup(user.principal(), listener);
        final Tuple<User, Realm> tuple = listener.get();
        assertThat(tuple, notNullValue());
        assertThat(tuple.v1(), notNullValue());
        assertThat(tuple.v1(), sameInstance(user));
        assertThat(tuple.v2(), notNullValue());
        assertThat(tuple.v2(), sameInstance(matchRealm));
    }

    public void testUserNotFound() throws Exception {
        final List<MockLookupRealm> realms = buildRealms(randomIntBetween(5, 9));
        final RealmUserLookup lookup = new RealmUserLookup(realms, threadContext);

        final String username = randomAlphaOfLength(5);

        final PlainActionFuture<Tuple<User, Realm>> listener = new PlainActionFuture<>();
        lookup.lookup(username, listener);
        final Tuple<User, Realm> tuple = listener.get();
        assertThat(tuple, nullValue());
    }

    public void testRealmException() {
        RealmIdentifier realmIdentifier = new RealmIdentifier("test", "test");
        final Realm realm = new Realm(new RealmConfig(realmIdentifier,
            Settings.builder().put(globalSettings)
                .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
            env, threadContext)) {
            @Override
            public boolean supports(AuthenticationToken token) {
                return false;
            }

            @Override
            public AuthenticationToken token(ThreadContext context) {
                return null;
            }

            @Override
            public void authenticate(AuthenticationToken token, ActionListener<AuthenticationResult> listener) {
                listener.onResponse(AuthenticationResult.notHandled());
            }

            @Override
            public void lookupUser(String username, ActionListener<User> listener) {
                listener.onFailure(new RuntimeException("FAILURE"));
            }
        };
        final RealmUserLookup lookup = new RealmUserLookup(Collections.singletonList(realm), threadContext);
        final PlainActionFuture<Tuple<User, Realm>> listener = new PlainActionFuture<>();
        lookup.lookup("anyone", listener);
        final RuntimeException e = expectThrows(RuntimeException.class, listener::actionGet);
        assertThat(e.getMessage(), equalTo("FAILURE"));
    }

    private List<MockLookupRealm> buildRealms(int realmCount) {
        final List<MockLookupRealm> realms = new ArrayList<>(realmCount);
        for (int i = 1; i <= realmCount; i++) {
            RealmIdentifier realmIdentifier = new RealmIdentifier("mock", "lookup-" + i);
            final RealmConfig config = new RealmConfig(realmIdentifier,
                Settings.builder().put(globalSettings)
                    .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0).build(),
                env,
                threadContext);
            final MockLookupRealm realm = new MockLookupRealm(config);
            for (int j = 0; j < 5; j++) {
                realm.registerUser(new User(randomAlphaOfLengthBetween(6, 12)));
            }
            realms.add(realm);
        }
        Collections.shuffle(realms, random());
        return realms;
    }
}
