/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.authc;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.After;
import org.junit.Before;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class LookupRealmsAwareRealmTests extends ESTestCase {
    private ThreadPool threadPool;
    private Realm realm;

    @Before
    public void setup() throws Exception {
        threadPool = new TestThreadPool("lookup realms aware tests");
        final RealmConfig realmConfig = mock(RealmConfig.class);
        when(realmConfig.threadContext()).thenReturn(threadPool.getThreadContext());
        realm = new DummyRealm(realmConfig);
    }

    @After
    public void cleanup() throws InterruptedException {
        terminate(threadPool);
    }

    public void testLookupUser() {
        final PlainActionFuture<User> future = new PlainActionFuture<>();
        realm.lookupUser(randomFrom("user1", "user2"), future);
        assertThat(future.actionGet(), is(nullValue()));
    }

    public void testLookupUserUsingLookupRealms() {
        final User user1 = new User("user1", "r1", "r2");
        final User user2 = new User("user2", "r1");
        final Realm lookupRealm1 = createDummyRealm("lookup-realm-1", user1, 0);
        final Realm lookupRealm2 = createDummyRealm("lookup-realm-2", user2, 1);
        final List<Realm> lookupRealms = Arrays.asList(lookupRealm1, lookupRealm2);
        final List<String> realmNames = lookupRealms.stream().map((realm) -> realm.name()).collect(Collectors.toList());
        when(realm.config.lookupRealms()).thenReturn(realmNames);
        realm.setLookupRealms(lookupRealms);

        final PlainActionFuture<User> future = new PlainActionFuture<>();
        final User usr = randomFrom(user1, user2);
        realm.lookupUser(usr.principal(), future);
        assertThat(future.actionGet(), is(notNullValue()));
        assertThat(future.actionGet(), is(sameInstance(usr)));
    }

    public void testLookupUserUsingLookupRealmsInOrderOfLookupRealms() {
        final User user1 = new User("user1", "r1", "r2");
        final User user2 = new User("user1", "r1");
        final Realm lookupRealm1 = createDummyRealm("lookup-realm-1", user1, 1);
        final Realm lookupRealm2 = createDummyRealm("lookup-realm-2", user2, 0);
        Realm[] realms = new Realm[] { lookupRealm1, lookupRealm2 };
        Arrays.sort(realms);
        final List<Realm> lookupRealms = Arrays.asList(realms);
        final List<String> realmNames = lookupRealms.stream().map((realm) -> realm.name()).collect(Collectors.toList());
        when(realm.config.lookupRealms()).thenReturn(realmNames);
        realm.setLookupRealms(lookupRealms);
        final PlainActionFuture<User> future = new PlainActionFuture<>();

        realm.lookupUser("user1", future);

        assertThat(future.actionGet(), is(notNullValue()));
        assertThat(future.actionGet(), is(sameInstance(user2)));
    }

    public void testSetLookupRealmsWithNullOrEmptyLookupRealmsButConfigSpecifiesLookupRealmDepenency() {
        final List<Realm> lookupRealms = randomBoolean() ? null : Collections.emptyList();
        when(realm.config.lookupRealms()).thenReturn(Arrays.asList(new String[] { randomAlphaOfLength(5) }));
        assertThat(realm.hasLookupDependency(), is(true));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> realm.setLookupRealms(lookupRealms));
        assertThat(e.getMessage(),
                is(equalTo("list of injected lookup realms is null or empty whereas realm config has lookup realms dependencies as : "
                        + realm.config.lookupRealms())));
    }

    public void testLookupUserUsingNullOrEmptyLookupRealms() {
        final List<Realm> lookupRealms = randomBoolean() ? null : Collections.emptyList();
        realm.setLookupRealms(lookupRealms);
        final PlainActionFuture<User> future = new PlainActionFuture<>();

        realm.lookupUser("user1", future);

        assertThat(future.actionGet(), is(nullValue()));
    }

    @SuppressWarnings("unchecked")
    private Realm createDummyRealm(final String realmName, final User user, int order) {
        final RealmConfig realmConfig = mock(RealmConfig.class);
        when(realmConfig.name()).thenReturn(realmName);
        when(realmConfig.enabled()).thenReturn(true);
        when(realmConfig.order()).thenReturn(order);
        final Realm lookupRealm = spy(new DummyRealm(realmConfig));
        Mockito.doAnswer(invocation -> {
            final String username = (String) invocation.getArguments()[0];
            final ActionListener<User> listener = (ActionListener<User>) invocation.getArguments()[1];
            if (username.equals(user.principal())) {
                listener.onResponse(user);
            } else {
                listener.onResponse(null);
            }
            return null;
        }).when(lookupRealm).lookupUser(any(String.class), any(ActionListener.class));
        return lookupRealm;
    }

    private static class DummyRealm extends Realm {
        DummyRealm(RealmConfig realmConfig) {
            super("type-2", realmConfig);
        }

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
        }
    }
}