/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.support;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.Realm;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.elasticsearch.common.Strings.collectionToDelimitedString;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DelegatedAuthorizationSupportTests extends ESTestCase {

    private List<MockLookupRealm> realms;
    private Settings globalSettings;
    private ThreadContext threadContext;
    private Environment env;

    @Before
    public void setupRealms() {
        globalSettings = Settings.builder()
            .put("path.home", createTempDir())
            .build();
        env = TestEnvironment.newEnvironment(globalSettings);
        threadContext = new ThreadContext(globalSettings);

        final int realmCount = randomIntBetween(5, 9);
        realms = new ArrayList<>(realmCount);
        for (int i = 1; i <= realmCount; i++) {
            realms.add(new MockLookupRealm(buildRealmConfig("lookup-" + i, Settings.EMPTY)));
        }
        shuffle(realms);
    }

    private <T> List<T> shuffle(List<T> list) {
        Collections.shuffle(list, random());
        return list;
    }

    private RealmConfig buildRealmConfig(String name, Settings settings) {
        RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("test", name);
        return new RealmConfig(
            realmIdentifier,
            Settings.builder().put(settings)
                .normalizePrefix("xpack.security.authc.realms.test." + name + ".")
                .put(globalSettings)
                .put(RealmSettings.getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 0)
                .build(),
            env, threadContext);
    }

    public void testEmptyDelegationList() throws ExecutionException, InterruptedException {
        final XPackLicenseState license = getLicenseState(true);
        final DelegatedAuthorizationSupport das = new DelegatedAuthorizationSupport(realms, buildRealmConfig("r", Settings.EMPTY), license);
        assertThat(das.hasDelegation(), equalTo(false));
        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        das.resolve("any", future);
        final AuthenticationResult result = future.get();
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
        assertThat(result.getUser(), nullValue());
        assertThat(result.getMessage(), equalTo("No [authorization_realms] have been configured"));
    }

    public void testMissingRealmInDelegationList() {
        final XPackLicenseState license = getLicenseState(true);
        final Settings settings = Settings.builder()
            .putList("authorization_realms", "no-such-realm")
            .build();
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () ->
            new DelegatedAuthorizationSupport(realms, buildRealmConfig("r", settings), license)
        );
        assertThat(ex.getMessage(), equalTo("configured authorization realm [no-such-realm] does not exist (or is not enabled)"));
    }

    public void testDelegationChainsAreRejected() {
        final XPackLicenseState license = getLicenseState(true);
        final Settings settings = Settings.builder()
            .putList("authorization_realms", "lookup-1", "lookup-2", "lookup-3")
            .build();
        globalSettings = Settings.builder()
            .put(globalSettings)
            .putList("xpack.security.authc.realms.test.lookup-2.authorization_realms", "lookup-1")
            .build();
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () ->
            new DelegatedAuthorizationSupport(realms, buildRealmConfig("realm1", settings), license)
        );
        assertThat(ex.getMessage(),
            equalTo("cannot use realm [test/lookup-2] as an authorization realm - it is already delegating authorization to [[lookup-1]]"));
    }

    public void testMatchInDelegationList() throws Exception {
        final XPackLicenseState license = getLicenseState(true);
        final List<MockLookupRealm> useRealms = shuffle(randomSubsetOf(randomIntBetween(1, realms.size()), realms));
        final Settings settings = Settings.builder()
            .putList("authorization_realms", useRealms.stream().map(Realm::name).collect(Collectors.toList()))
            .build();
        final User user = new User("my_user");
        randomFrom(useRealms).registerUser(user);
        final DelegatedAuthorizationSupport das = new DelegatedAuthorizationSupport(realms, buildRealmConfig("r", settings), license);
        assertThat(das.hasDelegation(), equalTo(true));
        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        das.resolve("my_user", future);
        final AuthenticationResult result = future.get();
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getUser(), sameInstance(user));
    }

    public void testRealmsAreOrdered() throws Exception {
        final XPackLicenseState license = getLicenseState(true);
        final List<MockLookupRealm> useRealms = shuffle(randomSubsetOf(randomIntBetween(3, realms.size()), realms));
        final List<String> names = useRealms.stream().map(Realm::name).collect(Collectors.toList());
        final Settings settings = Settings.builder()
            .putList("authorization_realms", names)
            .build();
        final List<User> users = new ArrayList<>(names.size());
        final String username = randomAlphaOfLength(8);
        for (MockLookupRealm r : useRealms) {
            final User user = new User(username, "role_" + r.name());
            users.add(user);
            r.registerUser(user);
        }

        final DelegatedAuthorizationSupport das = new DelegatedAuthorizationSupport(realms, buildRealmConfig("r", settings), license);
        assertThat(das.hasDelegation(), equalTo(true));
        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        das.resolve(username, future);
        final AuthenticationResult result = future.get();
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        assertThat(result.getUser(), sameInstance(users.get(0)));
        assertThat(result.getUser().roles(), arrayContaining("role_" + useRealms.get(0).name()));
    }

    public void testNoMatchInDelegationList() throws Exception {
        final XPackLicenseState license = getLicenseState(true);
        final List<MockLookupRealm> useRealms = shuffle(randomSubsetOf(randomIntBetween(1, realms.size()), realms));
        final Settings settings = Settings.builder()
            .putList("authorization_realms", useRealms.stream().map(Realm::name).collect(Collectors.toList()))
            .build();
        final DelegatedAuthorizationSupport das = new DelegatedAuthorizationSupport(realms, buildRealmConfig("r", settings), license);
        assertThat(das.hasDelegation(), equalTo(true));
        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        das.resolve("my_user", future);
        final AuthenticationResult result = future.get();
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
        assertThat(result.getUser(), nullValue());
        assertThat(result.getMessage(), equalTo("the principal [my_user] was authenticated, but no user could be found in realms [" +
            collectionToDelimitedString(useRealms.stream().map(Realm::toString).collect(Collectors.toList()), ",") + "]"));
    }

    public void testLicenseRejection() throws Exception {
        final XPackLicenseState license = getLicenseState(false);
        final Settings settings = Settings.builder()
            .putList("authorization_realms", realms.get(0).name())
            .build();
        final DelegatedAuthorizationSupport das = new DelegatedAuthorizationSupport(realms, buildRealmConfig("r", settings), license);
        assertThat(das.hasDelegation(), equalTo(true));
        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        das.resolve("my_user", future);
        final AuthenticationResult result = future.get();
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
        assertThat(result.getUser(), nullValue());
        assertThat(result.getMessage(), equalTo("authorization_realms are not permitted"));
        assertThat(result.getException(), instanceOf(ElasticsearchSecurityException.class));
        assertThat(result.getException().getMessage(), equalTo("current license is non-compliant for [authorization_realms]"));
    }

    private XPackLicenseState getLicenseState(boolean authzRealmsAllowed) {
        final XPackLicenseState license = mock(XPackLicenseState.class);
        when(license.checkFeature(Feature.SECURITY_AUTHORIZATION_REALM)).thenReturn(authzRealmsAllowed);
        return license;
    }
}
