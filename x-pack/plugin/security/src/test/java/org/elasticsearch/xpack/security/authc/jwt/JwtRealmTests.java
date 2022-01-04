/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.jwt;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.security.Security;
import org.junit.After;
import org.junit.Before;

import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class JwtRealmTests extends JwtTestCase {

    private ThreadPool threadPool;
    private ResourceWatcherService resourceWatcherService;
    private Settings defaultGlobalSettings;
    private SSLService sslService;
    private MockLicenseState licenseState;

    @Before
    public void init() throws Exception {
        this.threadPool = new TestThreadPool("JWT realm tests");
        this.resourceWatcherService = new ResourceWatcherService(Settings.EMPTY, this.threadPool);
        this.defaultGlobalSettings = Settings.builder().put("path.home", createTempDir()).build();
        this.sslService = new SSLService(TestEnvironment.newEnvironment(this.defaultGlobalSettings));
        this.licenseState = mock(MockLicenseState.class);
        when(this.licenseState.isAllowed(Security.DELEGATED_AUTHORIZATION_FEATURE)).thenReturn(true);
    }

    @After
    public void shutdown() throws InterruptedException {
        this.resourceWatcherService.close();
        terminate(this.threadPool);
    }

    public void testRealm() throws Exception {
        final RealmConfig realmConfig = super.buildRealmConfig(super.getAllRealmSettings().build());
        final UserRoleMapper userRoleMapper = super.buildRoleMapper("principal1", Set.of("role1, role2"));
        final JwtRealm jwtReam = new JwtRealm(realmConfig, threadPool, sslService, userRoleMapper, resourceWatcherService);

    }

    // public void testClaimPropertyMapping() throws Exception {
    // final String principal = randomAlphaOfLength(12);
    // final UserRoleMapper roleMapper = mock(UserRoleMapper.class);
    // final boolean populateMetadata = randomBoolean();
    // final boolean useAuthorizingRealm = false;
    // final AtomicReference<UserRoleMapper.UserData> userData = new AtomicReference<>();
    // doAnswer(this.getAnswer(userData)).when(roleMapper).resolveRoles(any(UserRoleMapper.UserData.class), anyActionListener());
    // final Exception e1 = expectThrows(
    // Exception.class,
    // () -> authenticateWithJwt(principal, roleMapper, populateMetadata, useAuthorizingRealm, REALM_NAME)
    // );
    // System.out.println(e1);
    // assertThat(e1.getCause().getMessage(), containsString("expects a claim with String or a String Array value"));
    // final Exception e2 = expectThrows(
    // Exception.class,
    // () -> authenticateWithJwt(principal, roleMapper, populateMetadata, useAuthorizingRealm, REALM_NAME)
    // );
    // System.out.println(e2);
    // assertThat(e2.getCause().getMessage(), containsString("expects a claim with String or a String Array value"));
    // }
    //
    // private AuthenticationResult<User> authenticateWithJwt(
    // final String principal,
    // final UserRoleMapper roleMapper,
    // final boolean populateMetadata,
    // final boolean useAuthorizingRealm,
    // final String authenticatingRealm
    // ) throws Exception {
    // // build DelegatedAuthorization realm (if useAuthorizingRealm is true)
    // final MockLookupRealm delegatedAuthorizationRealm = createDelegatedAuthorizationRealm(principal, useAuthorizingRealm);
    //
    // // update JWT realm settings based on populateMetadata and useAuthorizingRealm
    // final Settings.Builder jwtRealmSettingsBuilder = super.getBasicRealmSettings();
    // if (delegatedAuthorizationRealm != null) {
    // jwtRealmSettingsBuilder.putList(
    // getFullSettingKey(new RealmConfig.RealmIdentifier("jwt", REALM_NAME), DelegatedAuthorizationSettings.AUTHZ_REALMS),
    // delegatedAuthorizationRealm.name()
    // );
    // }
    // jwtRealmSettingsBuilder.put(getFullSettingKey(REALM_NAME, JwtRealmSettings.POPULATE_USER_METADATA), populateMetadata);
    //
    // // build JWT realm
    // final RealmConfig jwtRealmConfig = buildConfig(jwtRealmSettingsBuilder.build());
    // final JwtRealm jwtRealm = new JwtRealm(jwtRealmConfig, null, null, roleMapper, null);
    //
    // // initialize
    // this.initializeRealms(jwtRealm, delegatedAuthorizationRealm);
    // final JwtAuthenticationToken token = new JwtAuthenticationToken(
    // new SecureString("".toCharArray()),null, null);
    //
    // final JWTClaimsSet claims = new JWTClaimsSet.Builder().subject(principal)
    // .audience("https://rp.elastic.co/cb")
    // .expirationTime(Date.from(now().plusSeconds(3600)))
    // .issueTime(Date.from(now().minusSeconds(5)))
    // .jwtID(randomAlphaOfLength(8))
    // .issuer("https://op.company.org")
    // .claim("groups", Arrays.asList("group1", "group2", "groups3"))
    // .claim("mail", "cbarton@shield.gov")
    // .claim("name", "Clinton Barton")
    // .claim("id_token_hint", "thisis.aserialized.jwt")
    // .build();
    //
    // // mock the JWT authenticator response
    // final JwtAuthenticator authenticator = mock(JwtAuthenticator.class);
    // doAnswer((i) -> {
    // @SuppressWarnings("unchecked")
    // ActionListener<JWTClaimsSet> listener = (ActionListener<JWTClaimsSet>) i.getArguments()[1];
    // listener.onResponse(claims);
    // return null;
    // }).when(authenticator).authenticate(this.threadContext, anyActionListener());
    //
    // final PlainActionFuture<AuthenticationResult<User>> future = new PlainActionFuture<>();
    // jwtRealm.authenticate(token, future);
    // final AuthenticationResult<User> userAuthenticationResult = future.get();
    //
    // return userAuthenticationResult;
    // }
    //
    // private MockLookupRealm createDelegatedAuthorizationRealm(final String principal, final boolean useAuthorizingRealm) {
    // if (useAuthorizingRealm == false) {
    // return null;
    // }
    // final MockLookupRealm delegatedAuthorizationRealm;
    // final RealmConfig.RealmIdentifier realmIdentifier = new RealmConfig.RealmIdentifier("mock", "mock_lookup");
    // delegatedAuthorizationRealm = new MockLookupRealm(
    // new RealmConfig(
    // realmIdentifier,
    // Settings.builder().put(this.globalSettings).put(getFullSettingKey(realmIdentifier, RealmSettings.ORDER_SETTING), 1)
    // .build(),
    // this.env,
    // this.threadContext
    // )
    // );
    // delegatedAuthorizationRealm.registerUser(
    // new User(
    // principal,
    // new String[] { "lookup_user_role" },
    // "Clinton Barton",
    // "cbarton@shield.gov",
    // Collections.singletonMap("is_lookup", true),
    // true
    // )
    // );
    // return delegatedAuthorizationRealm;
    // }
    //
    // private void initializeRealms(Realm... realms) {
    // final MockLicenseState licenseState = mock(MockLicenseState.class);
    // when(licenseState.isAllowed(Security.DELEGATED_AUTHORIZATION_FEATURE)).thenReturn(true);
    // final List<Realm> realmList = Arrays.asList(realms);
    // for (final Realm realm : realmList) {
    // realm.initialize(realmList, licenseState);
    // }
    // }
    //
    // private Answer<Class<Void>> getAnswer(AtomicReference<UserRoleMapper.UserData> userData) {
    // return invocation -> {
    // assert invocation.getArguments().length == 2;
    // userData.set((UserRoleMapper.UserData) invocation.getArguments()[0]);
    // @SuppressWarnings("unchecked") ActionListener<Set<String>> listener =
    // (ActionListener<Set<String>>) invocation.getArguments()[1];
    // listener.onResponse(new HashSet<>(Arrays.asList("kibana_user", "role1")));
    // return null;
    // };
    // }
}
