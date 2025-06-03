/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xpack.security.authz.microsoft;

import com.microsoft.graph.models.Group;
import com.microsoft.graph.models.GroupCollectionResponse;
import com.microsoft.graph.models.odataerrors.MainError;
import com.microsoft.graph.models.odataerrors.ODataError;
import com.microsoft.graph.serviceclient.GraphServiceClient;
import com.microsoft.graph.users.UsersRequestBuilder;
import com.microsoft.graph.users.item.UserItemRequestBuilder;
import com.microsoft.graph.users.item.transitivememberof.TransitiveMemberOfRequestBuilder;
import com.microsoft.graph.users.item.transitivememberof.graphgroup.GraphGroupRequestBuilder;
import com.microsoft.kiota.RequestAdapter;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.After;
import org.junit.Before;

import java.util.Arrays;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.elasticsearch.xpack.security.authz.microsoft.MicrosoftGraphAuthzRealm.MICROSOFT_GRAPH_FEATURE;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class MicrosoftGraphAuthzRealmTests extends ESTestCase {

    private final Settings globalSettings = Settings.builder().put("path.home", createTempDir()).build();
    private final Environment env = TestEnvironment.newEnvironment(globalSettings);
    private final ThreadContext threadContext = new ThreadContext(globalSettings);
    private final ThreadPool threadPool = new TestThreadPool(getClass().getName());

    private final String realmName = randomAlphaOfLengthBetween(4, 10);
    private final String roleName = randomAlphaOfLengthBetween(4, 10);
    private final String username = randomAlphaOfLengthBetween(4, 10);
    private final String name = randomAlphaOfLengthBetween(4, 10);
    private final String email = Strings.format("[%s]@example.com", randomAlphaOfLengthBetween(4, 10));
    private final String groupId = randomAlphaOfLengthBetween(4, 10);
    private final RealmConfig.RealmIdentifier realmId = new RealmConfig.RealmIdentifier(
        MicrosoftGraphAuthzRealmSettings.REALM_TYPE,
        realmName
    );

    private final String clientId = randomAlphaOfLengthBetween(4, 10);
    private final String clientSecret = randomAlphaOfLengthBetween(4, 10);
    private final String tenantId = randomAlphaOfLengthBetween(4, 10);

    private static final AuthenticationToken fakeToken = new AuthenticationToken() {
        @Override
        public String principal() {
            fail("Should never be called");
            return null;
        }

        @Override
        public Object credentials() {
            fail("Should never be called");
            return null;
        }

        @Override
        public void clearCredentials() {
            fail("Should never be called");
        }
    };

    @Before
    public void setUp() throws Exception {
        super.setUp();

        final var logger = LogManager.getLogger(MicrosoftGraphAuthzRealm.class);
        Loggers.setLevel(logger, Level.TRACE);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testLookupUser() {
        final var roleMapper = mockRoleMapper(Set.of(groupId), Set.of(roleName));

        final var realmSettings = realmSettings().build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);
        final var client = mock(GraphServiceClient.class);
        when(client.getRequestAdapter()).thenReturn(mock(RequestAdapter.class));

        final var userRequestBuilder = mockGetUser(client);
        when(userRequestBuilder.get(any())).thenReturn(user(name, email));

        final var graphGroupRequestBuilder = mockGetGroupMembership(userRequestBuilder);
        when(graphGroupRequestBuilder.get(any())).thenReturn(groupMembership(groupId));

        final var licenseState = mockLicense(true);

        final var realm = new MicrosoftGraphAuthzRealm(roleMapper, config, client, licenseState, threadPool);
        final var future = new PlainActionFuture<User>();
        realm.lookupUser(username, future);

        try (var mockLog = MockLog.capture(MicrosoftGraphAuthzRealm.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "Fetch user properties",
                    MicrosoftGraphAuthzRealm.class.getName(),
                    Level.TRACE,
                    Strings.format("Fetched user with name [%s] and email [%s] from Microsoft Graph", name, email)
                )
            );

            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "Fetch group membership",
                    MicrosoftGraphAuthzRealm.class.getName(),
                    Level.TRACE,
                    Strings.format("Fetched [1] groups from Microsoft Graph: [%s]", groupId)
                )
            );

            final var user = future.actionGet();
            assertThat(user.principal(), equalTo(username));
            assertThat(user.fullName(), equalTo(name));
            assertThat(user.email(), equalTo(email));
            assertThat(user.roles(), arrayContaining(roleName));

            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testHandleGetUserPropertiesError() {
        final var roleMapper = mockRoleMapper(Set.of(groupId), Set.of(roleName));

        final var realmSettings = realmSettings().build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);
        final var client = mock(GraphServiceClient.class);
        final var requestAdapter = mock(RequestAdapter.class);
        when(client.getRequestAdapter()).thenReturn(requestAdapter);

        final var userItemRequestBuilder = mockGetUser(client);
        when(userItemRequestBuilder.get(any())).thenThrow(graphError("bad stuff happened"));

        final var licenseState = mockLicense(true);

        final var realm = new MicrosoftGraphAuthzRealm(roleMapper, config, client, licenseState, threadPool);
        final var future = new PlainActionFuture<User>();

        try (var mockLog = MockLog.capture(MicrosoftGraphAuthzRealm.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "Log exception",
                    MicrosoftGraphAuthzRealm.class.getName(),
                    Level.ERROR,
                    Strings.format("Failed to authorize [{}] with MS Graph realm", username)
                )
            );

            realm.lookupUser(username, future);
            final var thrown = assertThrows(ODataError.class, future::actionGet);
            assertThat(thrown.getMessage(), equalTo("bad stuff happened"));

            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testHandleGetGroupMembershipError() {
        final var roleMapper = mockRoleMapper(Set.of(groupId), Set.of(roleName));

        final var realmSettings = realmSettings().build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);
        final var client = mock(GraphServiceClient.class);
        when(client.getRequestAdapter()).thenReturn(mock(RequestAdapter.class));

        final var userRequestBuilder = mockGetUser(client);
        when(userRequestBuilder.get(any())).thenReturn(user(name, email));

        final var graphGroupRequestBuilder = mockGetGroupMembership(userRequestBuilder);
        when(graphGroupRequestBuilder.get(any())).thenThrow(graphError("bad stuff happened"));

        final var licenseState = mockLicense(true);

        final var realm = new MicrosoftGraphAuthzRealm(roleMapper, config, client, licenseState, threadPool);
        final var future = new PlainActionFuture<User>();

        try (var mockLog = MockLog.capture(MicrosoftGraphAuthzRealm.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "Log exception",
                    MicrosoftGraphAuthzRealm.class.getName(),
                    Level.ERROR,
                    Strings.format("Failed to authorize [{}] with MS Graph realm", username)
                )
            );

            realm.lookupUser(username, future);
            final var thrown = assertThrows(ODataError.class, future::actionGet);
            assertThat(thrown.getMessage(), equalTo("bad stuff happened"));

            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testGroupMembershipPagination() {
        final var groupId2 = randomAlphaOfLengthBetween(4, 10);
        final var groupId3 = randomAlphaOfLengthBetween(4, 10);

        final var roleMapper = mockRoleMapper(Set.of(groupId, groupId2, groupId3), Set.of(roleName));

        final var realmSettings = realmSettings().build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);
        final var client = mock(GraphServiceClient.class);
        final var requestAdapter = mock(RequestAdapter.class);
        when(client.getRequestAdapter()).thenReturn(requestAdapter);

        final var userItemRequestBuilder = mockGetUser(client);
        when(userItemRequestBuilder.get(any())).thenReturn(user(name, email));

        final var groupMembership1 = groupMembership(groupId);
        groupMembership1.setOdataNextLink("http://localhost:12345/page2");

        final var groupMembership2 = groupMembership(groupId2);
        groupMembership2.setOdataNextLink("http://localhost:12345/page3");

        final var groupMembership3 = groupMembership(groupId3);

        final var graphGroupRequestBuilder = mockGetGroupMembership(userItemRequestBuilder);
        when(graphGroupRequestBuilder.get(any())).thenReturn(groupMembership1);
        when(requestAdapter.send(any(), any(), any())).thenReturn(groupMembership2, groupMembership3);

        final var licenseState = mockLicense(true);

        final var realm = new MicrosoftGraphAuthzRealm(roleMapper, config, client, licenseState, threadPool);
        final var future = new PlainActionFuture<User>();
        realm.lookupUser(username, future);
        final var user = future.actionGet();
        assertThat(user.principal(), equalTo(username));
        assertThat(user.fullName(), equalTo(name));
        assertThat(user.email(), equalTo(email));
        assertThat(user.roles(), arrayContaining(roleName));
    }

    public void testLicenseCheck() {
        final var roleMapper = mock(UserRoleMapper.class);
        final var realmSettings = realmSettings().build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);
        final var client = mock(GraphServiceClient.class);

        final var licenseState = mockLicense(false);

        final var realm = new MicrosoftGraphAuthzRealm(roleMapper, config, client, licenseState, threadPool);
        final var future = new PlainActionFuture<User>();
        realm.lookupUser(username, future);
        final var thrown = assertThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(thrown.getMessage(), equalTo("current license is non-compliant for [microsoft_graph]"));
    }

    public void testClientIdSettingRequired() {
        final var roleMapper = mock(UserRoleMapper.class);
        final var realmSettings = realmSettings().put(
            getFullSettingKey(realmName, MicrosoftGraphAuthzRealmSettings.CLIENT_ID),
            randomBoolean() ? "" : null
        ).build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);
        final var client = mock(GraphServiceClient.class);

        final var licenseState = mockLicense(true);

        final var thrown = assertThrows(
            SettingsException.class,
            () -> new MicrosoftGraphAuthzRealm(roleMapper, config, client, licenseState, threadPool)
        );
        assertThat(
            thrown.getMessage(),
            equalTo(
                Strings.format(
                    "The configuration setting [%s] is required",
                    getFullSettingKey(realmName, MicrosoftGraphAuthzRealmSettings.CLIENT_ID)
                )
            )
        );
    }

    public void testClientSecretSettingRequired() {
        final var roleMapper = mock(UserRoleMapper.class);
        final var secureSettings = new MockSecureSettings();
        if (randomBoolean()) {
            secureSettings.setString(getFullSettingKey(realmName, MicrosoftGraphAuthzRealmSettings.CLIENT_SECRET), "");
        }
        final var realmSettings = Settings.builder()
            .put(globalSettings)
            .put(getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
            .put(getFullSettingKey(realmName, MicrosoftGraphAuthzRealmSettings.CLIENT_ID), clientId)
            .put(getFullSettingKey(realmName, MicrosoftGraphAuthzRealmSettings.TENANT_ID), tenantId)
            .setSecureSettings(secureSettings)
            .build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);
        final var client = mock(GraphServiceClient.class);

        final var licenseState = mockLicense(true);

        final var thrown = assertThrows(
            SettingsException.class,
            () -> new MicrosoftGraphAuthzRealm(roleMapper, config, client, licenseState, threadPool)
        );
        assertThat(
            thrown.getMessage(),
            equalTo(
                Strings.format(
                    "The configuration setting [%s] is required",
                    getFullSettingKey(realmName, MicrosoftGraphAuthzRealmSettings.CLIENT_SECRET)
                )
            )
        );
    }

    public void testTenantIdSettingRequired() {
        final var roleMapper = mock(UserRoleMapper.class);
        final var realmSettings = realmSettings().put(
            getFullSettingKey(realmName, MicrosoftGraphAuthzRealmSettings.TENANT_ID),
            randomBoolean() ? "" : null
        ).build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);
        final var client = mock(GraphServiceClient.class);

        final var licenseState = mockLicense(true);

        final var thrown = assertThrows(
            SettingsException.class,
            () -> new MicrosoftGraphAuthzRealm(roleMapper, config, client, licenseState, threadPool)
        );
        assertThat(
            thrown.getMessage(),
            equalTo(
                Strings.format(
                    "The configuration setting [%s] is required",
                    getFullSettingKey(realmName, MicrosoftGraphAuthzRealmSettings.TENANT_ID)
                )
            )
        );
    }

    public void testSupportsAlwaysReturnsFalse() {
        final var roleMapper = mock(UserRoleMapper.class);
        final var realmSettings = realmSettings().build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);
        final var client = mock(GraphServiceClient.class);

        final var licenseState = mockLicense(true);

        final var realm = new MicrosoftGraphAuthzRealm(roleMapper, config, client, licenseState, threadPool);

        assertThat(realm.supports(fakeToken), equalTo(false));
    }

    public void testTokenAlwaysReturnsNull() {
        final var roleMapper = mock(UserRoleMapper.class);
        final var realmSettings = realmSettings().build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);
        final var client = mock(GraphServiceClient.class);

        final var licenseState = mockLicense(true);

        final var realm = new MicrosoftGraphAuthzRealm(roleMapper, config, client, licenseState, threadPool);
        assertThat(realm.token(threadContext), equalTo(null));
    }

    public void testAuthenticateAlwaysReturnsNotHandled() {
        final var roleMapper = mock(UserRoleMapper.class);
        final var realmSettings = realmSettings().build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);
        final var client = mock(GraphServiceClient.class);

        final var licenseState = mockLicense(true);

        final var realm = new MicrosoftGraphAuthzRealm(roleMapper, config, client, licenseState, threadPool);
        final var future = new PlainActionFuture<AuthenticationResult<User>>();
        realm.authenticate(fakeToken, future);
        final var result = future.actionGet();
        assertThat(result, equalTo(AuthenticationResult.notHandled()));
    }

    private UserRoleMapper mockRoleMapper(Set<String> expectedGroups, Set<String> rolesToReturn) {
        final var roleMapper = mock(UserRoleMapper.class);
        doAnswer(invocation -> {
            var userData = (UserRoleMapper.UserData) invocation.getArguments()[0];
            assertEquals(userData.getGroups(), expectedGroups);
            @SuppressWarnings("unchecked")
            var listener = (ActionListener<Set<String>>) invocation.getArguments()[1];
            listener.onResponse(rolesToReturn);
            return null;
        }).when(roleMapper).resolveRoles(any(), any());

        return roleMapper;
    }

    private Settings.Builder realmSettings() {
        final var secureSettings = new MockSecureSettings();
        secureSettings.setString(getFullSettingKey(realmName, MicrosoftGraphAuthzRealmSettings.CLIENT_SECRET), clientSecret);

        return Settings.builder()
            .put(globalSettings)
            .put(getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
            .put(getFullSettingKey(realmName, MicrosoftGraphAuthzRealmSettings.CLIENT_ID), clientId)
            .put(getFullSettingKey(realmName, MicrosoftGraphAuthzRealmSettings.TENANT_ID), tenantId)
            .setSecureSettings(secureSettings);
    }

    private XPackLicenseState mockLicense(boolean msGraphAllowed) {
        final var licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(eq(MICROSOFT_GRAPH_FEATURE))).thenReturn(msGraphAllowed);
        return licenseState;
    }

    private UserItemRequestBuilder mockGetUser(GraphServiceClient client) {
        final var userRequestBuilder = mock(UsersRequestBuilder.class);
        final var userItemRequestBuilder = mock(UserItemRequestBuilder.class);

        when(client.users()).thenReturn(userRequestBuilder);
        when(userRequestBuilder.byUserId(eq(username))).thenReturn(userItemRequestBuilder);

        return userItemRequestBuilder;
    }

    private GraphGroupRequestBuilder mockGetGroupMembership(UserItemRequestBuilder userItemRequestBuilder) {
        final var memberOfRequestBuilder = mock(TransitiveMemberOfRequestBuilder.class);
        final var graphGroupRequestBuilder = mock(GraphGroupRequestBuilder.class);

        when(userItemRequestBuilder.transitiveMemberOf()).thenReturn(memberOfRequestBuilder);
        when(memberOfRequestBuilder.graphGroup()).thenReturn(graphGroupRequestBuilder);

        return graphGroupRequestBuilder;
    }

    private com.microsoft.graph.models.User user(String name, String email) {
        final var msUser = new com.microsoft.graph.models.User();
        msUser.setDisplayName(name);
        msUser.setMail(email);

        return msUser;
    }

    private GroupCollectionResponse groupMembership(String... groupIds) {
        final var groupMembership = new GroupCollectionResponse();
        groupMembership.setValue(Arrays.stream(groupIds).map(id -> {
            var group = new Group();
            group.setId(id);
            return group;
        }).toList());
        return groupMembership;
    }

    private ODataError graphError(String message) {
        final var error = new MainError();
        error.setCode("badRequest");
        error.setMessage(message);

        final var graphError = new ODataError();
        graphError.setError(error);

        return graphError;
    }
}
