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
import com.microsoft.graph.users.item.transitivememberof.graphgroup.GraphGroupRequestBuilder;
import com.microsoft.graph.users.item.transitivememberof.TransitiveMemberOfRequestBuilder;
import com.microsoft.kiota.RequestAdapter;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.RealmConfig;
import org.elasticsearch.xpack.core.security.authc.RealmSettings;
import org.elasticsearch.xpack.core.security.authc.support.UserRoleMapper;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.After;

import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authc.RealmSettings.getFullSettingKey;
import static org.elasticsearch.xpack.security.authz.microsoft.MicrosoftGraphAuthzRealm.MICROSOFT_GRAPH_FEATURE;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doAnswer;

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

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testLookupUser() {
        final var roleMapper = mockRoleMapper(Set.of(groupId), Set.of(roleName));

        final var realmSettings = Settings.builder()
            .put(globalSettings)
            .put(getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
            .build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);
        final var client = mock(GraphServiceClient.class);
        final var requestAdapter = mock(RequestAdapter.class);
        when(client.getRequestAdapter()).thenReturn(requestAdapter);

        final var userRequestBuilder = mock(UsersRequestBuilder.class);
        final var userItemRequestBuilder = mock(UserItemRequestBuilder.class);
        final var msUser = new com.microsoft.graph.models.User();
        msUser.setDisplayName(name);
        msUser.setMail(email);

        when(client.users()).thenReturn(userRequestBuilder);
        when(userRequestBuilder.byUserId(eq(username))).thenReturn(userItemRequestBuilder);
        when(userItemRequestBuilder.get(any())).thenReturn(msUser);

        final var memberOfRequestBuilder = mock(TransitiveMemberOfRequestBuilder.class);
        final var graphGroupRequestBuilder = mock(GraphGroupRequestBuilder.class);
        final var group = new Group();
        group.setId(groupId);
        final var groupMembership = new GroupCollectionResponse();
        groupMembership.setValue(List.of(group));

        when(userItemRequestBuilder.transitiveMemberOf()).thenReturn(memberOfRequestBuilder);
        when(memberOfRequestBuilder.graphGroup()).thenReturn(graphGroupRequestBuilder);
        when(graphGroupRequestBuilder.get(any())).thenReturn(groupMembership);

        final var licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(eq(MICROSOFT_GRAPH_FEATURE))).thenReturn(true);

        final var realm = new MicrosoftGraphAuthzRealm(roleMapper, config, client, licenseState, threadPool);
        final var future = new PlainActionFuture<User>();
        realm.lookupUser(username, future);
        final var user = future.actionGet();
        assertThat(user.principal(), equalTo(username));
        assertThat(user.fullName(), equalTo(name));
        assertThat(user.email(), equalTo(email));
        assertThat(user.roles(), arrayContaining(roleName));
    }

    public void testHandleGetUserPropertiesError() {
        final var roleMapper = mockRoleMapper(Set.of(groupId), Set.of(roleName));

        final var realmSettings = Settings.builder()
            .put(globalSettings)
            .put(getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
            .build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);
        final var client = mock(GraphServiceClient.class);
        final var requestAdapter = mock(RequestAdapter.class);
        when(client.getRequestAdapter()).thenReturn(requestAdapter);

        final var userRequestBuilder = mock(UsersRequestBuilder.class);
        final var userItemRequestBuilder = mock(UserItemRequestBuilder.class);
        final var graphError = new ODataError();
        final var error = new MainError();
        error.setCode("badRequest");
        error.setMessage("bad stuff happened");
        graphError.setError(error);

        when(client.users()).thenReturn(userRequestBuilder);
        when(userRequestBuilder.byUserId(eq(username))).thenReturn(userItemRequestBuilder);
        when(userItemRequestBuilder.get(any())).thenThrow(graphError);

        final var licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(eq(MICROSOFT_GRAPH_FEATURE))).thenReturn(true);

        final var realm = new MicrosoftGraphAuthzRealm(roleMapper, config, client, licenseState, threadPool);
        final var future = new PlainActionFuture<User>();
        realm.lookupUser(username, future);
        final var thrown = assertThrows(ODataError.class, future::actionGet);
        assertThat(thrown.getMessage(), equalTo("bad stuff happened"));
    }

    public void testHandleGetGroupMembershipError() {
        final var roleMapper = mockRoleMapper(Set.of(groupId), Set.of(roleName));

        final var realmSettings = Settings.builder()
            .put(globalSettings)
            .put(getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
            .build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);
        final var client = mock(GraphServiceClient.class);
        final var requestAdapter = mock(RequestAdapter.class);
        when(client.getRequestAdapter()).thenReturn(requestAdapter);

        final var userRequestBuilder = mock(UsersRequestBuilder.class);
        final var userItemRequestBuilder = mock(UserItemRequestBuilder.class);
        final var msUser = new com.microsoft.graph.models.User();
        msUser.setDisplayName(name);
        msUser.setMail(email);

        when(client.users()).thenReturn(userRequestBuilder);
        when(userRequestBuilder.byUserId(eq(username))).thenReturn(userItemRequestBuilder);
        when(userItemRequestBuilder.get(any())).thenReturn(msUser);

        final var memberOfRequestBuilder = mock(TransitiveMemberOfRequestBuilder.class);
        final var graphGroupRequestBuilder = mock(GraphGroupRequestBuilder.class);
        final var graphError = new ODataError();
        final var error = new MainError();
        error.setCode("badRequest");
        error.setMessage("bad stuff happened");
        graphError.setError(error);

        when(userItemRequestBuilder.transitiveMemberOf()).thenReturn(memberOfRequestBuilder);
        when(memberOfRequestBuilder.graphGroup()).thenReturn(graphGroupRequestBuilder);
        when(graphGroupRequestBuilder.get(any())).thenThrow(graphError);

        final var licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(eq(MICROSOFT_GRAPH_FEATURE))).thenReturn(true);

        final var realm = new MicrosoftGraphAuthzRealm(roleMapper, config, client, licenseState, threadPool);
        final var future = new PlainActionFuture<User>();
        realm.lookupUser(username, future);
        final var thrown = assertThrows(ODataError.class, future::actionGet);
        assertThat(thrown.getMessage(), equalTo("bad stuff happened"));
    }

    public void testGroupMembershipPagination() {
        final var groupId2 = randomAlphaOfLengthBetween(4, 10);
        final var groupId3 = randomAlphaOfLengthBetween(4, 10);

        final var roleMapper = mockRoleMapper(Set.of(groupId, groupId2, groupId3), Set.of(roleName));

        final var realmSettings = Settings.builder()
            .put(globalSettings)
            .put(getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
            .build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);
        final var client = mock(GraphServiceClient.class);
        final var requestAdapter = mock(RequestAdapter.class);
        when(client.getRequestAdapter()).thenReturn(requestAdapter);

        final var userRequestBuilder = mock(UsersRequestBuilder.class);
        final var userItemRequestBuilder = mock(UserItemRequestBuilder.class);
        final var msUser = new com.microsoft.graph.models.User();
        msUser.setDisplayName(name);
        msUser.setMail(email);

        when(client.users()).thenReturn(userRequestBuilder);
        when(userRequestBuilder.byUserId(eq(username))).thenReturn(userItemRequestBuilder);
        when(userItemRequestBuilder.get(any())).thenReturn(msUser);

        final var memberOfRequestBuilder = mock(TransitiveMemberOfRequestBuilder.class);
        final var graphGroupRequestBuilder = mock(GraphGroupRequestBuilder.class);
        final var group1 = new Group();
        group1.setId(groupId);
        final var groupMembership1 = new GroupCollectionResponse();
        groupMembership1.setValue(List.of(group1));
        groupMembership1.setOdataNextLink("http://localhost:12345/page2");

        final var group2 = new Group();
        group2.setId(groupId2);
        final var groupMembership2 = new GroupCollectionResponse();
        groupMembership2.setValue(List.of(group2));
        groupMembership2.setOdataNextLink("http://localhost:12345/page3");

        final var group3 = new Group();
        group3.setId(groupId3);
        final var groupMembership3 = new GroupCollectionResponse();
        groupMembership3.setValue(List.of(group3));

        when(userItemRequestBuilder.transitiveMemberOf()).thenReturn(memberOfRequestBuilder);
        when(memberOfRequestBuilder.graphGroup()).thenReturn(graphGroupRequestBuilder);
        when(graphGroupRequestBuilder.get(any())).thenReturn(groupMembership1);
        when(requestAdapter.send(any(), any(), any())).thenReturn(groupMembership2, groupMembership3);

        final var licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(eq(MICROSOFT_GRAPH_FEATURE))).thenReturn(true);

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
        final var realmSettings = Settings.builder()
            .put(globalSettings)
            .put(getFullSettingKey(realmId, RealmSettings.ORDER_SETTING), 0)
            .build();

        final var config = new RealmConfig(realmId, realmSettings, env, threadContext);
        final var client = mock(GraphServiceClient.class);

        final var licenseState = MockLicenseState.createMock();
        when(licenseState.isAllowed(eq(MICROSOFT_GRAPH_FEATURE))).thenReturn(false);

        final var realm = new MicrosoftGraphAuthzRealm(roleMapper, config, client, licenseState, threadPool);
        final var future = new PlainActionFuture<User>();
        realm.lookupUser(username, future);
        final var thrown = assertThrows(ElasticsearchSecurityException.class, future::actionGet);
        assertThat(thrown.getMessage(), equalTo("current license is non-compliant for [microsoft_graph]"));
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
}
