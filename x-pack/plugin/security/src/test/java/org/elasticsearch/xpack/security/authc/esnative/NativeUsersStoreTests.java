/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.FilterClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames;
import org.elasticsearch.xpack.core.security.user.APMSystemUser;
import org.elasticsearch.xpack.core.security.user.BeatsSystemUser;
import org.elasticsearch.xpack.core.security.user.ElasticUser;
import org.elasticsearch.xpack.core.security.user.KibanaSystemUser;
import org.elasticsearch.xpack.core.security.user.KibanaUser;
import org.elasticsearch.xpack.core.security.user.LogstashSystemUser;
import org.elasticsearch.xpack.core.security.user.RemoteMonitoringUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Consumer;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NativeUsersStoreTests extends ESTestCase {

    private static final String ENABLED_FIELD = User.Fields.ENABLED.getPreferredName();
    private static final String PASSWORD_FIELD = User.Fields.PASSWORD.getPreferredName();
    private static final String BLANK_PASSWORD = "";

    private Client client;
    private final List<Tuple<ActionRequest, ActionListener<? extends ActionResponse>>> requests = new CopyOnWriteArrayList<>();

    @Before
    public void setupMocks() {
        Client mockClient = mock(Client.class);
        when(mockClient.settings()).thenReturn(Settings.EMPTY);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(mockClient.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        client = new FilterClient(mockClient) {

            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
                requests.add(new Tuple<>(request, listener));
            }
        };
    }

    public void testPasswordUpsertWhenSetEnabledOnReservedUser() throws Exception {
        final NativeUsersStore nativeUsersStore = startNativeUsersStore();

        final String user = randomFrom(ElasticUser.NAME, KibanaUser.NAME, KibanaSystemUser.NAME,
            LogstashSystemUser.NAME, BeatsSystemUser.NAME, APMSystemUser.NAME, RemoteMonitoringUser.NAME);

        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        nativeUsersStore.setEnabled(user, true, WriteRequest.RefreshPolicy.IMMEDIATE, future);
        final UpdateRequest update = actionRespond(UpdateRequest.class, null);

        final Map<String, Object> docMap = update.doc().sourceAsMap();
        assertThat(docMap.get(ENABLED_FIELD), equalTo(Boolean.TRUE));
        assertThat(docMap.get(PASSWORD_FIELD), nullValue());

        final Map<String, Object> upsertMap = update.upsertRequest().sourceAsMap();
        assertThat(upsertMap.get(User.Fields.ENABLED.getPreferredName()), equalTo(Boolean.TRUE));
        assertThat(upsertMap.get(User.Fields.PASSWORD.getPreferredName()), equalTo(BLANK_PASSWORD));
    }

    public void testBlankPasswordInIndexImpliesDefaultPassword() throws Exception {
        final NativeUsersStore nativeUsersStore = startNativeUsersStore();

        final String user = randomFrom(ElasticUser.NAME, KibanaUser.NAME, KibanaSystemUser.NAME,
            LogstashSystemUser.NAME, BeatsSystemUser.NAME, APMSystemUser.NAME, RemoteMonitoringUser.NAME);
        final Map<String, Object> values = new HashMap<>();
        values.put(ENABLED_FIELD, Boolean.TRUE);
        values.put(PASSWORD_FIELD, BLANK_PASSWORD);

        final GetResult result = new GetResult(
                RestrictedIndicesNames.SECURITY_MAIN_ALIAS,
                MapperService.SINGLE_MAPPING_NAME,
                NativeUsersStore.getIdForUser(NativeUsersStore.RESERVED_USER_TYPE, randomAlphaOfLength(12)),
            0, 1, 1L,
                true,
                BytesReference.bytes(jsonBuilder().map(values)),
                Collections.emptyMap(),
                Collections.emptyMap());

        final PlainActionFuture<NativeUsersStore.ReservedUserInfo> future = new PlainActionFuture<>();
        nativeUsersStore.getReservedUserInfo(user, future);

        actionRespond(GetRequest.class, new GetResponse(result));

        final NativeUsersStore.ReservedUserInfo userInfo = future.get();
        assertThat(userInfo.hasEmptyPassword(), equalTo(true));
        assertThat(userInfo.enabled, equalTo(true));
        assertTrue(Hasher.verifyHash(new SecureString("".toCharArray()), userInfo.passwordHash));
    }

    public void testVerifyUserWithCorrectPassword() throws Exception {
        final NativeUsersStore nativeUsersStore = startNativeUsersStore();
        final String username = randomAlphaOfLengthBetween(4, 12);
        final SecureString password = new SecureString(randomAlphaOfLengthBetween(8, 16).toCharArray());
        final String[] roles = generateRandomStringArray(4, 12, false, false);

        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        nativeUsersStore.verifyPassword(username, password, future);

        respondToGetUserRequest(username, password, roles);

        final AuthenticationResult result = future.get();
        assertThat(result, notNullValue());
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.SUCCESS));
        final User user = result.getUser();
        assertThat(user, notNullValue());
        assertThat(user.enabled(), equalTo(true));
        assertThat(user.principal(), equalTo(username));
        assertThat(user.roles(), equalTo(roles));
        assertThat(user.authenticatedUser(), equalTo(user));
    }

    public void testVerifyUserWithIncorrectPassword() throws Exception {
        final NativeUsersStore nativeUsersStore = startNativeUsersStore();
        final String username = randomAlphaOfLengthBetween(4, 12);
        final SecureString correctPassword = new SecureString(randomAlphaOfLengthBetween(12, 16).toCharArray());
        final SecureString incorrectPassword = new SecureString(randomAlphaOfLengthBetween(8, 10).toCharArray());
        final String[] roles = generateRandomStringArray(4, 12, false, false);

        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        nativeUsersStore.verifyPassword(username, incorrectPassword, future);

        respondToGetUserRequest(username, correctPassword, roles);

        final AuthenticationResult result = future.get();
        assertThat(result, notNullValue());
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
        assertThat(result.getUser(), nullValue());
        assertThat(result.getMessage(), containsString("authentication failed"));
    }

    public void testVerifyNonExistentUser() throws Exception {
        final NativeUsersStore nativeUsersStore = startNativeUsersStore();
        final String username = randomAlphaOfLengthBetween(4, 12);
        final SecureString password = new SecureString(randomAlphaOfLengthBetween(8, 16).toCharArray());

        final PlainActionFuture<AuthenticationResult> future = new PlainActionFuture<>();
        nativeUsersStore.verifyPassword(username, password, future);

        final GetResult getResult = new GetResult(
                RestrictedIndicesNames.SECURITY_MAIN_ALIAS,
                MapperService.SINGLE_MAPPING_NAME,
                NativeUsersStore.getIdForUser(NativeUsersStore.USER_DOC_TYPE, username),
                UNASSIGNED_SEQ_NO, 0, 1L,
                false,
                null,
                Collections.emptyMap(),
            Collections.emptyMap());

        actionRespond(GetRequest.class, new GetResponse(getResult));

        final AuthenticationResult result = future.get();
        assertThat(result, notNullValue());
        assertThat(result.getStatus(), equalTo(AuthenticationResult.Status.CONTINUE));
        assertThat(result.getUser(), nullValue());
        assertThat(result.getMessage(), nullValue());
    }

    public void testDefaultReservedUserInfoPasswordEmpty() {
        NativeUsersStore.ReservedUserInfo disabledUserInfo = NativeUsersStore.ReservedUserInfo.defaultDisabledUserInfo();
        NativeUsersStore.ReservedUserInfo enabledUserInfo = NativeUsersStore.ReservedUserInfo.defaultEnabledUserInfo();
        NativeUsersStore.ReservedUserInfo constructedUserInfo =
            new NativeUsersStore.ReservedUserInfo(Hasher.PBKDF2.hash(new SecureString(randomAlphaOfLength(14))), randomBoolean());

        assertThat(disabledUserInfo.hasEmptyPassword(), equalTo(true));
        assertThat(enabledUserInfo.hasEmptyPassword(), equalTo(true));
        assertThat(constructedUserInfo.hasEmptyPassword(), equalTo(false));
    }

    @SuppressWarnings("unchecked")
    private <ARequest extends ActionRequest, AResponse extends ActionResponse> ARequest actionRespond(Class<ARequest> requestClass,
                                                                                                      AResponse response) {
        Tuple<ARequest, ActionListener<?>> tuple = findRequest(requestClass);
        ((ActionListener<AResponse>) tuple.v2()).onResponse(response);
        return tuple.v1();
    }

    private <ARequest extends ActionRequest> Tuple<ARequest, ActionListener<?>> findRequest(
        Class<ARequest> requestClass) {
        return this.requests.stream()
            .filter(t -> requestClass.isInstance(t.v1()))
            .map(t -> new Tuple<ARequest, ActionListener<?>>(requestClass.cast(t.v1()), t.v2()))
            .findFirst().orElseThrow(() -> new RuntimeException("Cannot find request of type " + requestClass));
    }

    private void respondToGetUserRequest(String username, SecureString password, String[] roles) throws IOException {
        // Native users store is initiated with default hashing algorithm
        final Map<String, Object> values = new HashMap<>();
        values.put(User.Fields.USERNAME.getPreferredName(), username);
        values.put(User.Fields.PASSWORD.getPreferredName(), String.valueOf(Hasher.BCRYPT.hash(password)));
        values.put(User.Fields.ROLES.getPreferredName(), roles);
        values.put(User.Fields.ENABLED.getPreferredName(), Boolean.TRUE);
        values.put(User.Fields.TYPE.getPreferredName(), NativeUsersStore.USER_DOC_TYPE);
        final BytesReference source = BytesReference.bytes(jsonBuilder().map(values));
        final GetResult getResult = new GetResult(
                RestrictedIndicesNames.SECURITY_MAIN_ALIAS,
                MapperService.SINGLE_MAPPING_NAME,
                NativeUsersStore.getIdForUser(NativeUsersStore.USER_DOC_TYPE, username),
                0, 1, 1L,
                true,
                source,
                Collections.emptyMap(),
                Collections.emptyMap());

        actionRespond(GetRequest.class, new GetResponse(getResult));
    }

    @SuppressWarnings("unchecked")
    private NativeUsersStore startNativeUsersStore() {
        SecurityIndexManager securityIndex = mock(SecurityIndexManager.class);
        when(securityIndex.isAvailable()).thenReturn(true);
        when(securityIndex.indexExists()).thenReturn(true);
        when(securityIndex.isMappingUpToDate()).thenReturn(true);
        when(securityIndex.isIndexUpToDate()).thenReturn(true);
        when(securityIndex.freeze()).thenReturn(securityIndex);
        doAnswer((i) -> {
            Runnable action = (Runnable) i.getArguments()[1];
            action.run();
            return null;
        }).when(securityIndex).prepareIndexIfNeededThenExecute(any(Consumer.class), any(Runnable.class));
        doAnswer((i) -> {
            Runnable action = (Runnable) i.getArguments()[1];
            action.run();
            return null;
        }).when(securityIndex).checkIndexVersionThenExecute(any(Consumer.class), any(Runnable.class));
        return new NativeUsersStore(Settings.EMPTY, client, securityIndex);
    }

}
