/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authc.esnative;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.security.SecurityTemplateService;
import org.elasticsearch.xpack.security.test.SecurityTestUtils;
import org.elasticsearch.xpack.security.user.ElasticUser;
import org.elasticsearch.xpack.security.user.KibanaUser;
import org.elasticsearch.xpack.security.user.LogstashSystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.junit.Before;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;

public class NativeUsersStoreTests extends ESTestCase {

    private static final String ENABLED_FIELD = User.Fields.ENABLED.getPreferredName();
    private static final String PASSWORD_FIELD = User.Fields.PASSWORD.getPreferredName();
    private static final String BLANK_PASSWORD = "";

    private InternalClient internalClient;
    private final List<Tuple<ActionRequest, ActionListener<? extends ActionResponse>>> requests = new CopyOnWriteArrayList<>();

    @Before
    public void setupMocks() {
        internalClient = new InternalClient(Settings.EMPTY, null, null, null) {

            @Override
            protected <
                    Request extends ActionRequest,
                    Response extends ActionResponse,
                    RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder>
                    > void doExecute(
                    Action<Request, Response, RequestBuilder> action,
                    Request request,
                    ActionListener<Response> listener) {
                requests.add(new Tuple<>(request, listener));
            }
        };
    }

    public void testPasswordUpsertWhenSetEnabledOnReservedUser() throws Exception {
        final NativeUsersStore nativeUsersStore = startNativeUsersStore();

        final String user = randomFrom(ElasticUser.NAME, KibanaUser.NAME, LogstashSystemUser.NAME);

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

        final String user = randomFrom(ElasticUser.NAME, KibanaUser.NAME, LogstashSystemUser.NAME);
        final Map<String, Object> values = new HashMap<>();
        values.put(ENABLED_FIELD, Boolean.TRUE);
        values.put(PASSWORD_FIELD, BLANK_PASSWORD);

        final GetResult result = new GetResult(
                SecurityTemplateService.SECURITY_INDEX_NAME,
                NativeUsersStore.RESERVED_USER_DOC_TYPE,
                randomAsciiOfLength(12),
                1L,
                true,
                jsonBuilder().map(values).bytes(),
                Collections.emptyMap());

        final PlainActionFuture<NativeUsersStore.ReservedUserInfo> future = new PlainActionFuture<>();
        nativeUsersStore.getReservedUserInfo(user, future);

        actionRespond(GetRequest.class, new GetResponse(result));

        final NativeUsersStore.ReservedUserInfo userInfo = future.get();
        assertThat(userInfo.hasDefaultPassword, equalTo(true));
        assertThat(userInfo.enabled, equalTo(true));
        assertThat(userInfo.passwordHash, equalTo(ReservedRealm.DEFAULT_PASSWORD_HASH));
    }

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

    private NativeUsersStore startNativeUsersStore() {
        final NativeUsersStore nativeUsersStore = new NativeUsersStore(Settings.EMPTY, internalClient);
        assertTrue(nativeUsersStore + " should be ready to start",
                nativeUsersStore.canStart(SecurityTestUtils.getClusterStateWithSecurityIndex(), true));
        nativeUsersStore.start();
        return nativeUsersStore;
    }

}