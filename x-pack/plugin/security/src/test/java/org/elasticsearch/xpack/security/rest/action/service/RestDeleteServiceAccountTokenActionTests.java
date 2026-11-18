/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.service;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountTokenAction;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class RestDeleteServiceAccountTokenActionTests extends RestActionTestCase {

    private AtomicReference<ActionType<?>> actionHolder;
    private AtomicReference<DeleteServiceAccountTokenRequest> requestHolder;

    @Before
    public void init() {
        final Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();
        actionHolder = new AtomicReference<>();
        requestHolder = new AtomicReference<>();
        controller().registerHandler(new RestDeleteServiceAccountTokenAction(settings, mock(XPackLicenseState.class)));
        verifyingClient.setExecuteVerifier((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(DeleteServiceAccountTokenRequest.class));
            actionHolder.set(actionType);
            requestHolder.set((DeleteServiceAccountTokenRequest) actionRequest);
            return new DeleteServiceAccountTokenResponse(true);
        });
    }

    public void testReservedNamespaceRoutesToTheBuiltInActionRegardlessOfCase() {
        for (String namespace : new String[] { "elastic", "ELASTIC", "Elastic" }) {
            dispatchRequest(request(namespace, "fleet-server", "token1"));
            assertThat(actionHolder.get(), sameInstance(DeleteServiceAccountTokenAction.INSTANCE));
        }
    }

    public void testAnyOtherNamespaceRoutesToTheUserManagedAction() {
        dispatchRequest(request("foo", "bar", "token1"));

        assertThat(actionHolder.get(), sameInstance(DeleteUserManagedServiceAccountTokenAction.INSTANCE));
        final DeleteServiceAccountTokenRequest deleteRequest = requestHolder.get();
        assertThat(deleteRequest.getNamespace(), equalTo("foo"));
        assertThat(deleteRequest.getServiceName(), equalTo("bar"));
        assertThat(deleteRequest.getTokenName(), equalTo("token1"));
    }

    private static FakeRestRequest request(String namespace, String service, String tokenName) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.DELETE)
            .withPath("/_security/service/" + namespace + "/" + service + "/credential/token/" + tokenName)
            .build();
    }
}
