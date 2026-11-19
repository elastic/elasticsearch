/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.service;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenAction;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenRequest;
import org.elasticsearch.xpack.core.security.action.service.CreateServiceAccountTokenResponse;
import org.elasticsearch.xpack.core.security.action.service.CreateUserManagedServiceAccountTokenAction;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;
import static org.mockito.Mockito.mock;

public class RestCreateServiceAccountTokenActionTests extends RestActionTestCase {

    private AtomicReference<ActionType<?>> actionHolder;
    private AtomicReference<CreateServiceAccountTokenRequest> requestHolder;

    @Before
    public void init() {
        final Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();
        actionHolder = new AtomicReference<>();
        requestHolder = new AtomicReference<>();
        controller().registerHandler(new RestCreateServiceAccountTokenAction(settings, mock(XPackLicenseState.class)));
        verifyingClient.setExecuteVerifier((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(CreateServiceAccountTokenRequest.class));
            actionHolder.set(actionType);
            requestHolder.set((CreateServiceAccountTokenRequest) actionRequest);
            return CreateServiceAccountTokenResponse.created("token", new SecureString("secret".toCharArray()));
        });
    }

    public void testReservedNamespaceRoutesToTheBuiltInActionRegardlessOfCase() {
        for (String namespace : new String[] { "elastic", "ELASTIC", "Elastic" }) {
            dispatchRequest(request(namespace, "fleet-server", "token1"));
            assertThat(actionHolder.get(), sameInstance(CreateServiceAccountTokenAction.INSTANCE));
        }
    }

    public void testAnyOtherNamespaceRoutesToTheUserManagedAction() {
        dispatchRequest(request("foo", "bar", "token1"));

        assertThat(actionHolder.get(), sameInstance(CreateUserManagedServiceAccountTokenAction.INSTANCE));
        final CreateServiceAccountTokenRequest createRequest = requestHolder.get();
        assertThat(createRequest.getNamespace(), equalTo("foo"));
        assertThat(createRequest.getServiceName(), equalTo("bar"));
        assertThat(createRequest.getTokenName(), equalTo("token1"));
    }

    public void testTokenNameIsGeneratedWhenTheRouteOmitsIt() {
        dispatchRequest(
            new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.POST)
                .withPath("/_security/service/foo/bar/credential/token")
                .build()
        );

        assertThat(actionHolder.get(), sameInstance(CreateUserManagedServiceAccountTokenAction.INSTANCE));
        assertThat(requestHolder.get().getTokenName(), startsWith("token_"));
    }

    private static FakeRestRequest request(String namespace, String service, String tokenName) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.POST)
            .withPath("/_security/service/" + namespace + "/" + service + "/credential/token/" + tokenName)
            .build();
    }
}
