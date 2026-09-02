/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.service;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.GetServiceAccountResponse;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountInfo;
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountManagedBy;
import org.junit.Before;

import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class RestGetServiceAccountActionTests extends RestActionTestCase {

    private AtomicReference<GetServiceAccountRequest> requestHolder;

    @Before
    public void init() {
        final Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();
        requestHolder = new AtomicReference<>();
        controller().registerHandler(new RestGetServiceAccountAction(settings, mock(XPackLicenseState.class)));
        verifyingClient.setExecuteVerifier((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(GetServiceAccountRequest.class));
            requestHolder.set((GetServiceAccountRequest) actionRequest);
            return new GetServiceAccountResponse(new ServiceAccountInfo[0]);
        });
    }

    public void testUnscopedRequestReportsBuiltInAccountsByDefault() {
        final GetServiceAccountRequest request = dispatch("/_security/service", null);
        assertThat(request.getNamespace(), nullValue());
        assertThat(request.getServiceName(), nullValue());
        assertThat(request.getManagedBy(), equalTo(EnumSet.of(ServiceAccountManagedBy.ELASTIC)));
    }

    public void testScopedRequestReportsBothKindsByDefault() {
        final GetServiceAccountRequest namespaceRequest = dispatch("/_security/service/ns", null);
        assertThat(namespaceRequest.getNamespace(), equalTo("ns"));
        assertThat(namespaceRequest.getServiceName(), nullValue());
        assertThat(namespaceRequest.getManagedBy(), equalTo(EnumSet.allOf(ServiceAccountManagedBy.class)));

        final GetServiceAccountRequest serviceRequest = dispatch("/_security/service/ns/svc", null);
        assertThat(serviceRequest.getNamespace(), equalTo("ns"));
        assertThat(serviceRequest.getServiceName(), equalTo("svc"));
        assertThat(serviceRequest.getManagedBy(), equalTo(EnumSet.allOf(ServiceAccountManagedBy.class)));
    }

    public void testManagedByReplacesTheDefault() {
        assertThat(dispatch("/_security/service", "user").getManagedBy(), equalTo(EnumSet.of(ServiceAccountManagedBy.USER)));
        assertThat(dispatch("/_security/service", "elastic,user").getManagedBy(), equalTo(EnumSet.allOf(ServiceAccountManagedBy.class)));
        assertThat(dispatch("/_security/service/ns/svc", "elastic").getManagedBy(), equalTo(EnumSet.of(ServiceAccountManagedBy.ELASTIC)));
        assertThat(dispatch("/_security/service/ns/svc", "user,user").getManagedBy(), equalTo(EnumSet.of(ServiceAccountManagedBy.USER)));
    }

    public void testEmptyManagedByIsLeftForTheRequestToReject() {
        final GetServiceAccountRequest request = dispatch("/_security/service", "");
        assertThat(request.getManagedBy(), empty());
        assertThat(request.validate().getMessage(), containsString("managed_by must name at least one of [elastic, user]"));
    }

    public void testUnknownManagedByValueIsRejected() {
        final RestGetServiceAccountAction action = new RestGetServiceAccountAction(Settings.EMPTY, mock(XPackLicenseState.class));
        final FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.GET)
            .withPath("/_security/service")
            .withParams(Map.of("managed_by", "elasticsearch"))
            .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.innerPrepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("invalid managed_by value [elasticsearch]; must be one of [elastic, user]"));
    }

    private GetServiceAccountRequest dispatch(String path, String managedBy) {
        requestHolder.set(null);
        final FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.GET)
            .withPath(path);
        if (managedBy != null) {
            builder.withParams(Map.of("managed_by", managedBy));
        }
        dispatchRequest(builder.build());
        final GetServiceAccountRequest request = requestHolder.get();
        assertThat("no request reached the transport layer for [" + path + "]", request, instanceOf(GetServiceAccountRequest.class));
        return request;
    }
}
