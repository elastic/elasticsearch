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
import org.elasticsearch.xpack.core.security.action.service.ServiceAccountType;
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
        assertThat(request.getType(), equalTo(EnumSet.of(ServiceAccountType.BUILT_IN)));
    }

    public void testScopedRequestReportsBothKindsByDefault() {
        final GetServiceAccountRequest namespaceRequest = dispatch("/_security/service/ns", null);
        assertThat(namespaceRequest.getNamespace(), equalTo("ns"));
        assertThat(namespaceRequest.getServiceName(), nullValue());
        assertThat(namespaceRequest.getType(), equalTo(EnumSet.allOf(ServiceAccountType.class)));

        final GetServiceAccountRequest serviceRequest = dispatch("/_security/service/ns/svc", null);
        assertThat(serviceRequest.getNamespace(), equalTo("ns"));
        assertThat(serviceRequest.getServiceName(), equalTo("svc"));
        assertThat(serviceRequest.getType(), equalTo(EnumSet.allOf(ServiceAccountType.class)));
    }

    public void testTypeReplacesTheDefault() {
        assertThat(dispatch("/_security/service", "user_managed").getType(), equalTo(EnumSet.of(ServiceAccountType.USER_MANAGED)));
        assertThat(dispatch("/_security/service", "built_in,user_managed").getType(), equalTo(EnumSet.allOf(ServiceAccountType.class)));
        assertThat(dispatch("/_security/service/ns/svc", "built_in").getType(), equalTo(EnumSet.of(ServiceAccountType.BUILT_IN)));
        assertThat(
            dispatch("/_security/service/ns/svc", "user_managed,user_managed").getType(),
            equalTo(EnumSet.of(ServiceAccountType.USER_MANAGED))
        );
    }

    public void testEmptyTypeIsLeftForTheRequestToReject() {
        final GetServiceAccountRequest request = dispatch("/_security/service", "");
        assertThat(request.getType(), empty());
        assertThat(request.validate().getMessage(), containsString("type must name at least one of [built_in, user_managed]"));
    }

    public void testUnknownTypeValueIsRejected() {
        final RestGetServiceAccountAction action = new RestGetServiceAccountAction(Settings.EMPTY, mock(XPackLicenseState.class));
        final FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.GET)
            .withPath("/_security/service")
            .withParams(Map.of("type", "elasticsearch"))
            .build();

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> action.innerPrepareRequest(request, null));
        assertThat(e.getMessage(), equalTo("invalid type value [elasticsearch]; must be one of [built_in, user_managed]"));
    }

    private GetServiceAccountRequest dispatch(String path, String type) {
        requestHolder.set(null);
        final FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.GET)
            .withPath(path);
        if (type != null) {
            builder.withParams(Map.of("type", type));
        }
        dispatchRequest(builder.build());
        final GetServiceAccountRequest request = requestHolder.get();
        assertThat("no request reached the transport layer for [" + path + "]", request, instanceOf(GetServiceAccountRequest.class));
        return request;
    }
}
