/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.service;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.service.PutUserManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.PutUserManagedServiceAccountResponse;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class RestPutUserManagedServiceAccountActionTests extends RestActionTestCase {

    private Settings settings;
    private AtomicReference<PutUserManagedServiceAccountRequest> requestHolder;

    @Before
    public void init() {
        settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();
        requestHolder = new AtomicReference<>();
        controller().registerHandler(new RestPutUserManagedServiceAccountAction(settings, mock(XPackLicenseState.class), true));
        verifyingClient.setExecuteVerifier((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(PutUserManagedServiceAccountRequest.class));
            requestHolder.set((PutUserManagedServiceAccountRequest) actionRequest);
            return new PutUserManagedServiceAccountResponse(true);
        });
    }

    public void testAccountIsTakenFromThePathAndRolesFromTheBody() {
        dispatchRequest(request("""
            {"roles":["role1","role2"]}""", Map.of()));

        final PutUserManagedServiceAccountRequest putRequest = requestHolder.get();
        assertThat(putRequest.getNamespace(), equalTo("ns"));
        assertThat(putRequest.getServiceName(), equalTo("svc"));
        assertThat(putRequest.getRoles(), contains("role1", "role2"));
        assertThat(putRequest.isEnabled(), equalTo(true));
        assertThat(putRequest.getRefreshPolicy(), equalTo(WriteRequest.RefreshPolicy.WAIT_UNTIL));
    }

    public void testEnabledAndRefreshAreReadWhenGiven() {
        dispatchRequest(request("""
            {"roles":[],"enabled":false}""", Map.of("refresh", "true")));

        final PutUserManagedServiceAccountRequest putRequest = requestHolder.get();
        assertThat(putRequest.getRoles(), empty());
        assertThat(putRequest.isEnabled(), equalTo(false));
        assertThat(putRequest.getRefreshPolicy(), equalTo(WriteRequest.RefreshPolicy.IMMEDIATE));
    }

    public void testCapabilityIsAdvertisedOnlyWhereTheFeatureIsAvailable() {
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        assertThat(
            new RestPutUserManagedServiceAccountAction(settings, licenseState, true).supportedCapabilities(),
            contains(UserManagedServiceAccountRestCapabilities.USER_MANAGED_SERVICE_ACCOUNTS)
        );
        assertThat(new RestPutUserManagedServiceAccountAction(settings, licenseState, false).supportedCapabilities(), empty());
    }

    private static FakeRestRequest request(String body, Map<String, String> params) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.PUT)
            .withPath("/_security/service/ns/svc")
            .withParams(params)
            .withContent(new BytesArray(body), XContentType.JSON)
            .build();
    }
}
