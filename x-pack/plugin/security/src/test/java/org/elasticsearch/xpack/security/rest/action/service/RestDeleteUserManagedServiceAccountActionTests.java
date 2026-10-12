/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.service;

import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountRequest;
import org.elasticsearch.xpack.core.security.action.service.DeleteUserManagedServiceAccountResponse;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class RestDeleteUserManagedServiceAccountActionTests extends RestActionTestCase {

    private Settings settings;
    private AtomicReference<DeleteUserManagedServiceAccountRequest> requestHolder;
    private AtomicBoolean found;

    @Before
    public void init() {
        settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();
        requestHolder = new AtomicReference<>();
        found = new AtomicBoolean(true);
        controller().registerHandler(new RestDeleteUserManagedServiceAccountAction(settings, mock(XPackLicenseState.class), true));
        verifyingClient.setExecuteVerifier((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(DeleteUserManagedServiceAccountRequest.class));
            requestHolder.set((DeleteUserManagedServiceAccountRequest) actionRequest);
            return new DeleteUserManagedServiceAccountResponse(found.get());
        });
    }

    public void testAccountIsTakenFromThePathAndForceDefaultsToFalse() {
        dispatchRequest(request(Map.of()));

        final DeleteUserManagedServiceAccountRequest deleteRequest = requestHolder.get();
        assertThat(deleteRequest.getNamespace(), equalTo("ns"));
        assertThat(deleteRequest.getServiceName(), equalTo("svc"));
        assertThat(deleteRequest.isForce(), equalTo(false));
        assertThat(deleteRequest.getRefreshPolicy(), equalTo(WriteRequest.RefreshPolicy.WAIT_UNTIL));
    }

    public void testForceAndRefreshAreReadWhenGiven() {
        dispatchRequest(request(Map.of("force", "true", "refresh", "false")));

        final DeleteUserManagedServiceAccountRequest deleteRequest = requestHolder.get();
        assertThat(deleteRequest.isForce(), equalTo(true));
        assertThat(deleteRequest.getRefreshPolicy(), equalTo(WriteRequest.RefreshPolicy.NONE));
    }

    public void testDeletingAnAccountThatWasNotThereAnswersNotFound() {
        found.set(true);
        assertThat(statusOf(request(Map.of())), equalTo(RestStatus.OK));

        found.set(false);
        assertThat(statusOf(request(Map.of())), equalTo(RestStatus.NOT_FOUND));
    }

    public void testCapabilityIsAdvertisedOnlyWhereTheFeatureIsAvailable() {
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        assertThat(
            new RestDeleteUserManagedServiceAccountAction(settings, licenseState, true).supportedCapabilities(),
            contains(UserManagedServiceAccountRestCapabilities.USER_MANAGED_SERVICE_ACCOUNTS)
        );
        assertThat(new RestDeleteUserManagedServiceAccountAction(settings, licenseState, false).supportedCapabilities(), empty());
    }

    private RestStatus statusOf(FakeRestRequest request) {
        final FakeRestChannel channel = new FakeRestChannel(request, true);
        final ThreadContext threadContext = verifyingClient.threadPool().getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            controller().dispatchRequest(request, channel, threadContext);
            return channel.capturedResponse().status();
        } finally {
            Releasables.close(channel.capturedResponse());
        }
    }

    private static FakeRestRequest request(Map<String, String> params) {
        return new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(RestRequest.Method.DELETE)
            .withPath("/_security/service/ns/svc")
            .withParams(params)
            .build();
    }
}
