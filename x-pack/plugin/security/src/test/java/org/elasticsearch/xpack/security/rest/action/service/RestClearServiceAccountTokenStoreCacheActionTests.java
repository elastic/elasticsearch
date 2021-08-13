/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.service;

import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheRequest;
import org.elasticsearch.xpack.core.security.action.ClearSecurityCacheResponse;
import org.elasticsearch.xpack.core.security.support.Validation;
import org.elasticsearch.xpack.core.security.support.ValidationTests;
import org.junit.Before;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestClearServiceAccountTokenStoreCacheActionTests extends RestActionTestCase {

    private Settings settings;
    private XPackLicenseState licenseState;
    private AtomicReference<ClearSecurityCacheRequest> requestHolder;

    @Before
    public void init() {
        settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();
        licenseState = mock(XPackLicenseState.class);
        requestHolder = new AtomicReference<>();
        controller().registerHandler(new RestClearServiceAccountTokenStoreCacheAction(settings, licenseState));
        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(ClearSecurityCacheRequest.class));
            requestHolder.set((ClearSecurityCacheRequest) actionRequest);
            final ClearSecurityCacheResponse response = mock(ClearSecurityCacheResponse.class);
            when(response.getClusterName()).thenReturn(new ClusterName(""));
            return response;
        }));
    }

    public void testInnerPrepareRequestWithEmptyTokenName() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String service = randomAlphaOfLengthBetween(3, 8);
        final String name = randomFrom("", "*", "_all");
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.POST)
            .withPath("/_security/service/" + namespace + "/" + service + "/credential/token/" + name + "/_clear_cache")
            .build();

        dispatchRequest(restRequest);

        final ClearSecurityCacheRequest clearSecurityCacheRequest = requestHolder.get();
        assertThat(clearSecurityCacheRequest.keys(), equalTo(new String[]{ namespace + "/" + service + "/"}));
    }

    public void testInnerPrepareRequestWithValidTokenNames() {
        final String namespace = randomAlphaOfLengthBetween(3, 8);
        final String service = randomAlphaOfLengthBetween(3, 8);
        final String[] names = randomArray(1, 3, String[]::new, ValidationTests::randomTokenName);
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withMethod(RestRequest.Method.POST)
            .withPath("/_security/service/" + namespace + "/" + service + "/credential/token/"
                + Strings.arrayToCommaDelimitedString(names) + "/_clear_cache")
            .build();

        dispatchRequest(restRequest);

        final ClearSecurityCacheRequest clearSecurityCacheRequest = requestHolder.get();
        assertThat(Set.of(clearSecurityCacheRequest.keys()),
            equalTo(Arrays.stream(names).map(n -> namespace + "/" + service + "/" + n).collect(Collectors.toUnmodifiableSet())));
    }

    public void testInnerPrepareRequestWillThrowErrorOnInvalidTokenNames() {
        final RestClearServiceAccountTokenStoreCacheAction restAction =
            new RestClearServiceAccountTokenStoreCacheAction(Settings.EMPTY, mock(XPackLicenseState.class));
        final String[] names = randomArray(2, 4, String[]::new,
            () -> randomValueOtherThanMany(n -> n.contains(","), ValidationTests::randomInvalidTokenName));
        // Add a valid name in the mix, we should still have one invalid name
        names[names.length - 1] = ValidationTests.randomTokenName();

        final FakeRestRequest fakeRestRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withParams(Map.of(
                "namespace", randomAlphaOfLengthBetween(3, 8),
                "service", randomAlphaOfLengthBetween(3, 8),
                "name", Strings.arrayToCommaDelimitedString(names)))
            .build();

        final IllegalArgumentException e =
            expectThrows(IllegalArgumentException.class, () -> restAction.innerPrepareRequest(fakeRestRequest, mock(NodeClient.class)));
        assertThat(e.getMessage(), containsString(Validation.INVALID_SERVICE_ACCOUNT_TOKEN_NAME_MESSAGE));
        assertThat(e.getMessage(), containsString("invalid service token name [" + names[0] + "]"));
    }
}
