/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.profile;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesRequest;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesResponse;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class RestSuggestProfilesActionTests extends RestActionTestCase {
    private Settings settings;
    private XPackLicenseState licenseState;
    private AtomicReference<SuggestProfilesRequest> requestHolder;

    @Before
    public void init() {
        settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();
        licenseState = mock(XPackLicenseState.class);
        requestHolder = new AtomicReference<>();
        controller().registerHandler(new RestSuggestProfilesAction(settings, licenseState));
        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(SuggestProfilesRequest.class));
            requestHolder.set((SuggestProfilesRequest) actionRequest);
            return mock(SuggestProfilesResponse.class);
        }));
    }

    public void testInnerPrepareRequestWithSourceParameter() {
        String expectedName = "bob";
        final Map<String, String> params = new HashMap<>(
            Map.of("source", "{\"name\":\"" + expectedName + "\"}", "source_content_type", "application/json")
        );
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(
            randomFrom(RestRequest.Method.GET, RestRequest.Method.POST)
        ).withPath("/_security/profile/_suggest").withParams(params).build();

        dispatchRequest(restRequest);

        final SuggestProfilesRequest suggestProfilesRequest = requestHolder.get();
        assertThat(suggestProfilesRequest.getName(), equalTo(expectedName));
    }
}
