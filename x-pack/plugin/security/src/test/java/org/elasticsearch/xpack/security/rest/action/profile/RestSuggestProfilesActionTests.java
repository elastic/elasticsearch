/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.profile;

import org.apache.lucene.search.TotalHits;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.license.MockLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesRequest;
import org.elasticsearch.xpack.core.security.action.profile.SuggestProfilesResponse;
import org.elasticsearch.xpack.security.Security;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RestSuggestProfilesActionTests extends RestActionTestCase {
    private Settings settings;
    private MockLicenseState licenseState;
    private RestSuggestProfilesAction restSuggestProfilesAction;
    private AtomicReference<SuggestProfilesRequest> requestHolder;

    @Before
    public void init() {
        settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();
        licenseState = MockLicenseState.createMock();
        requestHolder = new AtomicReference<>();
        restSuggestProfilesAction = new RestSuggestProfilesAction(settings, licenseState);
        controller().registerHandler(restSuggestProfilesAction);
        verifyingClient.setExecuteLocallyVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(SuggestProfilesRequest.class));
            requestHolder.set((SuggestProfilesRequest) actionRequest);
            return new SuggestProfilesResponse(new SuggestProfilesResponse.ProfileHit[0], 0, new TotalHits(0, TotalHits.Relation.EQUAL_TO));
        }));
    }

    public void testInnerPrepareRequestWithSourceParameter() {
        when(licenseState.isAllowed(Security.USER_PROFILE_COLLABORATION_FEATURE)).thenReturn(true);
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

    public void testParsingDataParameter() {
        when(licenseState.isAllowed(Security.USER_PROFILE_COLLABORATION_FEATURE)).thenReturn(true);
        final FakeRestRequest.Builder builder = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(
            randomFrom(RestRequest.Method.GET, RestRequest.Method.POST)
        ).withPath("/_security/profile/_suggest");

        if (randomBoolean()) {
            builder.withParams(new HashMap<>(Map.of("data", "app1,app2,other*,app5")));
        } else {
            builder.withContent(new BytesArray("{\"data\": \"app1,app2,other*,app5\"}"), XContentType.JSON);
        }
        final FakeRestRequest restRequest = builder.build();

        dispatchRequest(restRequest);

        final SuggestProfilesRequest suggestProfilesRequest = requestHolder.get();
        assertThat(suggestProfilesRequest.getDataKeys(), equalTo(Set.of("app1", "app2", "other*", "app5")));
    }

    public void testWillNotAllowDataInBothQueryParameterAndRequestBody() {
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withMethod(
            randomFrom(RestRequest.Method.GET, RestRequest.Method.POST)
        )
            .withPath("/_security/profile/_suggest")
            .withParams(new HashMap<>(Map.of("data", randomAlphaOfLengthBetween(3, 8))))
            .withContent(new BytesArray(Strings.format("{\"data\": \"%s\"}", randomAlphaOfLengthBetween(3, 8))), XContentType.JSON)
            .build();

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> restSuggestProfilesAction.innerPrepareRequest(restRequest, verifyingClient)
        );
        assertThat(
            e.getMessage(),
            containsString("The [data] parameter must be specified in either request body or as query parameter, but not both")
        );
    }

    public void testLicenseEnforcement() {
        final boolean featureAllowed = randomBoolean();
        when(licenseState.isAllowed(Security.USER_PROFILE_COLLABORATION_FEATURE)).thenReturn(featureAllowed);
        if (featureAllowed) {
            assertThat(restSuggestProfilesAction.checkFeatureAvailable(new FakeRestRequest()), nullValue());
            verify(licenseState).featureUsed(Security.USER_PROFILE_COLLABORATION_FEATURE);
        } else {
            final Exception e = restSuggestProfilesAction.checkFeatureAvailable(new FakeRestRequest());
            assertThat(e, instanceOf(ElasticsearchSecurityException.class));
            assertThat(e.getMessage(), containsString("current license is non-compliant for [user-profile-collaboration]"));
            assertThat(((ElasticsearchSecurityException) e).status(), equalTo(RestStatus.FORBIDDEN));
            verify(licenseState, never()).featureUsed(Security.USER_PROFILE_COLLABORATION_FEATURE);
        }
    }
}
