/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.InvalidateApiKeyResponse;

import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RestInvalidateApiKeyActionTests extends ESTestCase {
    private final XPackLicenseState mockLicenseState = mock(XPackLicenseState.class);
    private Settings settings = null;
    private ThreadPool threadPool = null;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        settings = Settings.builder()
                .put("path.home", createTempDir().toString())
                .put("node.name", "test-" + getTestName())
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
                .build();
        threadPool = new ThreadPool(settings);
        when(mockLicenseState.isSecurityEnabled()).thenReturn(true);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    public void testInvalidateApiKey() throws Exception {
        final String json1 = "{ \"realm_name\" : \"realm-1\", \"username\": \"user-x\" }";
        final String json2 = "{ \"realm_name\" : \"realm-1\" }";
        final String json3 = "{ \"username\": \"user-x\" }";
        final String json4 = "{ \"id\" : \"api-key-id-1\" }";
        final String json5 = "{ \"name\" : \"api-key-name-1\" }";
        final String json6 = "{ \"ids\" : [\"api-key-id-1\"] }";
        final String json = randomFrom(json1, json2, json3, json4, json5, json6);
        final boolean assertDeprecationWarning = json == json4;  // we want object identity comparison here
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
                .withContent(new BytesArray(json), XContentType.JSON).build();

        final SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        final RestChannel restChannel = new AbstractRestChannel(restRequest, randomBoolean()) {
            @Override
            public void sendResponse(RestResponse restResponse) {
                responseSetOnce.set(restResponse);
            }
        };

        final InvalidateApiKeyResponse invalidateApiKeyResponseExpected = new InvalidateApiKeyResponse(
                Collections.singletonList("api-key-id-1"), Collections.emptyList(), null);

        try (NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
                InvalidateApiKeyRequest invalidateApiKeyRequest = (InvalidateApiKeyRequest) request;
                ActionRequestValidationException validationException = invalidateApiKeyRequest.validate();
                if (validationException != null) {
                    listener.onFailure(validationException);
                    return;
                }
                if (invalidateApiKeyRequest.getName() != null && invalidateApiKeyRequest.getName().equals("api-key-name-1")
                        || invalidateApiKeyRequest.getIds() != null && Arrays.equals(
                            invalidateApiKeyRequest.getIds(), new String[] {"api-key-id-1"})
                        || invalidateApiKeyRequest.getRealmName() != null && invalidateApiKeyRequest.getRealmName().equals("realm-1")
                        || invalidateApiKeyRequest.getUserName() != null && invalidateApiKeyRequest.getUserName().equals("user-x")) {
                    listener.onResponse((Response) invalidateApiKeyResponseExpected);
                } else {
                    listener.onFailure(new ElasticsearchSecurityException("encountered an error while creating API key"));
                }
            }
        }) {
            final RestInvalidateApiKeyAction restInvalidateApiKeyAction = new RestInvalidateApiKeyAction(Settings.EMPTY, mockLicenseState);

            restInvalidateApiKeyAction.handleRequest(restRequest, restChannel, client);

            final RestResponse restResponse = responseSetOnce.get();
            assertNotNull(restResponse);
            final InvalidateApiKeyResponse actual = InvalidateApiKeyResponse
                    .fromXContent(createParser(XContentType.JSON.xContent(), restResponse.content()));
            assertThat(actual.getInvalidatedApiKeys(), equalTo(invalidateApiKeyResponseExpected.getInvalidatedApiKeys()));
            assertThat(actual.getPreviouslyInvalidatedApiKeys(),
                    equalTo(invalidateApiKeyResponseExpected.getPreviouslyInvalidatedApiKeys()));
            assertThat(actual.getErrors(), equalTo(invalidateApiKeyResponseExpected.getErrors()));
            if (assertDeprecationWarning) {
                assertWarnings("Deprecated field [id] used, replaced by [ids]");
            }
        }

    }

    public void testInvalidateApiKeyOwnedByCurrentAuthenticatedUser() throws Exception {
        final boolean isInvalidateRequestForOwnedKeysOnly = randomBoolean();
        final String json;
        if (isInvalidateRequestForOwnedKeysOnly) {
            json = "{ \"owner\" : \"true\" }";
        } else {
            json = "{ \"realm_name\" : \"realm-1\", \"owner\" : \"false\" }";
        }

        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY)
            .withContent(new BytesArray(json), XContentType.JSON).build();

        final SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        final RestChannel restChannel = new AbstractRestChannel(restRequest, randomBoolean()) {
            @Override
            public void sendResponse(RestResponse restResponse) {
                responseSetOnce.set(restResponse);
            }
        };

        final InvalidateApiKeyResponse invalidateApiKeyResponseExpectedWhenOwnerFlagIsTrue = new InvalidateApiKeyResponse(
            Collections.singletonList("api-key-id-1"), Collections.emptyList(), null);
        final InvalidateApiKeyResponse invalidateApiKeyResponseExpectedWhenOwnerFlagIsFalse = new InvalidateApiKeyResponse(
            Arrays.asList("api-key-id-1", "api-key-id-2"), Collections.emptyList(), null);

        try (NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @SuppressWarnings("unchecked")
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse>
            void doExecute(ActionType<Response> action, Request request, ActionListener<Response> listener) {
                InvalidateApiKeyRequest invalidateApiKeyRequest = (InvalidateApiKeyRequest) request;
                ActionRequestValidationException validationException = invalidateApiKeyRequest.validate();
                if (validationException != null) {
                    listener.onFailure(validationException);
                    return;
                }

                if (invalidateApiKeyRequest.ownedByAuthenticatedUser()) {
                    listener.onResponse((Response) invalidateApiKeyResponseExpectedWhenOwnerFlagIsTrue);
                } else if (invalidateApiKeyRequest.getRealmName() != null && invalidateApiKeyRequest.getRealmName().equals("realm-1")) {
                    listener.onResponse((Response) invalidateApiKeyResponseExpectedWhenOwnerFlagIsFalse);
                }
            }
        }) {
            final RestInvalidateApiKeyAction restInvalidateApiKeyAction = new RestInvalidateApiKeyAction(Settings.EMPTY, mockLicenseState);

            restInvalidateApiKeyAction.handleRequest(restRequest, restChannel, client);

            final RestResponse restResponse = responseSetOnce.get();
            assertNotNull(restResponse);
            assertThat(restResponse.status(), is(RestStatus.OK));
            final InvalidateApiKeyResponse actual = InvalidateApiKeyResponse
                .fromXContent(createParser(XContentType.JSON.xContent(), restResponse.content()));
            if (isInvalidateRequestForOwnedKeysOnly) {
                assertThat(actual.getInvalidatedApiKeys().size(), is(1));
                assertThat(actual.getInvalidatedApiKeys(),
                    containsInAnyOrder("api-key-id-1"));
            } else {
                assertThat(actual.getInvalidatedApiKeys().size(), is(2));
                assertThat(actual.getInvalidatedApiKeys(),
                    containsInAnyOrder("api-key-id-1", "api-key-id-2"));
            }
        }

    }
}
