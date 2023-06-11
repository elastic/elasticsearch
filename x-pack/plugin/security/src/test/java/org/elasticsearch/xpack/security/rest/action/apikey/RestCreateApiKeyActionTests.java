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
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.AbstractRestChannel;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class RestCreateApiKeyActionTests extends ESTestCase {
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
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        terminate(threadPool);
    }

    @SuppressWarnings({ "unchecked" })
    public void testCreateApiKeyApi() throws Exception {
        final String json = "{ \"name\" : \"my-api-key\", \"role_descriptors\": { \"role-a\": {\"cluster\":[\"a-1\", \"a-2\"]} } }";
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray(json),
            XContentType.JSON
        ).withParams(Collections.singletonMap("refresh", randomFrom("false", "true", "wait_for"))).build();

        final SetOnce<RestResponse> responseSetOnce = new SetOnce<>();
        final RestChannel restChannel = new AbstractRestChannel(restRequest, randomBoolean()) {
            @Override
            public void sendResponse(RestResponse restResponse) {
                responseSetOnce.set(restResponse);
            }
        };

        final CreateApiKeyResponse expected = new CreateApiKeyResponse(
            "my-api-key",
            UUID.randomUUID().toString(),
            new SecureString(randomAlphaOfLength(5)),
            Instant.now().plus(Duration.ofHours(5))
        );

        try (NodeClient client = new NodeClient(Settings.EMPTY, threadPool) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                CreateApiKeyRequest createApiKeyRequest = (CreateApiKeyRequest) request;
                @SuppressWarnings("unchecked")
                RestToXContentListener<CreateApiKeyResponse> actionListener = (RestToXContentListener<CreateApiKeyResponse>) listener;
                assertThat(createApiKeyRequest.getType(), is(ApiKey.Type.REST));
                if (createApiKeyRequest.getName().equals("my-api-key")) {
                    actionListener.onResponse(expected);
                } else {
                    listener.onFailure(new ElasticsearchSecurityException("encountered an error while creating API key"));
                }
            }
        }) {
            final RestCreateApiKeyAction restCreateApiKeyAction = new RestCreateApiKeyAction(Settings.EMPTY, mockLicenseState);
            restCreateApiKeyAction.handleRequest(restRequest, restChannel, client);

            final RestResponse restResponse = responseSetOnce.get();
            assertNotNull(restResponse);
            assertThat(
                CreateApiKeyResponse.fromXContent(createParser(XContentType.JSON.xContent(), restResponse.content())),
                equalTo(expected)
            );
        }
    }

}
