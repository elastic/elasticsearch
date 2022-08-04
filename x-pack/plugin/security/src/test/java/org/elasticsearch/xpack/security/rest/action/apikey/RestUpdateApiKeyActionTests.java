/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateApiKeyResponse;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;

public class RestUpdateApiKeyActionTests extends RestActionTestCase {

    private RestUpdateApiKeyAction restAction;
    private AtomicReference<UpdateApiKeyRequest> requestHolder;

    @Before
    public void init() {
        final Settings settings = Settings.builder().put(XPackSettings.SECURITY_ENABLED.getKey(), true).build();
        final XPackLicenseState licenseState = mock(XPackLicenseState.class);
        requestHolder = new AtomicReference<>();
        restAction = new RestUpdateApiKeyAction(settings, licenseState);
        controller().registerHandler(restAction);
        verifyingClient.setExecuteVerifier(((actionType, actionRequest) -> {
            assertThat(actionRequest, instanceOf(UpdateApiKeyRequest.class));
            requestHolder.set((UpdateApiKeyRequest) actionRequest);
            return new UpdateApiKeyResponse(true);
        }));
    }

    public void testAbsentRoleDescriptorsAndMetadataSetToNull() {
        final var apiKeyId = "api_key_id";
        final var builder = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
            .withPath("/_security/api_key/" + apiKeyId);
        if (randomBoolean()) {
            builder.withContent(new BytesArray("{}"), XContentType.JSON);
        }

        dispatchRequest(builder.build());

        final UpdateApiKeyRequest request = requestHolder.get();
        assertEquals(apiKeyId, request.getId());
        assertNull(request.getRoleDescriptors());
        assertNull(request.getMetadata());
    }
}
