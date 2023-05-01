/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyAction;
import org.mockito.ArgumentCaptor;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RestCreateCrossClusterApiKeyActionTests extends ESTestCase {

    public void testCreateApiKeyRequestHasTypeOfCrossCluster() throws Exception {
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(new BytesArray("""
            {
              "name": "my-key",
              "access": {
                "search": [
                  {
                    "names": [
                      "logs"
                    ]
                  }
                ]
              }
            }"""), XContentType.JSON).build();

        final var action = new RestCreateCrossClusterApiKeyAction(Settings.EMPTY, mock(XPackLicenseState.class));
        final NodeClient client = mock(NodeClient.class);
        action.handleRequest(restRequest, mock(RestChannel.class), client);

        final ArgumentCaptor<CreateApiKeyRequest> requestCaptor = ArgumentCaptor.forClass(CreateApiKeyRequest.class);
        verify(client).execute(eq(CreateCrossClusterApiKeyAction.INSTANCE), requestCaptor.capture(), any());

        final CreateApiKeyRequest createApiKeyRequest = requestCaptor.getValue();
        assertThat(createApiKeyRequest.getType(), is(ApiKey.Type.CROSS_CLUSTER));
    }
}
