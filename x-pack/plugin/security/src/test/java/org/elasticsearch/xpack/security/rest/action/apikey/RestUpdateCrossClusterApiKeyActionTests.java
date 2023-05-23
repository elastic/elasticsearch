/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.rest.action.apikey;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.UpdateCrossClusterApiKeyRequest;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyRequestTests.randomCrossClusterApiKeyAccessField;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class RestUpdateCrossClusterApiKeyActionTests extends ESTestCase {

    public void testUpdateHasTypeOfCrossCluster() throws Exception {
        final String id = randomAlphaOfLength(10);
        final String access = randomCrossClusterApiKeyAccessField();
        final boolean hasMetadata = randomBoolean();
        final FakeRestRequest restRequest = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
            new BytesArray(Strings.format("""
                {
                  "access": %s%s
                }""", access, hasMetadata ? ", \"metadata\":{\"key\":\"value\"}" : "")),
            XContentType.JSON
        ).withParams(Map.of("id", id)).build();

        final var action = new RestUpdateCrossClusterApiKeyAction(Settings.EMPTY, mock(XPackLicenseState.class));
        final NodeClient client = mock(NodeClient.class);
        action.handleRequest(restRequest, mock(RestChannel.class), client);

        final ArgumentCaptor<UpdateCrossClusterApiKeyRequest> requestCaptor = ArgumentCaptor.forClass(
            UpdateCrossClusterApiKeyRequest.class
        );
        verify(client).execute(eq(UpdateCrossClusterApiKeyAction.INSTANCE), requestCaptor.capture(), any());

        final UpdateCrossClusterApiKeyRequest request = requestCaptor.getValue();
        assertThat(request.getType(), is(ApiKey.Type.CROSS_CLUSTER));
        assertThat(request.getId(), equalTo(id));
        assertThat(request.getRoleDescriptors(), equalTo(List.of(CrossClusterApiKeyRoleDescriptorBuilder.parse(access).build())));
        if (hasMetadata) {
            assertThat(request.getMetadata(), equalTo(Map.of("key", "value")));
        } else {
            assertThat(request.getMetadata(), nullValue());
        }
    }
}
