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
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyRequest;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
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

        final ArgumentCaptor<CreateCrossClusterApiKeyRequest> requestCaptor = ArgumentCaptor.forClass(
            CreateCrossClusterApiKeyRequest.class
        );
        verify(client).execute(eq(CreateCrossClusterApiKeyAction.INSTANCE), requestCaptor.capture(), any());

        final CreateCrossClusterApiKeyRequest request = requestCaptor.getValue();
        assertThat(request.getType(), is(ApiKey.Type.CROSS_CLUSTER));
        assertThat(request.getName(), equalTo("my-key"));
        assertThat(
            request.getRoleDescriptors(),
            equalTo(
                List.of(
                    new RoleDescriptor(
                        "cross_cluster",
                        new String[] { "cross_cluster_search" },
                        new RoleDescriptor.IndicesPrivileges[] {
                            RoleDescriptor.IndicesPrivileges.builder()
                                .indices("logs")
                                .privileges("read", "read_cross_cluster", "view_index_metadata")
                                .build() },
                        null
                    )
                )
            )
        );
        assertThat(request.getMetadata(), nullValue());
    }
}
