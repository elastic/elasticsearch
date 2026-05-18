/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.transform.action.DeleteTransformAction;
import org.junit.After;
import org.junit.Before;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class RestDeleteTransformActionTests extends ESTestCase {

    private static final TimeValue DEFAULT_TIMEOUT = TimeValue.timeValueSeconds(30);

    private final RestDeleteTransformAction handler = new RestDeleteTransformAction();
    private RestChannel channel;
    private NodeClient client;

    @Before
    public void initializeMocks() {
        channel = mock(RestChannel.class);
        client = mock(NodeClient.class);
    }

    @After
    public void verifyNoMoreInteractionsWithClient() {
        verifyNoMoreInteractions(client);
    }

    public void testBodyRejection() throws Exception {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            {
                builder.field("id", "my_id");
            }
            builder.endObject();
            final FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withContent(
                new BytesArray(builder.toString()),
                XContentType.JSON
            ).build();
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> handler.prepareRequest(request, client));
            assertThat(e.getMessage(), equalTo("delete transform requests can not have a request body"));
        }
    }

    public void testDefaults() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(Map.of("id", "my-id")).build();
        handler.handleRequest(request, channel, client);

        DeleteTransformAction.Request expectedActionRequest = new DeleteTransformAction.Request("my-id", false, false, DEFAULT_TIMEOUT);
        verify(client).execute(eq(DeleteTransformAction.INSTANCE), eq(expectedActionRequest), any());
    }

    public void testForce() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(
            Map.of("id", "my-id", "force", "true")
        ).build();
        handler.handleRequest(request, channel, client);

        DeleteTransformAction.Request expectedActionRequest = new DeleteTransformAction.Request("my-id", true, false, DEFAULT_TIMEOUT);
        verify(client).execute(eq(DeleteTransformAction.INSTANCE), eq(expectedActionRequest), any());
    }

    public void testDeleteDestIndex() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(
            Map.of("id", "my-id", "delete_dest_index", "true")
        ).build();
        handler.handleRequest(request, channel, client);

        DeleteTransformAction.Request expectedActionRequest = new DeleteTransformAction.Request("my-id", false, true, DEFAULT_TIMEOUT);
        verify(client).execute(eq(DeleteTransformAction.INSTANCE), eq(expectedActionRequest), any());
    }

    public void testTimeout() throws Exception {
        FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(
            Map.of("id", "my-id", "timeout", "45s")
        ).build();
        handler.handleRequest(request, channel, client);

        DeleteTransformAction.Request expectedActionRequest = new DeleteTransformAction.Request(
            "my-id",
            false,
            false,
            TimeValue.timeValueSeconds(45)
        );
        verify(client).execute(eq(DeleteTransformAction.INSTANCE), eq(expectedActionRequest), any());
    }
}
