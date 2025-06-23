/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.rest.action;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.transform.action.ScheduleNowTransformAction;
import org.junit.Before;

import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class RestScheduleNowTransformActionTests extends ESTestCase {

    private static final String ID = "id";
    private static final String TIMEOUT = "timeout";

    private RestChannel channel;
    private NodeClient client;

    @Before
    public void initializeMocks() {
        channel = mock(RestChannel.class);
        client = mock(NodeClient.class);
    }

    public void testHandleRequest() throws Exception {
        RestScheduleNowTransformAction handler = new RestScheduleNowTransformAction();
        FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(Map.of(ID, "my-id")).build();

        handler.handleRequest(request, channel, client);

        ScheduleNowTransformAction.Request expectedActionRequest = new ScheduleNowTransformAction.Request(
            "my-id",
            TimeValue.timeValueSeconds(30)
        );
        verify(client).execute(eq(ScheduleNowTransformAction.INSTANCE), eq(expectedActionRequest), any());
        verifyNoMoreInteractions(client);
    }

    public void testHandleRequestWithTimeout() throws Exception {
        RestScheduleNowTransformAction handler = new RestScheduleNowTransformAction();
        FakeRestRequest request = new FakeRestRequest.Builder(NamedXContentRegistry.EMPTY).withParams(Map.of(ID, "my-id", TIMEOUT, "45s"))
            .build();

        handler.handleRequest(request, channel, client);

        ScheduleNowTransformAction.Request expectedActionRequest = new ScheduleNowTransformAction.Request(
            "my-id",
            TimeValue.timeValueSeconds(45)
        );
        verify(client).execute(eq(ScheduleNowTransformAction.INSTANCE), eq(expectedActionRequest), any());
        verifyNoMoreInteractions(client);
    }
}
