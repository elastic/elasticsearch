/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;

import java.util.List;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetPipelineActionTests extends ESTestCase {

    /**
     * Test that an error message is logged on a partial failure of
     * a TransportGetPipelineAction.
     */
    public void testGetPipelineMultipleIDsPartialFailure() throws Exception {
        // Set up a log appender for detecting log messages
        final MockLogAppender mockLogAppender = new MockLogAppender();
        mockLogAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "message",
                "org.elasticsearch.xpack.logstash.action.TransportGetPipelineAction",
                Level.INFO,
                "Could not retrieve logstash pipelines with ids: [2]"
            )
        );
        mockLogAppender.start();
        final Logger logger = LogManager.getLogger(TransportGetPipelineAction.class);

        // Set up a MultiGetResponse
        GetResponse mockResponse = mock(GetResponse.class);
        when(mockResponse.getId()).thenReturn("1");
        when(mockResponse.getSourceAsBytesRef()).thenReturn(mock(BytesReference.class));
        when(mockResponse.isExists()).thenReturn(true);
        MultiGetResponse.Failure failure = mock(MultiGetResponse.Failure.class);
        when(failure.getId()).thenReturn("2");
        MultiGetResponse multiGetResponse = new MultiGetResponse(
            new MultiGetItemResponse[] { new MultiGetItemResponse(mockResponse, null), new MultiGetItemResponse(null, failure) }
        );

        GetPipelineRequest request = new GetPipelineRequest(List.of("1", "2"));

        // Set up an ActionListener for the actual test conditions
        ActionListener<GetPipelineResponse> testActionListener = new ActionListener<>() {
            @Override
            public void onResponse(GetPipelineResponse getPipelineResponse) {
                // check successful pipeline get
                assertThat(getPipelineResponse, is(notNullValue()));
                assertThat(getPipelineResponse.pipelines().size(), equalTo(1));

                // check that failed pipeline get is logged
                mockLogAppender.assertAllExpectationsMatched();
            }

            @Override
            public void onFailure(Exception e) {
                // do nothing
            }
        };

        try (Client client = getMockClient(multiGetResponse)) {
            Loggers.addAppender(logger, mockLogAppender);
            TransportGetPipelineAction action = new TransportGetPipelineAction(
                mock(TransportService.class),
                mock(ActionFilters.class),
                client
            );
            action.doExecute(null, request, testActionListener);
        } finally {
            Loggers.removeAppender(logger, mockLogAppender);
            mockLogAppender.stop();
        }
    }

    public void testMissingIndexHandling() throws Exception {
        try (Client failureClient = getFailureClient(new IndexNotFoundException("foo"))) {
            final TransportGetPipelineAction action = new TransportGetPipelineAction(
                mock(TransportService.class),
                mock(ActionFilters.class),
                failureClient
            );
            final List<String> pipelines = randomList(0, 10, () -> randomAlphaOfLengthBetween(1, 8));
            final GetPipelineRequest request = new GetPipelineRequest(pipelines);
            PlainActionFuture<GetPipelineResponse> future = new PlainActionFuture<>();
            action.doExecute(null, request, future);
            assertThat(future.get().pipelines(), anEmptyMap());
        }
    }

    private Client getMockClient(ActionResponse response) {
        return new NoOpClient(getTestName()) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listener.onResponse((Response) response);
            }
        };
    }

    private Client getFailureClient(Exception e) {
        return new NoOpClient(getTestName()) {
            @Override
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (randomBoolean()) {
                    listener.onFailure(new RemoteTransportException("failed on other node", e));
                } else {
                    listener.onFailure(e);
                }
            }
        };
    }
}
