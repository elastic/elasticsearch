/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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
import org.elasticsearch.client.Client;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.transport.TransportService;

import java.util.List;

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
}
