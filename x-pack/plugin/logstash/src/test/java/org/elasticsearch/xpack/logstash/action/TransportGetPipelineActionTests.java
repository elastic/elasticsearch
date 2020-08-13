/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.logstash.action;

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
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.transport.TransportService;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetPipelineActionTests extends ESTestCase {

    public void testGetPipeline() {

        GetResponse mockResponse = mock(GetResponse.class);
        when(mockResponse.getId()).thenReturn("1");
        when(mockResponse.getSourceAsBytesRef()).thenReturn(mock(BytesReference.class));
        when(mockResponse.isExists()).thenReturn(true);
        MultiGetResponse.Failure failure = mock(MultiGetResponse.Failure.class);

        MultiGetResponse mgr = new MultiGetResponse(
            new MultiGetItemResponse[] { new MultiGetItemResponse(mockResponse, null), new MultiGetItemResponse(null, failure) }
        );
        Client client = new NoOpClient(getTestName()) {
            @Override
            @SuppressWarnings("unchecked")
            protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                listener.onResponse((Response) mgr);
            }
        };
        TransportGetPipelineAction action = new TransportGetPipelineAction(mock(TransportService.class), mock(ActionFilters.class), client);
        GetPipelineRequest request = new GetPipelineRequest(List.of("1", "2"));

        ActionListener<GetPipelineResponse> testActionListener = new ActionListener<>() {
            @Override
            public void onResponse(GetPipelineResponse getPipelineResponse) {
                assertThat(getPipelineResponse, is(notNullValue()));
                assertThat(getPipelineResponse.pipelines().size(), equalTo(1));

                // todo - the multiget response has a contrived failure, but it has been swallowed
                // silently. We need to either attach failures to GetPipelineResponse or log a warning
                // and test for it here
            }

            @Override
            public void onFailure(Exception e) {

            }
        };

        action.doExecute(null, request, testActionListener);

        client.close();
    }
}
