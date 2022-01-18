/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.logstash.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class TransportDeletePipelineActionTests extends ESTestCase {

    public void testDeletePipelineWithMissingIndex() throws Exception {
        try (Client client = getFailureClient(new IndexNotFoundException("missing .logstash"))) {
            final TransportDeletePipelineAction action = new TransportDeletePipelineAction(
                mock(TransportService.class),
                mock(ActionFilters.class),
                client
            );
            final DeletePipelineRequest request = new DeletePipelineRequest(randomAlphaOfLength(4));
            final PlainActionFuture<DeletePipelineResponse> future = new PlainActionFuture<>();
            action.doExecute(null, request, future);
            assertThat(future.get().isDeleted(), is(false));
        }
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
