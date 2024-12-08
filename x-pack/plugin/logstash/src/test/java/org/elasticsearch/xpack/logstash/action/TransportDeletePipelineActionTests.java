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
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.TransportService;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class TransportDeletePipelineActionTests extends ESTestCase {

    public void testDeletePipelineWithMissingIndex() throws Exception {
        try (var threadPool = createThreadPool()) {
            final var client = getFailureClient(threadPool, new IndexNotFoundException("missing .logstash"));
            TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor();
            final TransportDeletePipelineAction action = new TransportDeletePipelineAction(
                transportService,
                mock(ActionFilters.class),
                client
            );
            final DeletePipelineRequest request = new DeletePipelineRequest(randomAlphaOfLength(4));
            final PlainActionFuture<DeletePipelineResponse> future = new PlainActionFuture<>();
            action.doExecute(null, request, future);
            assertThat(future.get().isDeleted(), is(false));
        }
    }

    private Client getFailureClient(ThreadPool threadPool, Exception e) {
        return new NoOpClient(threadPool) {
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
