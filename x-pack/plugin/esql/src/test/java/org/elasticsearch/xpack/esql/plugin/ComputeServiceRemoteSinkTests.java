/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.compute.operator.exchange.ExchangeResponse;
import org.elasticsearch.compute.operator.exchange.RemoteSink;
import org.elasticsearch.compute.test.TestBlockFactory;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.transport.SendRequestTransportException;

import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class ComputeServiceRemoteSinkTests extends ESTestCase {

    private static final DiscoveryNode TARGET = DiscoveryNodeUtils.create("target");

    public void testRequestNotSentFailsOnlyAfterSinkIsClosed() {
        RecordingRemoteSink sink = new RecordingRemoteSink();
        PlainActionFuture<String> future = new PlainActionFuture<>();
        Exception failure = new SendRequestTransportException(
            TARGET,
            ComputeService.DATA_ACTION_NAME,
            new CircuitBreakingException("tripped", CircuitBreaker.Durability.TRANSIENT)
        );

        ComputeService.finishRemoteSinkIfNotSent(sink, future).onFailure(failure);

        assertNotNull("the sink must be closed", sink.closeListener);
        assertFalse("the failure must wait until the target answers the close", future.isDone());
        // The original failure propagates whether or not the target acknowledges the close.
        if (randomBoolean()) {
            sink.closeListener.onResponse(new ExchangeResponse(TestBlockFactory.getNonBreakingInstance(), null, true));
        } else {
            sink.closeListener.onFailure(new TaskCancelledException("cancelled"));
        }
        assertThat(failureOf(future), sameInstance(failure));
    }

    public void testFailureAfterSendLeavesSinkOpen() {
        RecordingRemoteSink sink = new RecordingRemoteSink();
        PlainActionFuture<String> future = new PlainActionFuture<>();
        Exception failure = randomFrom(
            new NodeDisconnectedException(TARGET, ComputeService.DATA_ACTION_NAME),
            new RemoteTransportException("failed on target", new ElasticsearchException("boom"))
        );

        ComputeService.finishRemoteSinkIfNotSent(sink, future).onFailure(failure);

        assertNull(sink.closeListener);
        assertThat(failureOf(future), sameInstance(failure));
    }

    public void testResponseLeavesSinkOpen() throws Exception {
        RecordingRemoteSink sink = new RecordingRemoteSink();
        PlainActionFuture<String> future = new PlainActionFuture<>();

        ComputeService.finishRemoteSinkIfNotSent(sink, future).onResponse("done");

        assertNull(sink.closeListener);
        assertThat(future.get(), equalTo("done"));
    }

    private static Throwable failureOf(PlainActionFuture<?> future) {
        assertTrue(future.isDone());
        return expectThrows(ExecutionException.class, future::get).getCause();
    }

    /** Holds the close request so the test decides when, and how, the target answers it. */
    private static class RecordingRemoteSink implements RemoteSink {
        private ActionListener<ExchangeResponse> closeListener;

        @Override
        public void fetchPageAsync(boolean allSourcesFinished, ActionListener<ExchangeResponse> listener) {
            assertTrue("only a close is expected", allSourcesFinished);
            assertNull("closed twice", closeListener);
            closeListener = listener;
        }
    }
}
