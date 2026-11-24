/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.action.EsqlStreamDeleteAction;
import org.elasticsearch.xpack.esql.streaming.AbstractStreamTestCase;
import org.elasticsearch.xpack.esql.streaming.DelegatingStreamStorage;
import org.elasticsearch.xpack.esql.streaming.DeleteStreamResultRequest;
import org.elasticsearch.xpack.esql.streaming.StreamId;
import org.elasticsearch.xpack.esql.streaming.StreamMetadata;
import org.elasticsearch.xpack.esql.streaming.StreamStorage;
import org.junit.After;
import org.junit.Before;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * Where a delete runs: on the node named in the stream id, as the async delete does, because only that node can cancel the query.
 * The transport of the base class sends nothing and records what would be sent.
 */
public class TransportEsqlStreamDeleteActionTests extends AbstractStreamTestCase {

    private static final String OTHER_NODE_ID = "other-node";

    private ClusterService clusterService;
    private DiscoveryNode otherNode;
    private final AtomicInteger storageCalls = new AtomicInteger();
    private TransportEsqlStreamDeleteAction action;

    @Before
    public void setUpAction() {
        otherNode = DiscoveryNodeUtils.create(OTHER_NODE_ID);
        clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode);
        ClusterServiceUtils.setState(
            clusterService,
            ClusterState.builder(clusterService.state())
                .nodes(
                    DiscoveryNodes.builder().add(localNode).add(otherNode).localNodeId(localNode.getId()).masterNodeId(localNode.getId())
                )
        );
        StreamStorage storage = new DelegatingStreamStorage(store) {
            @Override
            public void getMetadata(String streamId, ActionListener<StreamMetadata> listener) {
                storageCalls.incrementAndGet();
                super.getMetadata(streamId, listener);
            }

            @Override
            public void delete(String streamId, ActionListener<Void> listener) {
                storageCalls.incrementAndGet();
                super.delete(streamId, listener);
            }
        };
        // No user is authenticated in these tests, so the client that would look up privileges is not called.
        action = new TransportEsqlStreamDeleteAction(
            transportService,
            new ActionFilters(Set.of()),
            clusterService,
            threadPool,
            new NoOpClient(threadPool)
        ) {
            @Override
            protected StreamStorage createStreamStorage(ClusterService clusterService, ThreadPool threadPool, Client client) {
                return storage;
            }
        };
    }

    @After
    public void tearDownAction() {
        clusterService.close();
    }

    private PlainActionFuture<AcknowledgedResponse> delete(String id) {
        PlainActionFuture<AcknowledgedResponse> future = new PlainActionFuture<>();
        action.execute(null, new DeleteStreamResultRequest(id), future);
        return future;
    }

    public void testRunsLocallyWhenTheIdNamesThisNode() {
        CancellableTask task = registerQueryTask();
        String id = createStream(streamId(task));

        assertThat(safeGet(delete(id)).isAcknowledged(), is(true));

        assertThat(transport.capturedRequests(), emptyArray());
        assertThat(task.isCancelled(), is(true));
        assertThat(metadata(id), equalTo(null));
    }

    public void testForwardsToTheNodeInTheId() {
        String id = createStream(new StreamId(randomAlphaOfLength(10), new TaskId(OTHER_NODE_ID, randomNonNegativeLong())));

        PlainActionFuture<AcknowledgedResponse> future = delete(id);

        CapturingTransport.CapturedRequest[] sent = transport.capturedRequests();
        assertThat(sent, arrayWithSize(1));
        assertThat(sent[0].node(), equalTo(otherNode));
        assertThat(sent[0].action(), equalTo(EsqlStreamDeleteAction.NAME));
        assertThat(sent[0].request(), equalTo(new DeleteStreamResultRequest(id)));
        // The node is known from the id alone. This node does not read the storage, and it deletes nothing: the other node does.
        assertThat(storageCalls.get(), equalTo(0));
        assertThat(future.isDone(), is(false));

        transport.handleResponse(sent[0].requestId(), AcknowledgedResponse.TRUE);
        assertThat(safeGet(future).isAcknowledged(), is(true));
        assertThat(storageCalls.get(), equalTo(0));
    }

    public void testFailureOfTheOtherNodeIsPassedOn() {
        String id = createStream(new StreamId(randomAlphaOfLength(10), new TaskId(OTHER_NODE_ID, randomNonNegativeLong())));

        PlainActionFuture<AcknowledgedResponse> future = delete(id);
        CapturingTransport.CapturedRequest[] sent = transport.capturedRequests();
        assertThat(sent, arrayWithSize(1));
        Exception remote = randomBoolean() ? new ResourceNotFoundException(id) : new ElasticsearchException("simulated storage failure");
        transport.handleRemoteError(sent[0].requestId(), remote);

        // the status the other node answered with is the status of this delete: not found stays not found, a failure stays a failure
        Exception e = expectThrows(Exception.class, future::actionGet);
        assertThat(e, instanceOf(remote.getClass()));
        assertThat(e.getMessage(), equalTo(remote.getMessage()));
        assertThat(ExceptionsHelper.status(e), equalTo(ExceptionsHelper.status(remote)));
    }

    /** The node that ran the query has left the cluster: nothing is left to cancel, so this node deletes what is stored. */
    public void testRunsLocallyWhenTheNodeIsGone() {
        CancellableTask localTaskWithTheSameNumber = registerQueryTask();
        String id = createStream(new StreamId(randomAlphaOfLength(10), new TaskId("a-node-that-left", localTaskWithTheSameNumber.getId())));

        assertThat(safeGet(delete(id)).isAcknowledged(), is(true));

        assertThat(transport.capturedRequests(), emptyArray());
        assertThat(metadata(id), equalTo(null));
        assertThat(localTaskWithTheSameNumber.isCancelled(), is(false));
    }

    public void testUnknownStreamOfThisNodeIsNotFound() {
        String id = streamId(randomNonNegativeLong()).encoded();

        Exception e = expectThrows(ResourceNotFoundException.class, delete(id)::actionGet);

        assertThat(e.getMessage(), equalTo(id));
        assertThat(transport.capturedRequests(), emptyArray());
    }

    /** Async answers "400 invalid id" here. A stream is not found, as for any other string that is not a stored stream. */
    public void testMalformedIdIsNotFoundAndNotForwarded() {
        for (String malformed : new String[] { "wrong", "*", "_all", "a,b", streamId(1).encoded() + "x" }) {
            Exception e = expectThrows(ResourceNotFoundException.class, delete(malformed)::actionGet);
            assertThat(e.getMessage(), equalTo(malformed));
        }
        assertThat(transport.capturedRequests(), emptyArray());
        assertThat(storageCalls.get(), equalTo(0));
    }

    public void testMissingIdIsAValidationError() {
        Exception e = expectThrows(Exception.class, delete(randomBoolean() ? null : "")::actionGet);
        assertThat(e.getMessage(), equalTo("Validation Failed: 1: id is required;"));
        assertThat(storageCalls.get(), equalTo(0));
    }
}
