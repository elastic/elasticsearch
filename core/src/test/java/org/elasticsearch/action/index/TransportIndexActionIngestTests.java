/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.index;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.PipelineExecutionService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class TransportIndexActionIngestTests extends ESTestCase {

    /** Services needed by index action */
    TransportService transportService;
    ClusterService clusterService;
    IngestService ingestService;

    /** The ingest execution service we can capture calls to */
    PipelineExecutionService executionService;

    /** Arguments to callbacks we want to capture, but which require generics, so we must use @Captor */
    @Captor
    ArgumentCaptor<Consumer<Exception>> exceptionHandler;
    @Captor
    ArgumentCaptor<Consumer<Boolean>> successHandler;
    @Captor
    ArgumentCaptor<TransportResponseHandler<IndexResponse>> remoteResponseHandler;

    /** The actual action we want to test, with real indexing mocked */
    TestTransportIndexAction action;

    /** True if the next call to the index action should act as an ingest node */
    boolean localIngest;

    /** The nodes that forwarded index requests should be cycled through. */
    DiscoveryNodes nodes;
    DiscoveryNode remoteNode1;
    DiscoveryNode remoteNode2;

    /** A subclass of the real index action to allow skipping real indexing, and marking when it would have happened. */
    class TestTransportIndexAction extends TransportIndexAction {
        boolean isExecuted = false; // set when the "real" index execution happens
        TestTransportIndexAction() {
            super(Settings.EMPTY, transportService, clusterService, null, ingestService, null, null, null, null,
                new ActionFilters(Collections.emptySet()), null, null);
        }
        @Override
        protected boolean shouldAutoCreate(IndexRequest reqest, ClusterState state) {
            return false;
        }
        @Override
        protected void innerExecute(Task task, final IndexRequest request, final ActionListener<IndexResponse> listener) {
            isExecuted = true;
        }
    }

    @Before
    public void setupAction() {
        // initialize captors, which must be members to use @Capture because of generics
        MockitoAnnotations.initMocks(this);
        // setup services that will be called by action
        transportService = mock(TransportService.class);
        clusterService = mock(ClusterService.class);
        localIngest = true;
        // setup nodes for local and remote
        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(localNode.isIngestNode()).thenAnswer(stub -> localIngest);
        when(clusterService.localNode()).thenReturn(localNode);
        remoteNode1 = mock(DiscoveryNode.class);
        remoteNode2 = mock(DiscoveryNode.class);
        nodes = mock(DiscoveryNodes.class);
        ImmutableOpenMap<String, DiscoveryNode> ingestNodes = ImmutableOpenMap.<String, DiscoveryNode>builder(2)
            .fPut("node1", remoteNode1).fPut("node2", remoteNode2).build();
        when(nodes.getIngestNodes()).thenReturn(ingestNodes);
        ClusterState state = mock(ClusterState.class);
        when(state.getNodes()).thenReturn(nodes);
        when(clusterService.state()).thenReturn(state);
        doAnswer(invocation -> {
            ClusterChangedEvent event = mock(ClusterChangedEvent.class);
            when(event.state()).thenReturn(state);
            ((ClusterStateApplier)invocation.getArguments()[0]).applyClusterState(event);
            return null;
        }).when(clusterService).addStateApplier(any(ClusterStateApplier.class));
        // setup the mocked ingest service for capturing calls
        ingestService = mock(IngestService.class);
        executionService = mock(PipelineExecutionService.class);
        when(ingestService.getPipelineExecutionService()).thenReturn(executionService);
        action = new TestTransportIndexAction();
        reset(transportService); // call on construction of action
    }

    public void testIngestSkipped() throws Exception {
        IndexRequest indexRequest = new IndexRequest("index", "type", "id");
        indexRequest.source(Collections.emptyMap());
        action.execute(null, indexRequest, ActionListener.wrap(response -> {}, exception -> {
            throw new AssertionError(exception);
        }));
        assertTrue(action.isExecuted);
        verifyZeroInteractions(ingestService);
    }

    public void testIngestLocal() throws Exception {
        Exception exception = new Exception("fake exception");
        IndexRequest indexRequest = new IndexRequest("index", "type", "id");
        indexRequest.source(Collections.emptyMap());
        indexRequest.setPipeline("testpipeline");
        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);
        action.execute(null, indexRequest, ActionListener.wrap(
            response -> {
                responseCalled.set(true);
            },
            e -> {
                assertThat(e, sameInstance(exception));
                failureCalled.set(true);
            }));

        // check failure works, and passes through to the listener
        assertFalse(action.isExecuted); // haven't executed yet
        assertFalse(responseCalled.get());
        assertFalse(failureCalled.get());
        verify(executionService).executeIndexRequest(same(indexRequest), exceptionHandler.capture(), successHandler.capture());
        exceptionHandler.getValue().accept(exception);
        assertTrue(failureCalled.get());

        // now check success
        successHandler.getValue().accept(true);
        assertTrue(action.isExecuted);
        assertFalse(responseCalled.get()); // listener would only be called by real index action, not our mocked one
        verifyZeroInteractions(transportService);
    }

    public void testIngestForward() throws Exception {
        localIngest = false;
        IndexRequest indexRequest = new IndexRequest("index", "type", "id");
        indexRequest.source(Collections.emptyMap());
        indexRequest.setPipeline("testpipeline");
        IndexResponse indexResponse = mock(IndexResponse.class);
        AtomicBoolean responseCalled = new AtomicBoolean(false);
        ActionListener<IndexResponse> listener = ActionListener.wrap(
            response -> {
                responseCalled.set(true);
                assertSame(indexResponse, response);
            },
            e -> {
                throw new AssertionError(e);
            });
        action.execute(null, indexRequest, listener);

        // should not have executed ingest locally
        verify(executionService, never()).executeIndexRequest(any(), any(), any());
        // but instead should have sent to a remote node with the transport service
        ArgumentCaptor<DiscoveryNode> node = ArgumentCaptor.forClass(DiscoveryNode.class);
        verify(transportService).sendRequest(node.capture(), eq(IndexAction.NAME), any(), remoteResponseHandler.capture());
        boolean usedNode1 = node.getValue() == remoteNode1; // make sure we used one of the nodes
        if (usedNode1 == false) {
            assertSame(remoteNode2, node.getValue());
        }
        assertFalse(action.isExecuted); // no local index execution
        assertFalse(responseCalled.get()); // listener not called yet

        remoteResponseHandler.getValue().handleResponse(indexResponse); // call the listener for the remote node
        assertTrue(responseCalled.get()); // now the listener we passed should have been delegated to by the remote listener
        assertFalse(action.isExecuted); // still no local index execution

        // now make sure ingest nodes are rotated through with a subsequent request
        reset(transportService);
        action.execute(null, indexRequest, listener);
        verify(transportService).sendRequest(node.capture(), eq(IndexAction.NAME), any(), remoteResponseHandler.capture());
        if (usedNode1) {
            assertSame(remoteNode2, node.getValue());
        } else {
            assertSame(remoteNode1, node.getValue());
        }
    }
}
