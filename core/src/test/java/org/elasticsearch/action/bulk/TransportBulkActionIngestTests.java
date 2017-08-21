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

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.PipelineExecutionService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class TransportBulkActionIngestTests extends ESTestCase {

    /** Services needed by bulk action */
    TransportService transportService;
    ClusterService clusterService;
    IngestService ingestService;

    /** The ingest execution service we can capture calls to */
    PipelineExecutionService executionService;

    /** Arguments to callbacks we want to capture, but which require generics, so we must use @Captor */
    @Captor
    ArgumentCaptor<BiConsumer<IndexRequest, Exception>> failureHandler;
    @Captor
    ArgumentCaptor<Consumer<Exception>> completionHandler;
    @Captor
    ArgumentCaptor<TransportResponseHandler<BulkResponse>> remoteResponseHandler;
    @Captor
    ArgumentCaptor<Iterable<DocWriteRequest>> bulkDocsItr;

    /** The actual action we want to test, with real indexing mocked */
    TestTransportBulkAction action;

    /** Single item bulk write action that wraps index requests */
    TestSingleItemBulkWriteAction singleItemBulkWriteAction;

    /** True if the next call to the index action should act as an ingest node */
    boolean localIngest;

    /** The nodes that forwarded index requests should be cycled through. */
    DiscoveryNodes nodes;
    DiscoveryNode remoteNode1;
    DiscoveryNode remoteNode2;

    /** A subclass of the real bulk action to allow skipping real bulk indexing, and marking when it would have happened. */
    class TestTransportBulkAction extends TransportBulkAction {
        boolean isExecuted = false; // set when the "real" bulk execution happens
        TestTransportBulkAction() {
            super(Settings.EMPTY, null, transportService, clusterService, ingestService,
                null, null, new ActionFilters(Collections.emptySet()), null, null);
        }
        @Override
        protected boolean needToCheck() {
            return false;
        }
        @Override
        void executeBulk(Task task, final BulkRequest bulkRequest, final long startTimeNanos, final ActionListener<BulkResponse> listener,
                final AtomicArray<BulkItemResponse> responses, Map<String, IndexNotFoundException> indicesThatCannotBeCreated) {
            isExecuted = true;
        }
    }

    class TestSingleItemBulkWriteAction extends TransportSingleItemBulkWriteAction<IndexRequest, IndexResponse> {

        TestSingleItemBulkWriteAction(TestTransportBulkAction bulkAction) {
            super(Settings.EMPTY, IndexAction.NAME, TransportBulkActionIngestTests.this.transportService,
                    TransportBulkActionIngestTests.this.clusterService,
                    null, null, null, new ActionFilters(Collections.emptySet()), null,
                    IndexRequest::new, IndexRequest::new, ThreadPool.Names.INDEX, bulkAction, null);
        }

        @Override
        protected IndexResponse newResponseInstance() {
            return new IndexResponse();
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
        action = new TestTransportBulkAction();
        singleItemBulkWriteAction = new TestSingleItemBulkWriteAction(action);
        reset(transportService); // call on construction of action
    }

    public void testIngestSkipped() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest("index", "type", "id");
        indexRequest.source(Collections.emptyMap());
        bulkRequest.add(indexRequest);
        action.execute(null, bulkRequest, ActionListener.wrap(response -> {}, exception -> {
            throw new AssertionError(exception);
        }));
        assertTrue(action.isExecuted);
        verifyZeroInteractions(ingestService);
    }

    public void testSingleItemBulkActionIngestSkipped() throws Exception {
        IndexRequest indexRequest = new IndexRequest("index", "type", "id");
        indexRequest.source(Collections.emptyMap());
        singleItemBulkWriteAction.execute(null, indexRequest, ActionListener.wrap(response -> {}, exception -> {
            throw new AssertionError(exception);
        }));
        assertTrue(action.isExecuted);
        verifyZeroInteractions(ingestService);
    }

    public void testIngestLocal() throws Exception {
        Exception exception = new Exception("fake exception");
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest1 = new IndexRequest("index", "type", "id");
        indexRequest1.source(Collections.emptyMap());
        indexRequest1.setPipeline("testpipeline");
        IndexRequest indexRequest2 = new IndexRequest("index", "type", "id");
        indexRequest2.source(Collections.emptyMap());
        indexRequest2.setPipeline("testpipeline");
        bulkRequest.add(indexRequest1);
        bulkRequest.add(indexRequest2);

        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);
        action.execute(null, bulkRequest, ActionListener.wrap(
            response -> {
                BulkItemResponse itemResponse = response.iterator().next();
                assertThat(itemResponse.getFailure().getMessage(), containsString("fake exception"));
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
        verify(executionService).executeBulkRequest(bulkDocsItr.capture(), failureHandler.capture(), completionHandler.capture());
        completionHandler.getValue().accept(exception);
        assertTrue(failureCalled.get());

        // now check success
        Iterator<DocWriteRequest> req = bulkDocsItr.getValue().iterator();
        failureHandler.getValue().accept((IndexRequest)req.next(), exception); // have an exception for our one index request
        indexRequest2.setPipeline(null); // this is done by the real pipeline execution service when processing
        completionHandler.getValue().accept(null);
        assertTrue(action.isExecuted);
        assertFalse(responseCalled.get()); // listener would only be called by real index action, not our mocked one
        verifyZeroInteractions(transportService);
    }

    public void testSingleItemBulkActionIngestLocal() throws Exception {
        Exception exception = new Exception("fake exception");
        IndexRequest indexRequest = new IndexRequest("index", "type", "id");
        indexRequest.source(Collections.emptyMap());
        indexRequest.setPipeline("testpipeline");
        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);
        singleItemBulkWriteAction.execute(null, indexRequest, ActionListener.wrap(
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
        verify(executionService).executeBulkRequest(bulkDocsItr.capture(), failureHandler.capture(), completionHandler.capture());
        completionHandler.getValue().accept(exception);
        assertTrue(failureCalled.get());

        // now check success
        indexRequest.setPipeline(null); // this is done by the real pipeline execution service when processing
        completionHandler.getValue().accept(null);
        assertTrue(action.isExecuted);
        assertFalse(responseCalled.get()); // listener would only be called by real index action, not our mocked one
        verifyZeroInteractions(transportService);
    }

    public void testIngestForward() throws Exception {
        localIngest = false;
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest("index", "type", "id");
        indexRequest.source(Collections.emptyMap());
        indexRequest.setPipeline("testpipeline");
        bulkRequest.add(indexRequest);
        BulkResponse bulkResponse = mock(BulkResponse.class);
        AtomicBoolean responseCalled = new AtomicBoolean(false);
        ActionListener<BulkResponse> listener = ActionListener.wrap(
            response -> {
                responseCalled.set(true);
                assertSame(bulkResponse, response);
            },
            e -> {
                throw new AssertionError(e);
            });
        action.execute(null, bulkRequest, listener);

        // should not have executed ingest locally
        verify(executionService, never()).executeBulkRequest(any(), any(), any());
        // but instead should have sent to a remote node with the transport service
        ArgumentCaptor<DiscoveryNode> node = ArgumentCaptor.forClass(DiscoveryNode.class);
        verify(transportService).sendRequest(node.capture(), eq(BulkAction.NAME), any(), remoteResponseHandler.capture());
        boolean usedNode1 = node.getValue() == remoteNode1; // make sure we used one of the nodes
        if (usedNode1 == false) {
            assertSame(remoteNode2, node.getValue());
        }
        assertFalse(action.isExecuted); // no local index execution
        assertFalse(responseCalled.get()); // listener not called yet

        remoteResponseHandler.getValue().handleResponse(bulkResponse); // call the listener for the remote node
        assertTrue(responseCalled.get()); // now the listener we passed should have been delegated to by the remote listener
        assertFalse(action.isExecuted); // still no local index execution

        // now make sure ingest nodes are rotated through with a subsequent request
        reset(transportService);
        action.execute(null, bulkRequest, listener);
        verify(transportService).sendRequest(node.capture(), eq(BulkAction.NAME), any(), remoteResponseHandler.capture());
        if (usedNode1) {
            assertSame(remoteNode2, node.getValue());
        } else {
            assertSame(remoteNode1, node.getValue());
        }
    }

    public void testSingleItemBulkActionIngestForward() throws Exception {
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
        singleItemBulkWriteAction.execute(null, indexRequest, listener);

        // should not have executed ingest locally
        verify(executionService, never()).executeBulkRequest(any(), any(), any());
        // but instead should have sent to a remote node with the transport service
        ArgumentCaptor<DiscoveryNode> node = ArgumentCaptor.forClass(DiscoveryNode.class);
        verify(transportService).sendRequest(node.capture(), eq(BulkAction.NAME), any(), remoteResponseHandler.capture());
        boolean usedNode1 = node.getValue() == remoteNode1; // make sure we used one of the nodes
        if (usedNode1 == false) {
            assertSame(remoteNode2, node.getValue());
        }
        assertFalse(action.isExecuted); // no local index execution
        assertFalse(responseCalled.get()); // listener not called yet

        BulkItemResponse itemResponse = new BulkItemResponse(0, DocWriteRequest.OpType.CREATE, indexResponse);
        BulkItemResponse[] bulkItemResponses = new BulkItemResponse[1];
        bulkItemResponses[0] = itemResponse;
        remoteResponseHandler.getValue().handleResponse(new BulkResponse(bulkItemResponses, 0)); // call the listener for the remote node
        assertTrue(responseCalled.get()); // now the listener we passed should have been delegated to by the remote listener
        assertFalse(action.isExecuted); // still no local index execution

        // now make sure ingest nodes are rotated through with a subsequent request
        reset(transportService);
        singleItemBulkWriteAction.execute(null, indexRequest, listener);
        verify(transportService).sendRequest(node.capture(), eq(BulkAction.NAME), any(), remoteResponseHandler.capture());
        if (usedNode1) {
            assertSame(remoteNode2, node.getValue());
        } else {
            assertSame(remoteNode1, node.getValue());
        }
    }

}
