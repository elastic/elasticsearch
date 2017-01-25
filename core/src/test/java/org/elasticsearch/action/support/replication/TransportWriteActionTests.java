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

package org.elasticsearch.action.support.replication;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.HashSet;
import java.util.function.Consumer;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class TransportWriteActionTests extends ESTestCase {
    private IndexShard indexShard;
    private Translog.Location location;

    @Before
    public void initCommonMocks() {
        indexShard = mock(IndexShard.class);
        location = mock(Translog.Location.class);
    }

    public void testPrimaryNoRefreshCall() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.NONE); // The default, but we'll set it anyway just to be explicit
        TestAction testAction = new TestAction();
        TransportWriteAction.WritePrimaryResult<TestRequest, TestResponse> result =
                testAction.shardOperationOnPrimary(request, indexShard);
        CapturingActionListener<TestResponse> listener = new CapturingActionListener<>();
        result.respond(listener);
        assertNotNull(listener.response);
        assertNull(listener.failure);
        verify(indexShard, never()).refresh(any());
        verify(indexShard, never()).addRefreshListener(any(), any());
    }

    public void testReplicaNoRefreshCall() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.NONE); // The default, but we'll set it anyway just to be explicit
        TestAction testAction = new TestAction();
        TransportWriteAction.WriteReplicaResult<TestRequest> result =
                testAction.shardOperationOnReplica(request, indexShard);
        CapturingActionListener<TransportResponse.Empty> listener = new CapturingActionListener<>();
        result.respond(listener);
        assertNotNull(listener.response);
        assertNull(listener.failure);
        verify(indexShard, never()).refresh(any());
        verify(indexShard, never()).addRefreshListener(any(), any());
    }

    public void testPrimaryImmediateRefresh() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        TestAction testAction = new TestAction();
        TransportWriteAction.WritePrimaryResult<TestRequest, TestResponse> result =
                testAction.shardOperationOnPrimary(request, indexShard);
        CapturingActionListener<TestResponse> listener = new CapturingActionListener<>();
        result.respond(listener);
        assertNotNull(listener.response);
        assertNull(listener.failure);
        assertTrue(listener.response.forcedRefresh);
        verify(indexShard).refresh("refresh_flag_index");
        verify(indexShard, never()).addRefreshListener(any(), any());
    }

    public void testReplicaImmediateRefresh() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        TestAction testAction = new TestAction();
        TransportWriteAction.WriteReplicaResult<TestRequest> result =
                testAction.shardOperationOnReplica(request, indexShard);
        CapturingActionListener<TransportResponse.Empty> listener = new CapturingActionListener<>();
        result.respond(listener);
        assertNotNull(listener.response);
        assertNull(listener.failure);
        verify(indexShard).refresh("refresh_flag_index");
        verify(indexShard, never()).addRefreshListener(any(), any());
    }

    public void testPrimaryWaitForRefresh() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);

        TestAction testAction = new TestAction();
        TransportWriteAction.WritePrimaryResult<TestRequest, TestResponse> result =
                testAction.shardOperationOnPrimary(request, indexShard);
        CapturingActionListener<TestResponse> listener = new CapturingActionListener<>();
        result.respond(listener);
        assertNull(listener.response); // Haven't reallresponded yet

        @SuppressWarnings({ "unchecked", "rawtypes" })
        ArgumentCaptor<Consumer<Boolean>> refreshListener = ArgumentCaptor.forClass((Class) Consumer.class);
        verify(indexShard, never()).refresh(any());
        verify(indexShard).addRefreshListener(any(), refreshListener.capture());

        // Now we can fire the listener manually and we'll get a response
        boolean forcedRefresh = randomBoolean();
        refreshListener.getValue().accept(forcedRefresh);
        assertNotNull(listener.response);
        assertNull(listener.failure);
        assertEquals(forcedRefresh, listener.response.forcedRefresh);
    }

    public void testReplicaWaitForRefresh() throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        TestAction testAction = new TestAction();
        TransportWriteAction.WriteReplicaResult<TestRequest> result = testAction.shardOperationOnReplica(request, indexShard);
        CapturingActionListener<TransportResponse.Empty> listener = new CapturingActionListener<>();
        result.respond(listener);
        assertNull(listener.response); // Haven't responded yet
        @SuppressWarnings({ "unchecked", "rawtypes" })
        ArgumentCaptor<Consumer<Boolean>> refreshListener = ArgumentCaptor.forClass((Class) Consumer.class);
        verify(indexShard, never()).refresh(any());
        verify(indexShard).addRefreshListener(any(), refreshListener.capture());

        // Now we can fire the listener manually and we'll get a response
        boolean forcedRefresh = randomBoolean();
        refreshListener.getValue().accept(forcedRefresh);
        assertNotNull(listener.response);
        assertNull(listener.failure);
    }

    public void testDocumentFailureInShardOperationOnPrimary() throws Exception {
        TestRequest request = new TestRequest();
        TestAction testAction = new TestAction(true, true);
        TransportWriteAction.WritePrimaryResult<TestRequest, TestResponse> writePrimaryResult =
                testAction.shardOperationOnPrimary(request, indexShard);
        CapturingActionListener<TestResponse> listener = new CapturingActionListener<>();
        writePrimaryResult.respond(listener);
        assertNull(listener.response);
        assertNotNull(listener.failure);
    }

    public void testDocumentFailureInShardOperationOnReplica() throws Exception {
        TestRequest request = new TestRequest();
        TestAction testAction = new TestAction(randomBoolean(), true);
        TransportWriteAction.WriteReplicaResult<TestRequest> writeReplicaResult =
                testAction.shardOperationOnReplica(request, indexShard);
        CapturingActionListener<TransportResponse.Empty> listener = new CapturingActionListener<>();
        writeReplicaResult.respond(listener);
        assertNull(listener.response);
        assertNotNull(listener.failure);
    }

    private class TestAction extends TransportWriteAction<TestRequest, TestRequest, TestResponse> {

        private final boolean withDocumentFailureOnPrimary;
        private final boolean withDocumentFailureOnReplica;

        protected TestAction() {
            this(false, false);
        }
        protected TestAction(boolean withDocumentFailureOnPrimary, boolean withDocumentFailureOnReplica) {
            super(Settings.EMPTY, "test",
                    new TransportService(Settings.EMPTY, null, null, TransportService.NOOP_TRANSPORT_INTERCEPTOR, x -> null, null),
                    null, null, null, null, new ActionFilters(new HashSet<>()), new IndexNameExpressionResolver(Settings.EMPTY),
                    TestRequest::new, TestRequest::new, ThreadPool.Names.SAME);
            this.withDocumentFailureOnPrimary = withDocumentFailureOnPrimary;
            this.withDocumentFailureOnReplica = withDocumentFailureOnReplica;
        }

        @Override
        protected TestResponse newResponseInstance() {
            return new TestResponse();
        }

        @Override
        protected WritePrimaryResult<TestRequest, TestResponse> shardOperationOnPrimary(
                TestRequest request, IndexShard primary) throws Exception {
            final WritePrimaryResult<TestRequest, TestResponse> primaryResult;
            if (withDocumentFailureOnPrimary) {
                primaryResult = new WritePrimaryResult<>(request, null, null, new RuntimeException("simulated"), primary, logger);
            } else {
                primaryResult = new WritePrimaryResult<>(request, new TestResponse(), location, null, primary, logger);
            }
            return primaryResult;
        }

        @Override
        protected WriteReplicaResult<TestRequest> shardOperationOnReplica(TestRequest request, IndexShard replica) throws Exception {
            final WriteReplicaResult<TestRequest> replicaResult;
            if (withDocumentFailureOnReplica) {
                replicaResult = new WriteReplicaResult<>(request, null, new RuntimeException("simulated"), replica, logger);
            } else {
                replicaResult = new WriteReplicaResult<>(request, location, null, replica, logger);
            }
            return replicaResult;
        }
    }

    private static class TestRequest extends ReplicatedWriteRequest<TestRequest> {
        public TestRequest() {
            setShardId(new ShardId("test", "test", 1));
        }
    }

    private static class TestResponse extends ReplicationResponse implements WriteResponse {
        boolean forcedRefresh;

        @Override
        public void setForcedRefresh(boolean forcedRefresh) {
            this.forcedRefresh = forcedRefresh;
        }
    }

    private static class CapturingActionListener<R> implements ActionListener<R> {
        private R response;
        private Exception failure;

        @Override
        public void onResponse(R response) {
            this.response = response;
        }

        @Override
        public void onFailure(Exception failure) {
            this.failure = failure;
        }
    }
}
