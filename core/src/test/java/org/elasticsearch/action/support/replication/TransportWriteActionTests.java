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

import java.util.HashSet;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.TransportWriteAction.RespondingWriteResult;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.Translog.Location;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

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
        noRefreshCall(TestAction::shardOperationOnPrimary, r -> assertFalse(r.forcedRefresh));
    }

    public void testReplicaNoRefreshCall() throws Exception {
        noRefreshCall(TestAction::shardOperationOnReplica, r -> {});
    }

    private <R> void noRefreshCall(ThrowingBiFunction<TestAction, TestRequest, RespondingWriteResult<R>> action, Consumer<R> resultChecker)
            throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.NONE); // The default, but we'll set it anyway just to be explicit
        RespondingWriteResult<R> result = action.apply(new TestAction(), request);
        CapturingActionListener<R> listener = new CapturingActionListener<>();
        result.respond(listener);
        assertNotNull(listener.response);
        verify(indexShard, never()).refresh(any());
        verify(indexShard, never()).addRefreshListener(any(), any());
    }

    public void testPrimaryImmediateRefresh() throws Exception {
        immediateRefresh(TestAction::shardOperationOnPrimary, r -> assertTrue(r.forcedRefresh));
    }

    public void testReplicaImmediateRefresh() throws Exception {
        immediateRefresh(TestAction::shardOperationOnReplica, r -> {});
    }

    private <R> void immediateRefresh(ThrowingBiFunction<TestAction, TestRequest, RespondingWriteResult<R>> action,
            Consumer<R> resultChecker) throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.IMMEDIATE);
        RespondingWriteResult<R> result = action.apply(new TestAction(), request);
        CapturingActionListener<R> listener = new CapturingActionListener<>();
        result.respond(listener);
        assertNotNull(listener.response);
        resultChecker.accept(listener.response);
        verify(indexShard).refresh("refresh_flag_index");
        verify(indexShard, never()).addRefreshListener(any(), any());
    }

    public void testPrimaryWaitForRefresh() throws Exception {
        waitForRefresh(TestAction::shardOperationOnPrimary, (r, forcedRefresh) -> assertEquals(forcedRefresh, r.forcedRefresh));
    }

    public void testReplicaWaitForRefresh() throws Exception {
        waitForRefresh(TestAction::shardOperationOnReplica, (r, forcedRefresh) -> {});
    }

    private <R> void waitForRefresh(ThrowingBiFunction<TestAction, TestRequest, RespondingWriteResult<R>> action,
            BiConsumer<R, Boolean> resultChecker) throws Exception {
        TestRequest request = new TestRequest();
        request.setRefreshPolicy(RefreshPolicy.WAIT_UNTIL);
        RespondingWriteResult<R> result = action.apply(new TestAction(), request);
        CapturingActionListener<R> listener = new CapturingActionListener<>();
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
        resultChecker.accept(listener.response, forcedRefresh);
    }

    private class TestAction extends TransportWriteAction<TestRequest, TestResponse> {
        protected TestAction() {
            super(Settings.EMPTY, "test", mock(TransportService.class), null, null, null, null, new ActionFilters(new HashSet<>()),
                    new IndexNameExpressionResolver(Settings.EMPTY), TestRequest::new, ThreadPool.Names.SAME);
        }

        @Override
        protected IndexShard indexShard(TestRequest request) {
            return indexShard;
        }

        @Override
        protected WriteResult<TestResponse> onPrimaryShard(TestRequest request, IndexShard indexShard) throws Exception {
            return new WriteResult<>(new TestResponse(), location);
        }

        @Override
        protected Location onReplicaShard(TestRequest request, IndexShard indexShard) {
            return location;
        }

        @Override
        protected TestResponse newResponseInstance() {
            return new TestResponse();
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

        @Override
        public void onResponse(R response) {
            this.response = response;
        }

        @Override
        public void onFailure(Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private interface ThrowingBiFunction<A, B, R> {
        R apply(A a, B b) throws Exception;
    }
}
