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

package org.elasticsearch.indices.cluster;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.seqno.RetentionLeaseBackgroundSyncAction;
import org.elasticsearch.index.seqno.RetentionLeaseSyncAction;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.mock.orig.Mockito.verifyNoMoreInteractions;
import static org.elasticsearch.mock.orig.Mockito.when;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class IndicesClusterStateServiceTests extends ESTestCase {

    ThreadPool threadPool;

    @Before
    public void createServices() {
        threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
    }

    public void testRetentionLeaseBackgroundSyncExecution() {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);

        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        final Logger mockLogger = mock(Logger.class);
        final TaskManager taskManager = mock(TaskManager.class);
        when(taskManager.registerAndExecute(any(), any(), any(), any(), any())).thenCallRealMethod();
        final TransportService transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        final RetentionLeases retentionLeases = mock(RetentionLeases.class);
        final AtomicBoolean invoked = new AtomicBoolean();
        final RetentionLeaseBackgroundSyncAction action = new RetentionLeaseBackgroundSyncAction(
            Settings.EMPTY,
            transportService,
            null,
            indicesService,
            threadPool,
            null,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver()) {

            @Override
            protected void doExecute(Task task, RetentionLeaseBackgroundSyncAction.Request request,
                                     ActionListener<ReplicationResponse> listener) {
                assertTrue(threadPool.getThreadContext().isSystemContext());
                assertThat(request.shardId(), sameInstance(indexShard.shardId()));
                assertThat(request.getRetentionLeases(), sameInstance(retentionLeases));
                if (randomBoolean()) {
                    listener.onResponse(new ReplicationResponse());
                } else {
                    final Exception e = randomFrom(
                        new AlreadyClosedException("closed"),
                        new IndexShardClosedException(indexShard.shardId()),
                        new TransportException(randomFrom(
                            "failed",
                            "TransportService is closed stopped can't send request",
                            "transport stopped, action: indices:admin/seq_no/retention_lease_background_sync[p]")),
                        new RuntimeException("failed"));
                    listener.onFailure(e);
                    if (e.getMessage().equals("failed")) {
                        final ArgumentCaptor<ParameterizedMessage> captor = ArgumentCaptor.forClass(ParameterizedMessage.class);
                        verify(mockLogger).warn(captor.capture(), same(e));
                        final ParameterizedMessage message = captor.getValue();
                        assertThat(message.getFormat(), equalTo("{} retention lease background sync failed"));
                        assertThat(message.getParameters(), arrayContaining(indexShard.shardId()));
                    }
                    verifyNoMoreInteractions(mockLogger);
                }
                invoked.set(true);
            }
        };
        NodeClient client = new NodeClient(Settings.EMPTY, null);
        Map<ActionType, TransportAction> actions = Collections.singletonMap(RetentionLeaseBackgroundSyncAction.TYPE, action);
        client.initialize(actions, taskManager, null, null);
        IndicesClusterStateService service = new IndicesClusterStateService(Settings.EMPTY, null, null, threadPool, null, null, null,
            null, null, null, null, null, null, client) {
            @Override
            protected Logger getLogger() {
                return mockLogger;
            }
        };

        service.backgroundSync(indexShard.shardId(), retentionLeases);
        assertTrue(invoked.get());
    }


    public void testRetentionLeaseSyncExecution() {
        final IndicesService indicesService = mock(IndicesService.class);

        final Index index = new Index("index", "uuid");
        final IndexService indexService = mock(IndexService.class);
        when(indicesService.indexServiceSafe(index)).thenReturn(indexService);
        final int id = randomIntBetween(0, 4);
        final IndexShard indexShard = mock(IndexShard.class);
        when(indexService.getShard(id)).thenReturn(indexShard);

        final ShardId shardId = new ShardId(index, id);
        when(indexShard.shardId()).thenReturn(shardId);

        final Logger mockLogger = mock(Logger.class);
        final TaskManager taskManager = mock(TaskManager.class);
        when(taskManager.registerAndExecute(any(), any(), any(), any(), any())).thenCallRealMethod();
        final TransportService transportService = mock(TransportService.class);
        when(transportService.getTaskManager()).thenReturn(taskManager);

        final RetentionLeases retentionLeases = mock(RetentionLeases.class);
        final AtomicBoolean invoked = new AtomicBoolean();
        final RetentionLeaseSyncAction action = new RetentionLeaseSyncAction(
            Settings.EMPTY,
            transportService,
            null,
            indicesService,
            threadPool,
            null,
            new ActionFilters(Collections.emptySet()),
            new IndexNameExpressionResolver()) {

            @Override
            protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
                assertTrue(threadPool.getThreadContext().isSystemContext());
                assertThat(request.shardId(), sameInstance(indexShard.shardId()));
                assertThat(request.getRetentionLeases(), sameInstance(retentionLeases));
                if (randomBoolean()) {
                    listener.onResponse(new Response());
                } else {
                    final Exception e = randomFrom(
                        new AlreadyClosedException("closed"),
                        new IndexShardClosedException(indexShard.shardId()),
                        new RuntimeException("failed"));
                    listener.onFailure(e);
                    if (e instanceof AlreadyClosedException == false && e instanceof IndexShardClosedException == false) {
                        final ArgumentCaptor<ParameterizedMessage> captor = ArgumentCaptor.forClass(ParameterizedMessage.class);
                        verify(mockLogger).warn(captor.capture(), same(e));
                        final ParameterizedMessage message = captor.getValue();
                        assertThat(message.getFormat(), equalTo("{} retention lease sync failed"));
                        assertThat(message.getParameters(), arrayContaining(indexShard.shardId()));
                    }
                    verifyNoMoreInteractions(mockLogger);
                }
                invoked.set(true);
            }
        };
        NodeClient client = new NodeClient(Settings.EMPTY, null);
        Map<ActionType, TransportAction> actions = Collections.singletonMap(RetentionLeaseSyncAction.TYPE, action);
        client.initialize(actions, taskManager, null, null);
        IndicesClusterStateService service = new IndicesClusterStateService(Settings.EMPTY, null, null, threadPool, null, null, null,
            null, null, null, null, null, null, client) {
            @Override
            protected Logger getLogger() {
                return mockLogger;
            }
        };

        // execution happens on the test thread, so no need to register an actual listener to callback
        service.sync(indexShard.shardId(), retentionLeases, ActionListener.wrap(() -> {}));
        assertTrue(invoked.get());
    }

}
