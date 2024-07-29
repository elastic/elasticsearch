/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.BiFunction;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class TransportReplicationActionBypassCircuitBreakerOnReplicaIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPlugin.class);
    }

    public static class Request extends ReplicationRequest<Request> {
        public Request(ShardId shardId) {
            super(shardId);
        }

        public Request(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String toString() {
            return "test-request";
        }
    }

    public static class Response extends ReplicationResponse {
        public Response() {}

        public Response(StreamInput in) throws IOException {
            super(in);
        }
    }

    public static class TestAction extends TransportReplicationAction<Request, Request, Response> {
        private static final String ACTION_NAME = "internal:test-replication-action";
        private static final ActionType<Response> TYPE = new ActionType<>(ACTION_NAME);

        @Inject
        public TestAction(
            Settings settings,
            TransportService transportService,
            ClusterService clusterService,
            IndicesService indicesService,
            ThreadPool threadPool,
            ShardStateAction shardStateAction,
            ActionFilters actionFilters
        ) {
            super(
                settings,
                ACTION_NAME,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                actionFilters,
                Request::new,
                Request::new,
                threadPool.executor(ThreadPool.Names.GENERIC),
                SyncGlobalCheckpointAfterOperation.DoNotSync,
                PrimaryActionExecution.RejectOnOverload,
                ReplicaActionExecution.BypassCircuitBreaker
            );
        }

        @Override
        protected Response newResponseInstance(StreamInput in) throws IOException {
            return new Response(in);
        }

        @Override
        protected void shardOperationOnPrimary(
            Request shardRequest,
            IndexShard primary,
            ActionListener<PrimaryResult<Request, Response>> listener
        ) {
            listener.onResponse(new PrimaryResult<>(shardRequest, new Response()));
        }

        @Override
        protected void shardOperationOnReplica(Request shardRequest, IndexShard replica, ActionListener<ReplicaResult> listener) {
            listener.onResponse(new ReplicaResult());
        }
    }

    public static class TestPlugin extends Plugin implements ActionPlugin {

        public TestPlugin() {}

        @Override
        public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
            return List.of(new ActionHandler<>(TestAction.TYPE, TestAction.class));
        }
    }

    private enum PrimaryOrReplica implements BiFunction<String, String, String> {
        PRIMARY {
            @Override
            public String apply(String primaryName, String replicaName) {
                return primaryName;
            }
        },
        REPLICA {
            @Override
            public String apply(String primaryName, String replicaName) {
                return replicaName;
            }
        }
    }

    public void testActionCompletesWhenReplicaCircuitBreakersAreAtCapacity() {
        maxOutCircuitBreakersAndExecuteAction(PrimaryOrReplica.REPLICA);
    }

    public void testActionFailsWhenPrimaryCircuitBreakersAreAtCapacity() {
        AssertionError assertionError = assertThrows(
            AssertionError.class,
            () -> maxOutCircuitBreakersAndExecuteAction(PrimaryOrReplica.PRIMARY)
        );
        assertNotNull(
            "Not caused by CircuitBreakingException " + ExceptionsHelper.stackTrace(assertionError),
            ExceptionsHelper.unwrap(assertionError, CircuitBreakingException.class)
        );
    }

    private void maxOutCircuitBreakersAndExecuteAction(PrimaryOrReplica nodeToMaxOutCircuitBreakers) {
        internalCluster().startMasterOnlyNodes(2);
        String primary = internalCluster().startDataOnlyNode();
        assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            )
        );

        String replica = internalCluster().startDataOnlyNode();
        String coordinator = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        ensureGreen("test");

        try (
            var ignored = fullyAllocateCircuitBreakerOnNode(
                nodeToMaxOutCircuitBreakers.apply(primary, replica),
                CircuitBreaker.IN_FLIGHT_REQUESTS
            )
        ) {
            PlainActionFuture<Response> testActionResult = new PlainActionFuture<>();
            client(coordinator).execute(TestAction.TYPE, new Request(new ShardId(resolveIndex("test"), 0)), testActionResult);
            safeGet(testActionResult);
        }
    }
}
