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
package org.elasticsearch.action.resync;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.PrimaryReplicaSyncer;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.util.function.Supplier;

public class TransportResyncReplicationAction extends TransportWriteAction<ResyncReplicationRequest,
    ResyncReplicationRequest, ResyncReplicationResponse> implements PrimaryReplicaSyncer.SyncAction {

    private static String ACTION_NAME = "internal:index/seq_no/resync";

    @Inject
    public TransportResyncReplicationAction(Settings settings, TransportService transportService,
                                            ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool,
                                            ShardStateAction shardStateAction, ActionFilters actionFilters,
                                            IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings, ACTION_NAME, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
            indexNameExpressionResolver, ResyncReplicationRequest::new, ResyncReplicationRequest::new, ThreadPool.Names.BULK);
    }

    @Override
    protected void registerRequestHandlers(String actionName, TransportService transportService, Supplier<ResyncReplicationRequest> request,
                                           Supplier<ResyncReplicationRequest> replicaRequest, String executor) {
        transportService.registerRequestHandler(actionName, request, ThreadPool.Names.SAME, new OperationTransportHandler());
        // we should never reject resync because of thread pool capacity on primary
        transportService.registerRequestHandler(transportPrimaryAction,
            () -> new ConcreteShardRequest<>(request),
            executor, true, true,
            new PrimaryOperationTransportHandler());
        transportService.registerRequestHandler(transportReplicaAction,
            () -> new ConcreteReplicaRequest<>(replicaRequest),
            executor, true, true,
            new ReplicaOperationTransportHandler());
    }

    @Override
    protected ResyncReplicationResponse newResponseInstance() {
        return new ResyncReplicationResponse();
    }

    @Override
    protected ReplicationOperation.Replicas newReplicasProxy(long primaryTerm) {
        // We treat the resync as best-effort for now and don't mark unavailable shard copies as stale.
        return new ReplicasProxy(primaryTerm);
    }

    @Override
    protected void sendReplicaRequest(
        final ConcreteReplicaRequest<ResyncReplicationRequest> replicaRequest,
        final DiscoveryNode node,
        final ActionListener<ReplicationOperation.ReplicaResponse> listener) {
        if (node.getVersion().onOrAfter(Version.V_6_0_0_alpha1)) {
            super.sendReplicaRequest(replicaRequest, node, listener);
        } else {
            final long pre60NodeCheckpoint = SequenceNumbers.PRE_60_NODE_CHECKPOINT;
            listener.onResponse(new ReplicaResponse(pre60NodeCheckpoint, pre60NodeCheckpoint));
        }
    }

    @Override
    protected WritePrimaryResult<ResyncReplicationRequest, ResyncReplicationResponse> shardOperationOnPrimary(
        ResyncReplicationRequest request, IndexShard primary) throws Exception {
        final ResyncReplicationRequest replicaRequest = performOnPrimary(request, primary);
        return new WritePrimaryResult<>(replicaRequest, new ResyncReplicationResponse(), null, null, primary, logger);
    }

    public static ResyncReplicationRequest performOnPrimary(ResyncReplicationRequest request, IndexShard primary) {
        return request;
    }

    @Override
    protected WriteReplicaResult shardOperationOnReplica(ResyncReplicationRequest request, IndexShard replica) throws Exception {
        Translog.Location location = performOnReplica(request, replica);
        return new WriteReplicaResult(request, location, null, replica, logger);
    }

    public static Translog.Location performOnReplica(ResyncReplicationRequest request, IndexShard replica) throws Exception {
        Translog.Location location = null;
        for (Translog.Operation operation : request.getOperations()) {
            try {
                final Engine.Result operationResult = replica.applyTranslogOperation(operation, Engine.Operation.Origin.REPLICA,
                    update -> {
                        throw new TransportReplicationAction.RetryOnReplicaException(replica.shardId(),
                            "Mappings are not available on the replica yet, triggered update: " + update);
                    });
                location = syncOperationResultOrThrow(operationResult, location);
            } catch (Exception e) {
                // if its not a failure to be ignored, let it bubble up
                if (!TransportActions.isShardNotAvailableException(e)) {
                    throw e;
                }
            }
        }
        return location;
    }

    @Override
    public void sync(ResyncReplicationRequest request, Task parentTask, String primaryAllocationId, long primaryTerm,
                     ActionListener<ResyncReplicationResponse> listener) {
        // skip reroute phase
        transportService.sendChildRequest(
            clusterService.localNode(),
            transportPrimaryAction,
            new ConcreteShardRequest<>(request, primaryAllocationId, primaryTerm),
            parentTask,
            transportOptions,
            new TransportResponseHandler<ResyncReplicationResponse>() {
                @Override
                public ResyncReplicationResponse newInstance() {
                    return newResponseInstance();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }

                @Override
                public void handleResponse(ResyncReplicationResponse response) {
                    final ReplicationResponse.ShardInfo.Failure[] failures = response.getShardInfo().getFailures();
                    // noinspection ForLoopReplaceableByForEach
                    for (int i = 0; i < failures.length; i++) {
                        final ReplicationResponse.ShardInfo.Failure f = failures[i];
                        logger.info(
                                new ParameterizedMessage(
                                        "{} primary-replica resync to replica on node [{}] failed", f.fullShardId(), f.nodeId()),
                                f.getCause());
                    }
                    listener.onResponse(response);
                }

                @Override
                public void handleException(TransportException exp) {
                    final Throwable cause = exp.unwrapCause();
                    if (TransportActions.isShardNotAvailableException(cause)) {
                        logger.trace("primary became unavailable during resync, ignoring", exp);
                    } else {
                        listener.onFailure(exp);
                    }
                }
            });
    }

}
