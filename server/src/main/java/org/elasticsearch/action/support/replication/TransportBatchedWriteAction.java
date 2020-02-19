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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.TransportActions;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog.Location;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

/**
 * Base class for transport actions that modify data in some shard like index, delete, and shardBulk.
 * Allows performing async actions (e.g. refresh) after performing write operations on primary and replica shards
 */
public abstract class TransportBatchedWriteAction<
            Request extends ReplicatedWriteRequest<Request>,
            ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>,
            Response extends ReplicationResponse & WriteResponse
        > extends TransportReplicationAction<Request, ReplicaRequest, Response> {

    protected TransportBatchedWriteAction(Settings settings, String actionName, TransportService transportService,
                                          ClusterService clusterService, IndicesService indicesService, ThreadPool threadPool,
                                          ShardStateAction shardStateAction, ActionFilters actionFilters, Writeable.Reader<Request> request,
                                          Writeable.Reader<ReplicaRequest> replicaRequest, String executor,
                                          boolean forceExecutionOnPrimary) {
        super(settings, actionName, transportService, clusterService, indicesService, threadPool, shardStateAction, actionFilters,
              request, replicaRequest, executor, true, forceExecutionOnPrimary);
    }

    /** Syncs operation result to the translog or throws a shard not available failure */
    protected static Location syncOperationResultOrThrow(final Engine.Result operationResult,
                                                         final Location currentLocation) throws Exception {
        final Location location;
        if (operationResult.getFailure() != null) {
            // check if any transient write operation failures should be bubbled up
            Exception failure = operationResult.getFailure();
            assert failure instanceof MapperParsingException : "expected mapper parsing failures. got " + failure;
            throw failure;
        } else {
            location = locationToSync(currentLocation, operationResult.getTranslogLocation());
        }
        return location;
    }

    public static Location locationToSync(Location current, Location next) {
        /* here we are moving forward in the translog with each operation. Under the hood this might
         * cross translog files which is ok since from the user perspective the translog is like a
         * tape where only the highest location needs to be fsynced in order to sync all previous
         * locations even though they are not in the same file. When the translog rolls over files
         * the previous file is fsynced on after closing if needed.*/
        assert next != null : "next operation can't be null";
        assert current == null || current.compareTo(next) < 0 :
                "translog locations are not increasing";
        return next;
    }

    @Override
    protected ReplicationOperation.Replicas<ReplicaRequest> newReplicasProxy() {
        return new WriteActionReplicasProxy();
    }

    /**
     * Called on the primary with a reference to the primary {@linkplain IndexShard} to modify.
     *
     * @param listener listener for the result of the operation on primary, including current translog location and operation response
     * and failure async refresh is performed on the <code>primary</code> shard according to the <code>Request</code> refresh policy
     */
    @Override
    protected abstract void shardOperationOnPrimary(
            Request request, IndexShard primary, ActionListener<PrimaryResult<ReplicaRequest, Response>> listener);

    /**
     * Called once per replica with a reference to the replica {@linkplain IndexShard} to modify.
     *
     * @return the result of the operation on replica, including current translog location and operation response and failure
     * async refresh is performed on the <code>replica</code> shard according to the <code>ReplicaRequest</code> refresh policy
     */
    @Override
    protected abstract WriteReplicaResult shardOperationOnReplica(ReplicaRequest request, IndexShard replica) throws Exception;

    /**
     * Result of taking the action on the primary.
     *
     * NOTE: public for testing
     */
    public static class WritePrimaryResult<ReplicaRequest extends ReplicatedWriteRequest<ReplicaRequest>,
            Response extends ReplicationResponse & WriteResponse> extends PrimaryResult<ReplicaRequest, Response> {

        public WritePrimaryResult(ReplicaRequest request, @Nullable Response finalResponse) {
            super(request, finalResponse, null);
        }
    }

    /**
     * Result of taking the action on the replica.
     */
    public static class WriteReplicaResult extends ReplicaResult {

        public WriteReplicaResult() {
            super(null);
        }
    }

    @Override
    protected ClusterBlockLevel globalBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    @Override
    public ClusterBlockLevel indexBlockLevel() {
        return ClusterBlockLevel.WRITE;
    }

    /**
     * A proxy for <b>write</b> operations that need to be performed on the
     * replicas, where a failure to execute the operation should fail
     * the replica shard and/or mark the replica as stale.
     *
     * This extends {@code TransportReplicationAction.ReplicasProxy} to do the
     * failing and stale-ing.
     */
    class WriteActionReplicasProxy extends ReplicasProxy {

        @Override
        public void failShardIfNeeded(ShardRouting replica, long primaryTerm, String message, Exception exception,
                                      ActionListener<Void> listener) {
            if (TransportActions.isShardNotAvailableException(exception) == false) {
                logger.warn(new ParameterizedMessage("[{}] {}", replica.shardId(), message), exception);
            }
            shardStateAction.remoteShardFailed(
                replica.shardId(), replica.allocationId().getId(), primaryTerm, true, message, exception, listener);
        }

        @Override
        public void markShardCopyAsStaleIfNeeded(ShardId shardId, String allocationId, long primaryTerm, ActionListener<Void> listener) {
            shardStateAction.remoteShardFailed(shardId, allocationId, primaryTerm, true, "mark copy as stale", null, listener);
        }
    }
}
