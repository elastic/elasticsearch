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

package org.elasticsearch.index.seqno;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.WriteResponse;
import org.elasticsearch.action.support.replication.ReplicatedWriteRequest;
import org.elasticsearch.action.support.replication.ReplicationOperation;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Objects;

/**
 * Write action responsible for syncing retention leases to replicas. This action is deliberately a write action so that if a replica misses
 * a retention lease sync then that shard will be marked as stale.
 */
public class RetentionLeaseSyncAction extends
        TransportWriteAction<RetentionLeaseSyncAction.Request, RetentionLeaseSyncAction.Request, RetentionLeaseSyncAction.Response> {

    public static String ACTION_NAME = "indices:admin/seq_no/retention_lease_sync";

    private static final Logger LOGGER = LogManager.getLogger(RetentionLeaseSyncAction.class);

    protected Logger getLogger() {
        return LOGGER;
    }

    @Inject
    public RetentionLeaseSyncAction(
            final Settings settings,
            final TransportService transportService,
            final ClusterService clusterService,
            final IndicesService indicesService,
            final ThreadPool threadPool,
            final ShardStateAction shardStateAction,
            final ActionFilters actionFilters,
            final IndexNameExpressionResolver indexNameExpressionResolver) {
        super(
                settings,
                ACTION_NAME,
                transportService,
                clusterService,
                indicesService,
                threadPool,
                shardStateAction,
                actionFilters,
                indexNameExpressionResolver,
                RetentionLeaseSyncAction.Request::new,
                RetentionLeaseSyncAction.Request::new,
                ThreadPool.Names.MANAGEMENT);
    }

    /**
     * Sync the specified retention leases for the specified shard. The callback is invoked when the sync succeeds or fails.
     *
     * @param shardId         the shard to sync
     * @param retentionLeases the retention leases to sync
     * @param listener        the callback to invoke when the sync completes normally or abnormally
     */
    public void sync(
            final ShardId shardId,
            final RetentionLeases retentionLeases,
            final ActionListener<ReplicationResponse> listener) {
        Objects.requireNonNull(shardId);
        Objects.requireNonNull(retentionLeases);
        Objects.requireNonNull(listener);
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // we have to execute under the system context so that if security is enabled the sync is authorized
            threadContext.markAsSystemContext();
            execute(
                    new RetentionLeaseSyncAction.Request(shardId, retentionLeases),
                    ActionListener.wrap(
                            listener::onResponse,
                            e -> {
                                if (ExceptionsHelper.unwrap(e, AlreadyClosedException.class, IndexShardClosedException.class) == null) {
                                    getLogger().warn(new ParameterizedMessage("{} retention lease sync failed", shardId), e);
                                }
                                listener.onFailure(e);
                            }));
        }
    }

    @Override
    protected WritePrimaryResult<Request, Response> shardOperationOnPrimary(
            final Request request,
            final IndexShard primary) throws IOException {
        assert request.waitForActiveShards().equals(ActiveShardCount.NONE) : request.waitForActiveShards();
        Objects.requireNonNull(request);
        Objects.requireNonNull(primary);
        primary.persistRetentionLeases();
        return new WritePrimaryResult<>(request, new Response(), null, null, primary, getLogger());
    }

    @Override
    protected void sendReplicaRequest(
            final ConcreteReplicaRequest<Request> replicaRequest,
            final DiscoveryNode node,
            final ActionListener<ReplicationOperation.ReplicaResponse> listener) {
        if (node.getVersion().onOrAfter(Version.V_6_7_0)) {
            super.sendReplicaRequest(replicaRequest, node, listener);
        } else {
            listener.onResponse(new ReplicaResponse(SequenceNumbers.NO_OPS_PERFORMED, SequenceNumbers.NO_OPS_PERFORMED));
        }
    }

    @Override
    protected WriteReplicaResult<Request> shardOperationOnReplica(
            final Request request,
            final IndexShard replica) throws IOException {
        Objects.requireNonNull(request);
        Objects.requireNonNull(replica);
        replica.updateRetentionLeasesOnReplica(request.getRetentionLeases());
        replica.persistRetentionLeases();
        return new WriteReplicaResult<>(request, null, null, replica, getLogger());
    }

    @Override
    public ClusterBlockLevel indexBlockLevel() {
        return null;
    }

    public static final class Request extends ReplicatedWriteRequest<Request> {

        private RetentionLeases retentionLeases;

        public RetentionLeases getRetentionLeases() {
            return retentionLeases;
        }

        public Request() {

        }

        public Request(final ShardId shardId, final RetentionLeases retentionLeases) {
            super(Objects.requireNonNull(shardId));
            this.retentionLeases = Objects.requireNonNull(retentionLeases);
            waitForActiveShards(ActiveShardCount.NONE);
        }

        @Override
        public void readFrom(final StreamInput in) throws IOException {
            super.readFrom(in);
            retentionLeases = new RetentionLeases(in);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(Objects.requireNonNull(out));
            retentionLeases.writeTo(out);
        }

        @Override
        public String toString() {
            return "Request{" +
                    "retentionLeases=" + retentionLeases +
                    ", shardId=" + shardId +
                    ", timeout=" + timeout +
                    ", index='" + index + '\'' +
                    ", waitForActiveShards=" + waitForActiveShards +
                    '}';
        }

    }

    public static final class Response extends ReplicationResponse implements WriteResponse {

        @Override
        public void setForcedRefresh(final boolean forcedRefresh) {
            // ignore
        }

    }

    @Override
    protected Response newResponseInstance() {
        return new Response();
    }

}
