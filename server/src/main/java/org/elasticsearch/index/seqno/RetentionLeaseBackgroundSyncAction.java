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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.replication.ReplicationRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.support.replication.ReplicationTask;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.cluster.action.shard.ShardStateAction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardClosedException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

/**
 * Replication action responsible for background syncing retention leases to replicas. This action is deliberately a replication action so
 * that if a replica misses a background retention lease sync then that shard will not be marked as stale. We have some tolerance for a
 * shard copy missing renewals of retention leases since the background sync interval is much smaller than the expected lifetime of
 * retention leases.
 */
public class RetentionLeaseBackgroundSyncAction extends TransportReplicationAction<
        RetentionLeaseBackgroundSyncAction.Request,
        RetentionLeaseBackgroundSyncAction.Request,
        ReplicationResponse> {

    public static final String ACTION_NAME = "indices:admin/seq_no/retention_lease_background_sync";
    private static final Logger LOGGER = LogManager.getLogger(RetentionLeaseSyncAction.class);

    protected Logger getLogger() {
        return LOGGER;
    }

    @Inject
    public RetentionLeaseBackgroundSyncAction(
            final Settings settings,
            final TransportService transportService,
            final ClusterService clusterService,
            final IndicesService indicesService,
            final ThreadPool threadPool,
            final ShardStateAction shardStateAction,
            final ActionFilters actionFilters) {
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
                ThreadPool.Names.MANAGEMENT);
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<ReplicationResponse> listener) {
        assert false : "use RetentionLeaseBackgroundSyncAction#backgroundSync";
    }

    final void backgroundSync(ShardId shardId, String primaryAllocationId, long primaryTerm, RetentionLeases retentionLeases) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // we have to execute under the system context so that if security is enabled the sync is authorized
            threadContext.markAsSystemContext();
            final Request request = new Request(shardId, retentionLeases);
            final ReplicationTask task = (ReplicationTask) taskManager.register("transport", "retention_lease_background_sync", request);
            transportService.sendChildRequest(clusterService.localNode(), transportPrimaryAction,
                new ConcreteShardRequest<>(request, primaryAllocationId, primaryTerm),
                task,
                transportOptions,
                new TransportResponseHandler<ReplicationResponse>() {
                    @Override
                    public ReplicationResponse read(StreamInput in) throws IOException {
                        return newResponseInstance(in);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }

                    @Override
                    public void handleResponse(ReplicationResponse response) {
                        task.setPhase("finished");
                        taskManager.unregister(task);
                    }

                    @Override
                    public void handleException(TransportException e) {
                        task.setPhase("finished");
                        taskManager.unregister(task);
                        if (ExceptionsHelper.unwrap(e, NodeClosedException.class) != null) {
                            // node shutting down
                            return;
                        }
                        if (ExceptionsHelper.unwrap(e,
                                                    IndexNotFoundException.class,
                                                    AlreadyClosedException.class,
                                                    IndexShardClosedException.class) != null) {
                            // the index was deleted or the shard is closed
                            return;
                        }
                        getLogger().warn(new ParameterizedMessage("{} retention lease background sync failed", shardId), e);
                    }
                });
        }
    }

    @Override
    protected void shardOperationOnPrimary(
            final Request request,
            final IndexShard primary, ActionListener<PrimaryResult<Request, ReplicationResponse>> listener) {
        ActionListener.completeWith(listener, () -> {
            assert request.waitForActiveShards().equals(ActiveShardCount.NONE) : request.waitForActiveShards();
            Objects.requireNonNull(request);
            Objects.requireNonNull(primary);
            primary.persistRetentionLeases();
            return new PrimaryResult<>(request, new ReplicationResponse());
        });
    }

    @Override
    protected void shardOperationOnReplica(Request request, IndexShard replica, ActionListener<ReplicaResult> listener) {
        ActionListener.completeWith(listener, () -> {
            Objects.requireNonNull(request);
            Objects.requireNonNull(replica);
            replica.updateRetentionLeasesOnReplica(request.getRetentionLeases());
            replica.persistRetentionLeases();
            return new ReplicaResult();
        });
    }

    public static final class Request extends ReplicationRequest<Request> {

        private RetentionLeases retentionLeases;

        public RetentionLeases getRetentionLeases() {
            return retentionLeases;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            retentionLeases = new RetentionLeases(in);
        }

        public Request(final ShardId shardId, final RetentionLeases retentionLeases) {
            super(Objects.requireNonNull(shardId));
            this.retentionLeases = Objects.requireNonNull(retentionLeases);
            waitForActiveShards(ActiveShardCount.NONE);
        }

        @Override
        public void writeTo(final StreamOutput out) throws IOException {
            super.writeTo(Objects.requireNonNull(out));
            retentionLeases.writeTo(out);
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new ReplicationTask(id, type, action, "retention_lease_background_sync shardId=" + shardId, parentTaskId, headers);
        }

        @Override
        public String toString() {
            return "RetentionLeaseBackgroundSyncAction.Request{" +
                    "retentionLeases=" + retentionLeases +
                    ", shardId=" + shardId +
                    ", timeout=" + timeout +
                    ", index='" + index + '\'' +
                    ", waitForActiveShards=" + waitForActiveShards +
                    '}';
        }

    }

    @Override
    protected ReplicationResponse newResponseInstance(StreamInput in) throws IOException {
        return new ReplicationResponse(in);
    }

}
