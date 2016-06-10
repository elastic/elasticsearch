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
package org.elasticsearch.indices.flush;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.SyncedFlushResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardNotFoundException;
import org.elasticsearch.indices.IndexClosedException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class SyncedFlushService extends AbstractComponent implements IndexEventListener {

    private static final String PRE_SYNCED_FLUSH_ACTION_NAME = "internal:indices/flush/synced/pre";
    private static final String SYNCED_FLUSH_ACTION_NAME = "internal:indices/flush/synced/sync";
    private static final String IN_FLIGHT_OPS_ACTION_NAME = "internal:indices/flush/synced/in_flight";

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    @Inject
    public SyncedFlushService(Settings settings, IndicesService indicesService, ClusterService clusterService, TransportService transportService, IndexNameExpressionResolver indexNameExpressionResolver) {
        super(settings);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        transportService.registerRequestHandler(PRE_SYNCED_FLUSH_ACTION_NAME, PreShardSyncedFlushRequest::new, ThreadPool.Names.FLUSH, new PreSyncedFlushTransportHandler());
        transportService.registerRequestHandler(SYNCED_FLUSH_ACTION_NAME, ShardSyncedFlushRequest::new, ThreadPool.Names.FLUSH, new SyncedFlushTransportHandler());
        transportService.registerRequestHandler(IN_FLIGHT_OPS_ACTION_NAME, InFlightOpsRequest::new, ThreadPool.Names.SAME, new InFlightOpCountTransportHandler());
    }

    @Override
    public void onShardInactive(final IndexShard indexShard) {
        // we only want to call sync flush once, so only trigger it when we are on a primary
        if (indexShard.routingEntry().primary()) {
            attemptSyncedFlush(indexShard.shardId(), new ActionListener<ShardsSyncedFlushResult>() {
                @Override
                public void onResponse(ShardsSyncedFlushResult syncedFlushResult) {
                    logger.trace("{} sync flush on inactive shard returned successfully for sync_id: {}", syncedFlushResult.getShardId(), syncedFlushResult.syncId());
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.debug("{} sync flush on inactive shard failed", e, indexShard.shardId());
                }
            });
        }
    }

    /**
     * a utility method to perform a synced flush for all shards of multiple indices. see {@link #attemptSyncedFlush(ShardId, ActionListener)}
     * for more details.
     */
    public void attemptSyncedFlush(final String[] aliasesOrIndices, IndicesOptions indicesOptions, final ActionListener<SyncedFlushResponse> listener) {
        final ClusterState state = clusterService.state();
        final Index[] concreteIndices = indexNameExpressionResolver.concreteIndices(state, indicesOptions, aliasesOrIndices);
        final Map<String, List<ShardsSyncedFlushResult>> results = ConcurrentCollections.newConcurrentMap();
        int numberOfShards = 0;
        for (Index index : concreteIndices) {
            final IndexMetaData indexMetaData = state.metaData().getIndexSafe(index);
            numberOfShards += indexMetaData.getNumberOfShards();
            results.put(index.getName(), Collections.synchronizedList(new ArrayList<>()));

        }
        if (numberOfShards == 0) {
            listener.onResponse(new SyncedFlushResponse(results));
            return;
        }
        final CountDown countDown = new CountDown(numberOfShards);

        for (final Index concreteIndex : concreteIndices) {
            final String index = concreteIndex.getName();
            final IndexMetaData indexMetaData = state.metaData().getIndexSafe(concreteIndex);
            final int indexNumberOfShards = indexMetaData.getNumberOfShards();
            for (int shard = 0; shard < indexNumberOfShards; shard++) {
                final ShardId shardId = new ShardId(indexMetaData.getIndex(), shard);
                innerAttemptSyncedFlush(shardId, state, new ActionListener<ShardsSyncedFlushResult>() {
                    @Override
                    public void onResponse(ShardsSyncedFlushResult syncedFlushResult) {
                        results.get(index).add(syncedFlushResult);
                        if (countDown.countDown()) {
                            listener.onResponse(new SyncedFlushResponse(results));
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        logger.debug("{} unexpected error while executing synced flush", shardId);
                        final int totalShards = indexMetaData.getNumberOfReplicas() + 1;
                        results.get(index).add(new ShardsSyncedFlushResult(shardId, totalShards, e.getMessage()));
                        if (countDown.countDown()) {
                            listener.onResponse(new SyncedFlushResponse(results));
                        }
                    }
                });
            }
        }
    }

    /*
    * Tries to flush all copies of a shard and write a sync id to it.
    * After a synced flush two shard copies may only contain the same sync id if they contain the same documents.
    * To ensure this, synced flush works in three steps:
    * 1. Flush all shard copies and gather the commit ids for each copy after the flush
    * 2. Ensure that there are no ongoing indexing operations on the primary
    * 3. Perform an additional flush on each shard copy that writes the sync id
    *
    * Step 3 is only executed on a shard if
    * a) the shard has no uncommitted changes since the last flush
    * b) the last flush was the one executed in 1 (use the collected commit id to verify this)
    *
    * This alone is not enough to ensure that all copies contain the same documents. Without step 2 a sync id would be written for inconsistent copies in the following scenario:
    *
    * Write operation has completed on a primary and is being sent to replicas. The write request does not reach the replicas until sync flush is finished.
    * Step 1 is executed. After the flush the commit points on primary contains a write operation that the replica does not have.
    * Step 3 will be executed on primary and replica as well because there are no uncommitted changes on primary (the first flush committed them) and there are no uncommitted
    * changes on the replica (the write operation has not reached the replica yet).
    *
    * Step 2 detects this scenario and fails the whole synced flush if a write operation is ongoing on the primary.
    * Together with the conditions for step 3 (same commit id and no uncommitted changes) this guarantees that a snc id will only
    * be written on a primary if no write operation was executed between step 1 and step 3 and sync id will only be written on
    * the replica if it contains the same changes that the primary contains.
    *
    * Synced flush is a best effort operation. The sync id may be written on all, some or none of the copies.
    **/
    public void attemptSyncedFlush(final ShardId shardId, final ActionListener<ShardsSyncedFlushResult> actionListener) {
        innerAttemptSyncedFlush(shardId, clusterService.state(), actionListener);
    }

    private void innerAttemptSyncedFlush(final ShardId shardId, final ClusterState state, final ActionListener<ShardsSyncedFlushResult> actionListener) {
        try {
            final IndexShardRoutingTable shardRoutingTable = getShardRoutingTable(shardId, state);
            final List<ShardRouting> activeShards = shardRoutingTable.activeShards();
            final int totalShards = shardRoutingTable.getSize();

            if (activeShards.size() == 0) {
                actionListener.onResponse(new ShardsSyncedFlushResult(shardId, totalShards, "no active shards"));
                return;
            }

            final ActionListener<Map<String, Engine.CommitId>> commitIdsListener = new ActionListener<Map<String, Engine.CommitId>>() {
                @Override
                public void onResponse(final Map<String, Engine.CommitId> commitIds) {
                    if (commitIds.isEmpty()) {
                        actionListener.onResponse(new ShardsSyncedFlushResult(shardId, totalShards, "all shards failed to commit on pre-sync"));
                        return;
                    }
                    final ActionListener<InFlightOpsResponse> inflightOpsListener = new ActionListener<InFlightOpsResponse>() {
                        @Override
                        public void onResponse(InFlightOpsResponse response) {
                            final int inflight = response.opCount();
                            assert inflight >= 0;
                            if (inflight != 0) {
                                actionListener.onResponse(new ShardsSyncedFlushResult(shardId, totalShards, "[" + inflight + "] ongoing operations on primary"));
                            } else {
                                // 3. now send the sync request to all the shards
                                String syncId = UUIDs.base64UUID();
                                sendSyncRequests(syncId, activeShards, state, commitIds, shardId, totalShards, actionListener);
                            }
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            actionListener.onFailure(e);
                        }
                    };
                    // 2. fetch in flight operations
                    getInflightOpsCount(shardId, state, shardRoutingTable, inflightOpsListener);
                }

                @Override
                public void onFailure(Throwable e) {
                    actionListener.onFailure(e);
                }
            };

            // 1. send pre-sync flushes to all replicas
            sendPreSyncRequests(activeShards, state, shardId, commitIdsListener);
        } catch (Throwable t) {
            actionListener.onFailure(t);
        }
    }

    final IndexShardRoutingTable getShardRoutingTable(ShardId shardId, ClusterState state) {
        final IndexRoutingTable indexRoutingTable = state.routingTable().index(shardId.getIndexName());
        if (indexRoutingTable == null) {
            IndexMetaData index = state.getMetaData().index(shardId.getIndex());
            if (index != null && index.getState() == IndexMetaData.State.CLOSE) {
                throw new IndexClosedException(shardId.getIndex());
            }
            throw new IndexNotFoundException(shardId.getIndexName());
        }
        final IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId.id());
        if (shardRoutingTable == null) {
            throw new ShardNotFoundException(shardId);
        }
        return shardRoutingTable;
    }

    /**
     * returns the number of in flight operations on primary. -1 upon error.
     */
    protected void getInflightOpsCount(final ShardId shardId, ClusterState state, IndexShardRoutingTable shardRoutingTable, final ActionListener<InFlightOpsResponse> listener) {
        try {
            final ShardRouting primaryShard = shardRoutingTable.primaryShard();
            final DiscoveryNode primaryNode = state.nodes().get(primaryShard.currentNodeId());
            if (primaryNode == null) {
                logger.trace("{} failed to resolve node for primary shard {}, skipping sync", shardId, primaryShard);
                listener.onResponse(new InFlightOpsResponse(-1));
                return;
            }
            logger.trace("{} retrieving in flight operation count", shardId);
            transportService.sendRequest(primaryNode, IN_FLIGHT_OPS_ACTION_NAME, new InFlightOpsRequest(shardId),
                    new BaseTransportResponseHandler<InFlightOpsResponse>() {
                        @Override
                        public InFlightOpsResponse newInstance() {
                            return new InFlightOpsResponse();
                        }

                        @Override
                        public void handleResponse(InFlightOpsResponse response) {
                            listener.onResponse(response);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.debug("{} unexpected error while retrieving in flight op count", shardId);
                            listener.onFailure(exp);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    });
        } catch (Throwable t) {
            listener.onFailure(t);
        }
    }


    void sendSyncRequests(final String syncId, final List<ShardRouting> shards, ClusterState state, Map<String, Engine.CommitId> expectedCommitIds,
                          final ShardId shardId, final int totalShards, final ActionListener<ShardsSyncedFlushResult> listener) {
        final CountDown countDown = new CountDown(shards.size());
        final Map<ShardRouting, ShardSyncedFlushResponse> results = ConcurrentCollections.newConcurrentMap();
        for (final ShardRouting shard : shards) {
            final DiscoveryNode node = state.nodes().get(shard.currentNodeId());
            if (node == null) {
                logger.trace("{} is assigned to an unknown node. skipping for sync id [{}]. shard routing {}", shardId, syncId, shard);
                results.put(shard, new ShardSyncedFlushResponse("unknown node"));
                contDownAndSendResponseIfDone(syncId, shards, shardId, totalShards, listener, countDown, results);
                continue;
            }
            final Engine.CommitId expectedCommitId = expectedCommitIds.get(shard.currentNodeId());
            if (expectedCommitId == null) {
                logger.trace("{} can't resolve expected commit id for current node, skipping for sync id [{}]. shard routing {}", shardId, syncId, shard);
                results.put(shard, new ShardSyncedFlushResponse("no commit id from pre-sync flush"));
                contDownAndSendResponseIfDone(syncId, shards, shardId, totalShards, listener, countDown, results);
                continue;
            }
            logger.trace("{} sending synced flush request to {}. sync id [{}].", shardId, shard, syncId);
            transportService.sendRequest(node, SYNCED_FLUSH_ACTION_NAME, new ShardSyncedFlushRequest(shard.shardId(), syncId, expectedCommitId),
                    new BaseTransportResponseHandler<ShardSyncedFlushResponse>() {
                        @Override
                        public ShardSyncedFlushResponse newInstance() {
                            return new ShardSyncedFlushResponse();
                        }

                        @Override
                        public void handleResponse(ShardSyncedFlushResponse response) {
                            ShardSyncedFlushResponse existing = results.put(shard, response);
                            assert existing == null : "got two answers for node [" + node + "]";
                            // count after the assert so we won't decrement twice in handleException
                            contDownAndSendResponseIfDone(syncId, shards, shardId, totalShards, listener, countDown, results);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.trace("{} error while performing synced flush on [{}], skipping", exp, shardId, shard);
                            results.put(shard, new ShardSyncedFlushResponse(exp.getMessage()));
                            contDownAndSendResponseIfDone(syncId, shards, shardId, totalShards, listener, countDown, results);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    });
        }

    }

    private void contDownAndSendResponseIfDone(String syncId, List<ShardRouting> shards, ShardId shardId, int totalShards,
            ActionListener<ShardsSyncedFlushResult> listener, CountDown countDown, Map<ShardRouting, ShardSyncedFlushResponse> results) {
        if (countDown.countDown()) {
            assert results.size() == shards.size();
            listener.onResponse(new ShardsSyncedFlushResult(shardId, syncId, totalShards, results));
        }
    }

    /**
     * send presync requests to all started copies of the given shard
     */
    void sendPreSyncRequests(final List<ShardRouting> shards, final ClusterState state, final ShardId shardId, final ActionListener<Map<String, Engine.CommitId>> listener) {
        final CountDown countDown = new CountDown(shards.size());
        final ConcurrentMap<String, Engine.CommitId> commitIds = ConcurrentCollections.newConcurrentMap();
        for (final ShardRouting shard : shards) {
            logger.trace("{} sending pre-synced flush request to {}", shardId, shard);
            final DiscoveryNode node = state.nodes().get(shard.currentNodeId());
            if (node == null) {
                logger.trace("{} shard routing {} refers to an unknown node. skipping.", shardId, shard);
                if (countDown.countDown()) {
                    listener.onResponse(commitIds);
                }
                continue;
            }
            transportService.sendRequest(node, PRE_SYNCED_FLUSH_ACTION_NAME, new PreShardSyncedFlushRequest(shard.shardId()), new BaseTransportResponseHandler<PreSyncedFlushResponse>() {
                @Override
                public PreSyncedFlushResponse newInstance() {
                    return new PreSyncedFlushResponse();
                }

                @Override
                public void handleResponse(PreSyncedFlushResponse response) {
                    Engine.CommitId existing = commitIds.putIfAbsent(node.getId(), response.commitId());
                    assert existing == null : "got two answers for node [" + node + "]";
                    // count after the assert so we won't decrement twice in handleException
                    if (countDown.countDown()) {
                        listener.onResponse(commitIds);
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.trace("{} error while performing pre synced flush on [{}], skipping", exp, shardId, shard);
                    if (countDown.countDown()) {
                        listener.onResponse(commitIds);
                    }
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
    }

    private PreSyncedFlushResponse performPreSyncedFlush(PreShardSyncedFlushRequest request) {
        IndexShard indexShard = indicesService.indexServiceSafe(request.shardId().getIndex()).getShard(request.shardId().id());
        FlushRequest flushRequest = new FlushRequest().force(false).waitIfOngoing(true);
        logger.trace("{} performing pre sync flush", request.shardId());
        Engine.CommitId commitId = indexShard.flush(flushRequest);
        logger.trace("{} pre sync flush done. commit id {}", request.shardId(), commitId);
        return new PreSyncedFlushResponse(commitId);
    }

    private ShardSyncedFlushResponse performSyncedFlush(ShardSyncedFlushRequest request) {
        IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(request.shardId().id());
        logger.trace("{} performing sync flush. sync id [{}], expected commit id {}", request.shardId(), request.syncId(), request.expectedCommitId());
        Engine.SyncedFlushResult result = indexShard.syncFlush(request.syncId(), request.expectedCommitId());
        logger.trace("{} sync flush done. sync id [{}], result [{}]", request.shardId(), request.syncId(), result);
        switch (result) {
            case SUCCESS:
                return new ShardSyncedFlushResponse();
            case COMMIT_MISMATCH:
                return new ShardSyncedFlushResponse("commit has changed");
            case PENDING_OPERATIONS:
                return new ShardSyncedFlushResponse("pending operations");
            default:
                throw new ElasticsearchException("unknown synced flush result [" + result + "]");
        }
    }

    private InFlightOpsResponse performInFlightOps(InFlightOpsRequest request) {
        IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = indexService.getShard(request.shardId().id());
        if (indexShard.routingEntry().primary() == false) {
            throw new IllegalStateException("[" + request.shardId() +"] expected a primary shard");
        }
        int opCount = indexShard.getActiveOperationsCount();
        logger.trace("{} in flight operations sampled at [{}]", request.shardId(), opCount);
        return new InFlightOpsResponse(opCount);
    }

    public final static class PreShardSyncedFlushRequest extends TransportRequest {
        private ShardId shardId;

        public PreShardSyncedFlushRequest() {
        }

        public PreShardSyncedFlushRequest(ShardId shardId) {
            this.shardId = shardId;
        }

        @Override
        public String toString() {
            return "PreShardSyncedFlushRequest{" +
                    "shardId=" + shardId +
                    '}';
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            this.shardId = ShardId.readShardId(in);
        }

        public ShardId shardId() {
            return shardId;
        }
    }

    /**
     * Response for first step of synced flush (flush) for one shard copy
     */
    final static class PreSyncedFlushResponse extends TransportResponse {

        Engine.CommitId commitId;

        PreSyncedFlushResponse() {
        }

        PreSyncedFlushResponse(Engine.CommitId commitId) {
            this.commitId = commitId;
        }

        public Engine.CommitId commitId() {
            return commitId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            commitId = new Engine.CommitId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            commitId.writeTo(out);
        }
    }

    public static final class ShardSyncedFlushRequest extends TransportRequest {

        private String syncId;
        private Engine.CommitId expectedCommitId;
        private ShardId shardId;

        public ShardSyncedFlushRequest() {
        }

        public ShardSyncedFlushRequest(ShardId shardId, String syncId, Engine.CommitId expectedCommitId) {
            this.expectedCommitId = expectedCommitId;
            this.shardId = shardId;
            this.syncId = syncId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = ShardId.readShardId(in);
            expectedCommitId = new Engine.CommitId(in);
            syncId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            expectedCommitId.writeTo(out);
            out.writeString(syncId);
        }

        public ShardId shardId() {
            return shardId;
        }

        public String syncId() {
            return syncId;
        }

        public Engine.CommitId expectedCommitId() {
            return expectedCommitId;
        }

        @Override
        public String toString() {
            return "ShardSyncedFlushRequest{" +
                    "shardId=" + shardId +
                    ",syncId='" + syncId + '\'' +
                    '}';
        }
    }

    /**
     * Response for third step of synced flush (writing the sync id) for one shard copy
     */
    public static final class ShardSyncedFlushResponse extends TransportResponse {

        /**
         * a non null value indicates a failure to sync flush. null means success
         */
        String failureReason;

        public ShardSyncedFlushResponse() {
            failureReason = null;
        }

        public ShardSyncedFlushResponse(String failureReason) {
            this.failureReason = failureReason;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            failureReason = in.readOptionalString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(failureReason);
        }

        public boolean success() {
            return failureReason == null;
        }

        public String failureReason() {
            return failureReason;
        }

        @Override
        public String toString() {
            return "ShardSyncedFlushResponse{" +
                    "success=" + success() +
                    ", failureReason='" + failureReason + '\'' +
                    '}';
        }

        public static ShardSyncedFlushResponse readSyncedFlushResponse(StreamInput in) throws IOException {
            ShardSyncedFlushResponse shardSyncedFlushResponse = new ShardSyncedFlushResponse();
            shardSyncedFlushResponse.readFrom(in);
            return shardSyncedFlushResponse;
        }
    }


    public static final class InFlightOpsRequest extends TransportRequest {

        private ShardId shardId;

        public InFlightOpsRequest() {
        }

        public InFlightOpsRequest(ShardId shardId) {
            this.shardId = shardId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = ShardId.readShardId(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
        }

        public ShardId shardId() {
            return shardId;
        }

        @Override
        public String toString() {
            return "InFlightOpsRequest{" +
                    "shardId=" + shardId +
                    '}';
        }
    }

    /**
     * Response for second step of synced flush (check operations in flight)
     */
    static final class InFlightOpsResponse extends TransportResponse {

        int opCount;

        public InFlightOpsResponse() {
        }

        public InFlightOpsResponse(int opCount) {
            this.opCount = opCount;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            opCount = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(opCount);
        }

        public int opCount() {
            return opCount;
        }

        @Override
        public String toString() {
            return "InFlightOpsResponse{" +
                    "opCount=" + opCount +
                    '}';
        }
    }

    private final class PreSyncedFlushTransportHandler implements TransportRequestHandler<PreShardSyncedFlushRequest> {

        @Override
        public void messageReceived(PreShardSyncedFlushRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(performPreSyncedFlush(request));
        }
    }

    private final class SyncedFlushTransportHandler implements TransportRequestHandler<ShardSyncedFlushRequest> {

        @Override
        public void messageReceived(ShardSyncedFlushRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(performSyncedFlush(request));
        }
    }

    private final class InFlightOpCountTransportHandler implements TransportRequestHandler<InFlightOpsRequest> {

        @Override
        public void messageReceived(InFlightOpsRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(performInFlightOps(request));
        }
    }

}
