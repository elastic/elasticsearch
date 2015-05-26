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
package org.elasticsearch.indices;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ImmutableShardRouting;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class SyncedFlushService extends AbstractComponent {

    private static final String PRE_SYNCED_FLUSH_ACTION_NAME = "internal:indices/flush/synced/pre";
    private static final String SYNCED_FLUSH_ACTION_NAME = "internal:indices/flush/synced/sync";
    private static final String IN_FLIGHT_OPS_ACTION_NAME = "internal:indices/flush/synced/in_flight";

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final TransportService transportService;

    @Inject
    public SyncedFlushService(Settings settings, IndicesService indicesService, ClusterService clusterService, TransportService transportService) {
        super(settings);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.transportService = transportService;

        transportService.registerRequestHandler(PRE_SYNCED_FLUSH_ACTION_NAME, PreSyncedFlushRequest.class, ThreadPool.Names.FLUSH, new PreSyncedFlushTransportHandler());
        transportService.registerRequestHandler(SYNCED_FLUSH_ACTION_NAME, SyncedFlushRequest.class, ThreadPool.Names.FLUSH, new SyncedFlushTransportHandler());
        transportService.registerRequestHandler(IN_FLIGHT_OPS_ACTION_NAME, InFlightOpsRequest.class, ThreadPool.Names.SAME, new InFlightOpCountTransportHandler());
        indicesService.indicesLifecycle().addListener(new IndicesLifecycle.Listener() {
            @Override
            public void onShardInactive(final IndexShard indexShard) {
                // we only want to call sync flush once, so only trigger it when we are on a primary
                if (indexShard.routingEntry().primary()) {
                    attemptSyncedFlush(indexShard.shardId(), new ActionListener<SyncedFlushResult>() {
                        @Override
                        public void onResponse(SyncedFlushResult syncedFlushResult) {
                            logger.debug("{} sync flush on inactive shard returned successfully for sync_id: {}", syncedFlushResult.getShardId(), syncedFlushResult.syncId());
                        }

                        @Override
                        public void onFailure(Throwable e) {
                            logger.debug("{} sync flush on inactive shard failed", e, indexShard.shardId());
                        }
                    });
                }
            }
        });
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
    public void attemptSyncedFlush(final ShardId shardId, final ActionListener<SyncedFlushResult> actionListener) {
        try {
            final ClusterState state = clusterService.state();
            final IndexShardRoutingTable shardRoutingTable = getActiveShardRoutings(shardId, state);
            final List<ShardRouting> activeShards = shardRoutingTable.activeShards();
            final ActionListener<Map<String, Engine.CommitId>> commitIdsListener = new ActionListener<Map<String, Engine.CommitId>>() {
                @Override
                public void onResponse(final Map<String, Engine.CommitId> commitIds) {
                    if (commitIds.isEmpty()) {
                        actionListener.onResponse(new SyncedFlushResult(shardId, "all shards failed to commit on pre-sync"));
                    }
                    final ActionListener<InFlightOpsResponse> inflightOpsListener = new ActionListener<InFlightOpsResponse>() {
                        @Override
                        public void onResponse(InFlightOpsResponse response) {
                            final int inflight = response.opCount();
                            assert inflight >= -1;
                            if (inflight != 1) { // 1 means that there are no write operations are in flight (>1) and the shard is not closed (0).
                                actionListener.onResponse(new SyncedFlushResult(shardId, "operation counter on primary is non zero [" + inflight + "]"));
                            } else {
                                // 3. now send the sync request to all the shards
                                String syncId = Strings.base64UUID();
                                sendSyncRequests(syncId, activeShards, state, commitIds, shardId, actionListener);
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

    final IndexShardRoutingTable getActiveShardRoutings(ShardId shardId, ClusterState state) {
        final IndexRoutingTable indexRoutingTable = state.routingTable().index(shardId.index().name());
        if (indexRoutingTable == null) {
            IndexMetaData index = state.getMetaData().index(shardId.index().getName());
            if (index != null && index.state() == IndexMetaData.State.CLOSE) {
                throw new IndexClosedException(shardId.index());
            }
            throw new IndexMissingException(shardId.index());
        }
        final IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId.id());
        if (shardRoutingTable == null) {
            throw new IndexShardMissingException(shardId);
        }
        return shardRoutingTable;
    }

    /**
     * returns the number of inflight operations on primary. -1 upon error.
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
                            logger.debug("{} unexpected error while retrieving inflight op count", shardId);
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


    void sendSyncRequests(final String syncId, final List<ShardRouting> shards, ClusterState state, Map<String, Engine.CommitId> expectedCommitIds, final ShardId shardId, final ActionListener<SyncedFlushResult> listener) {
        final CountDown countDown = new CountDown(shards.size());
        final Map<ShardRouting, SyncedFlushResponse> results = ConcurrentCollections.newConcurrentMap();
        for (final ShardRouting shard : shards) {
            final DiscoveryNode node = state.nodes().get(shard.currentNodeId());
            if (node == null) {
                logger.trace("{} is assigned to an unknown node. skipping for sync id [{}]. shard routing {}", shardId, syncId, shard);
                results.put(shard, new SyncedFlushResponse("unknown node"));
                contDownAndSendResponseIfDone(syncId, shards, shardId, listener, countDown, results);
                continue;
            }
            final Engine.CommitId expectedCommitId = expectedCommitIds.get(shard.currentNodeId());
            if (expectedCommitId == null) {
                logger.trace("{} can't resolve expected commit id for {}, skipping for sync id [{}]. shard routing {}", shardId, syncId, shard);
                results.put(shard, new SyncedFlushResponse("no commit id from pre-sync flush"));
                contDownAndSendResponseIfDone(syncId, shards, shardId, listener, countDown, results);
                continue;
            }
            logger.trace("{} sending synced flush request to {}. sync id [{}].", shardId, shard, syncId);
            transportService.sendRequest(node, SYNCED_FLUSH_ACTION_NAME, new SyncedFlushRequest(shard.shardId(), syncId, expectedCommitId),
                    new BaseTransportResponseHandler<SyncedFlushResponse>() {
                        @Override
                        public SyncedFlushResponse newInstance() {
                            return new SyncedFlushResponse();
                        }

                        @Override
                        public void handleResponse(SyncedFlushResponse response) {
                            SyncedFlushResponse existing = results.put(shard, response);
                            assert existing == null : "got two answers for node [" + node + "]";
                            // count after the assert so we won't decrement twice in handleException
                            contDownAndSendResponseIfDone(syncId, shards, shardId, listener, countDown, results);
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.trace("{} error while performing synced flush on [{}], skipping", exp, shardId, shard);
                            results.put(shard, new SyncedFlushResponse(exp.getMessage()));
                            contDownAndSendResponseIfDone(syncId, shards, shardId, listener, countDown, results);
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    });
        }

    }

    private void contDownAndSendResponseIfDone(String syncId, List<ShardRouting> shards, ShardId shardId, ActionListener<SyncedFlushResult> listener, CountDown countDown, Map<ShardRouting, SyncedFlushResponse> results) {
        if (countDown.countDown()) {
            assert results.size() == shards.size();
            listener.onResponse(new SyncedFlushResult(shardId, syncId, results));
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
                if(countDown.countDown()) {
                   listener.onResponse(commitIds);
                }
                continue;
            }
            transportService.sendRequest(node, PRE_SYNCED_FLUSH_ACTION_NAME, new PreSyncedFlushRequest(shard.shardId()), new BaseTransportResponseHandler<PreSyncedFlushResponse>() {
                @Override
                public PreSyncedFlushResponse newInstance() {
                    return new PreSyncedFlushResponse();
                }

                @Override
                public void handleResponse(PreSyncedFlushResponse response) {
                    Engine.CommitId existing = commitIds.putIfAbsent(node.id(), response.commitId());
                    assert existing == null : "got two answers for node [" + node + "]";
                    // count after the assert so we won't decrement twice in handleException
                    if(countDown.countDown()) {
                        listener.onResponse(commitIds);
                    }
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.trace("{} error while performing pre synced flush on [{}], skipping", shardId, exp, shard);
                    if(countDown.countDown()) {
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

    private PreSyncedFlushResponse performPreSyncedFlush(PreSyncedFlushRequest request) {
        IndexShard indexShard = indicesService.indexServiceSafe(request.shardId().getIndex()).shardSafe(request.shardId().id());
        FlushRequest flushRequest = new FlushRequest().force(false).waitIfOngoing(true);
        logger.trace("{} performing pre sync flush", request.shardId());
        Engine.CommitId commitId = indexShard.flush(flushRequest);
        logger.trace("{} pre sync flush done. commit id {}", request.shardId(), commitId);
        return new PreSyncedFlushResponse(commitId);
    }

    private SyncedFlushResponse performSyncedFlush(SyncedFlushRequest request) {
        IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = indexService.shardSafe(request.shardId().id());
        logger.trace("{} performing sync flush. sync id [{}], expected commit id {}", request.shardId(), request.syncId(), request.expectedCommitId());
        Engine.SyncedFlushResult result = indexShard.syncFlush(request.syncId(), request.expectedCommitId());
        logger.trace("{} sync flush done. sync id [{}], result  [{}]", request.shardId(), request.syncId(), result);
        switch (result) {
            case SUCCESS:
                return new SyncedFlushResponse();
            case COMMIT_MISMATCH:
                return new SyncedFlushResponse("commit has changed");
            case PENDING_OPERATIONS:
                return new SyncedFlushResponse("pending operations");
            default:
                throw new ElasticsearchException("unknown synced flush result [" + result + "]");
        }
    }

    private InFlightOpsResponse performInFlightOps(InFlightOpsRequest request) {
        IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = indexService.shardSafe(request.shardId().id());
        if (indexShard.routingEntry().primary() == false) {
            throw new IndexShardException(request.shardId(), "expected a primary shard");
        }
        int opCount = indexShard.getOperationsCount();
        logger.trace("{} in flight operations sampled at [{}]", request.shardId(), opCount);
        return new InFlightOpsResponse(opCount);
    }

    /**
     * Result for all copies of a shard
     */
    public static class SyncedFlushResult extends TransportResponse {
        private String failureReason;
        private Map<ShardRouting, SyncedFlushResponse> shardResponses;
        private String syncId;
        private ShardId shardId;

        public SyncedFlushResult() {
        }

        public ShardId getShardId() {
            return shardId;
        }

        /**
         * failure constructor
         */
        public SyncedFlushResult(ShardId shardId, String failureReason) {
            this.syncId = null;
            this.failureReason = failureReason;
            this.shardResponses = ImmutableMap.of();
            this.shardId = shardId;
        }

        /**
         * success constructor
         */
        public SyncedFlushResult(ShardId shardId, String syncId, Map<ShardRouting, SyncedFlushResponse> shardResponses) {
            this.failureReason = null;
            ImmutableMap.Builder<ShardRouting, SyncedFlushResponse> builder = ImmutableMap.builder();
            this.shardResponses = builder.putAll(shardResponses).build();
            this.syncId = syncId;
            this.shardId = shardId;
        }

        /**
         * @return true if one or more shard copies was successful, false if all failed before step three of synced flush
         */
        public boolean success() {
            return syncId != null;
        }

        /**
         * @return the reason for the failure if synced flush failed before step three of synced flush
         */
        public String failureReason() {
            return failureReason;
        }

        public String syncId() {
            return syncId;
        }

        /**
         * @return total number of shards for which a sync attempt was made
         */
        public int totalShards() {
            return shardResponses.size();
        }

        /**
         * @return total number of successful shards
         */
        public int successfulShards() {
            int i = 0;
            for (SyncedFlushResponse result : shardResponses.values()) {
                if (result.success()) {
                    i++;
                }
            }
            return i;
        }

        /**
         * @return Individual responses for each shard copy with a detailed failure message if the copy failed to perform the synced flush.
         * Empty if synced flush failed before step three.
         */
        public Map<ShardRouting, SyncedFlushResponse> shardResponses() {
            return shardResponses;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(failureReason);
            out.writeOptionalString(syncId);
            out.writeVInt(shardResponses.size());
            for (Map.Entry<ShardRouting, SyncedFlushResponse> result : shardResponses.entrySet()) {
                result.getKey().writeTo(out);
                result.getValue().writeTo(out);
            }
            shardId.writeTo(out);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            failureReason = in.readOptionalString();
            syncId = in.readOptionalString();
            int size = in.readVInt();
            ImmutableMap.Builder<ShardRouting, SyncedFlushResponse> builder = ImmutableMap.builder();
            for (int i = 0; i < size; i++) {
                ImmutableShardRouting shardRouting = ImmutableShardRouting.readShardRoutingEntry(in);
                SyncedFlushResponse syncedFlushRsponse = new SyncedFlushResponse();
                syncedFlushRsponse.readFrom(in);
                builder.put(shardRouting, syncedFlushRsponse);
            }
            shardResponses = builder.build();
            shardId = ShardId.readShardId(in);
        }

        public ShardId shardId() {
            return shardId;
        }
    }

    final static class PreSyncedFlushRequest extends TransportRequest {
        private ShardId shardId;

        PreSyncedFlushRequest() {
        }

        public PreSyncedFlushRequest(ShardId shardId) {
            this.shardId = shardId;
        }

        @Override
        public String toString() {
            return "PreSyncedFlushRequest{" +
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

    static final class SyncedFlushRequest extends TransportRequest {

        private String syncId;
        private Engine.CommitId expectedCommitId;
        private ShardId shardId;

        public SyncedFlushRequest() {
        }

        public SyncedFlushRequest(ShardId shardId, String syncId, Engine.CommitId expectedCommitId) {
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
            return "SyncedFlushRequest{" +
                    "shardId=" + shardId +
                    ",syncId='" + syncId + '\'' +
                    '}';
        }
    }

    /**
     * Response for third step of synced flush (writing the sync id) for one shard copy
     */
    public static final class SyncedFlushResponse extends TransportResponse {

        /**
         * a non null value indicates a failure to sync flush. null means success
         */
        String failureReason;

        public SyncedFlushResponse() {
            failureReason = null;
        }

        public SyncedFlushResponse(String failureReason) {
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
            return "SyncedFlushResponse{" +
                    "success=" + success() +
                    ", failureReason='" + failureReason + '\'' +
                    '}';
        }
    }


    static final class InFlightOpsRequest extends TransportRequest {

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

    private final class PreSyncedFlushTransportHandler implements TransportRequestHandler<PreSyncedFlushRequest> {

        @Override
        public void messageReceived(PreSyncedFlushRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(performPreSyncedFlush(request));
        }
    }

    private final class SyncedFlushTransportHandler implements TransportRequestHandler<SyncedFlushRequest> {

        @Override
        public void messageReceived(SyncedFlushRequest request, TransportChannel channel) throws Exception {
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
