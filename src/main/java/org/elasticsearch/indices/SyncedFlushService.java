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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.node.liveness.LivenessRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexShardMissingException;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SyncedFlushService extends AbstractComponent {

    // nocommmit: check these are ok
    public static final String PRE_SYNCED_FLUSH_ACTION_NAME = "internal:indices/flush/synced/pre";
    public static final String SYNCED_FLUSH_ACTION_NAME = "internal:indices/flush/synced/sync";
    public static final String IN_FLIGHT_OPS_ACTION_NAME = "internal:indices/flush/synced/in_flight";

    public static final String SETTING_PRE_SYNC_TIMEOUT = "indices.flush.synced.presync_timeout";
    public static final String SETTING_SYNC_TIMEOUT = "indices.flush.synced.sync_timeout";
    public static final String SETTING_IN_FLIGHT_OPS_TIMEOUT = "indices.flush.synced.in_flight_ops_timeout";

    private final IndicesService indicesService;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final TimeValue preSyncTimeout;
    private final TimeValue syncTimeout;
    private final TimeValue inflightOpsTimeout;

    @Inject
    public SyncedFlushService(Settings settings, IndicesService indicesService, ClusterService clusterService, TransportService transportService) {
        super(settings);
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.transportService = transportService;

        transportService.registerRequestHandler(PRE_SYNCED_FLUSH_ACTION_NAME, PreSyncedFlushRequest.class, ThreadPool.Names.FLUSH, new PreSyncedFlushTransportHandler());
        transportService.registerRequestHandler(SYNCED_FLUSH_ACTION_NAME, SyncedFlushRequest.class, ThreadPool.Names.FLUSH, new SyncedFlushTransportHandler());
        transportService.registerRequestHandler(IN_FLIGHT_OPS_ACTION_NAME, InFlightOpsRequest.class, ThreadPool.Names.SAME, new InFlightOpCountTransportHandler());
        preSyncTimeout = settings.getAsTime(SETTING_PRE_SYNC_TIMEOUT, TimeValue.timeValueMinutes(5));
        syncTimeout = settings.getAsTime(SETTING_SYNC_TIMEOUT, TimeValue.timeValueMinutes(5));
        inflightOpsTimeout = settings.getAsTime(SETTING_IN_FLIGHT_OPS_TIMEOUT, TimeValue.timeValueMinutes(5));
        indicesService.indicesLifecycle().addListener(new IndicesLifecycle.Listener() {
            @Override
            public void onShardInactive(IndexShard indexShard) {
                // we only want to call sync flush once, so only trigger it when we are on a primary
                if (indexShard.routingEntry().primary()) {
                    attemptSyncedFlush(indexShard.shardId());
                }
            }
        });
    }

    public SyncedFlushResult attemptSyncedFlush(ShardId shardId) {
        final ClusterState state = clusterService.state();
        final IndexRoutingTable indexRoutingTable = state.routingTable().index(shardId.index().name());
        if (indexRoutingTable == null) {
            throw new IndexMissingException(shardId.index());
        }
        final IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId.id());
        if (shardRoutingTable == null) {
            throw new IndexShardMissingException(shardId);
        }
        final List<ShardRouting> activeShards = shardRoutingTable.activeShards();
        Map<String, byte[]> commitIds = sendPreSyncRequests(activeShards, state, shardId);

        if (commitIds.isEmpty()) {
            return new SyncedFlushResult("all shards failed to commit on pre-sync");
        }

        int inflight = getInflightOpsCount(shardId, state, shardRoutingTable);
        if (inflight != 1) {
            return new SyncedFlushResult("operation counter on primary is non zero [" + inflight + "]");
        }

        String syncId = Strings.base64UUID();
        Map<ShardRouting, SyncedFlushResponse> results = sendSyncRequests(syncId, activeShards, state, commitIds, shardId);

        return new SyncedFlushResult(syncId, results);
    }

    /**
     * returns the number of inflight operations on primary. -1 upon error.
     */
    protected int getInflightOpsCount(final ShardId shardId, ClusterState state, IndexShardRoutingTable shardRoutingTable) {
        final ShardRouting primaryShard = shardRoutingTable.primaryShard();
        final DiscoveryNode primaryNode = state.nodes().get(primaryShard.currentNodeId());
        if (primaryNode == null) {
            logger.trace("{} failed to resolve node for primary shard {}, skipping sync", shardId, primaryShard);
            return -1;
        }
        final AtomicInteger result = new AtomicInteger(-1);
        final CountDownLatch latch = new CountDownLatch(1);
        transportService.sendRequest(primaryNode, IN_FLIGHT_OPS_ACTION_NAME, new InFlightOpsRequest(shardId),
                new BaseTransportResponseHandler<InFlightOpsResponse>() {
                    @Override
                    public InFlightOpsResponse newInstance() {
                        return new InFlightOpsResponse();
                    }

                    @Override
                    public void handleResponse(InFlightOpsResponse response) {
                        result.set(response.opCount());
                        latch.countDown();
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.debug("{} unexpected error while retrieving inflight op count", shardId);
                        result.set(-1);
                        latch.countDown();
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                });
        try {
            if (latch.await(inflightOpsTimeout.millis(), TimeUnit.MILLISECONDS) == false) {
                logger.debug("{} in flight operation check timed out after [{}]", shardId, syncTimeout);
            }
        } catch (InterruptedException e) {
            logger.debug("{} interrupted while waiting for in flight operation check", shardId);
        }

        final int count = result.get();
        logger.trace("{} in flight operation count [{}]", shardId, count);
        return count;
    }


    private Map<ShardRouting, SyncedFlushResponse> sendSyncRequests(final String syncId, List<ShardRouting> shards, ClusterState state, Map<String, byte[]> expectedCommitIds, final ShardId shardId) {
        final CountDownLatch countDownLatch = new CountDownLatch(shards.size());
        final Map<ShardRouting, SyncedFlushResponse> results = ConcurrentCollections.newConcurrentMap();
        for (final ShardRouting shard : shards) {
            final DiscoveryNode node = state.nodes().get(shard.currentNodeId());
            if (node == null) {
                logger.trace("{} is assigned to an unknown node. skipping for sync id [{}]. shard routing {}", shardId, syncId, shard);
                results.put(shard, new SyncedFlushResponse("unknown node"));
                countDownLatch.countDown();
                continue;
            }
            final byte[] expectedCommitId = expectedCommitIds.get(shard.currentNodeId());
            if (expectedCommitId == null) {
                logger.trace("{} can't resolve expected commit id for {}, skipping for sync id [{}]. shard routing {}", shardId, syncId, shard);
                results.put(shard, new SyncedFlushResponse("no commit id from pre-sync flush"));
                countDownLatch.countDown();
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
                            countDownLatch.countDown();
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            logger.trace("{} error while performing synced flush on [{}], skipping", exp, shardId, shard);
                            results.put(shard, new SyncedFlushResponse(exp.getMessage()));
                            countDownLatch.countDown();
                        }

                        @Override
                        public String executor() {
                            return ThreadPool.Names.SAME;
                        }
                    });
        }
        try {
            if (countDownLatch.await(syncTimeout.millis(), TimeUnit.MILLISECONDS) == false) {
                logger.debug("{} waiting for synced flush with id [{}] timed out after [{}]. pending ops [{}]", shardId, syncId, syncTimeout, countDownLatch.getCount());
            }
        } catch (InterruptedException e) {
            logger.debug("{} interrupted while waiting for sync requests (sync id [{}])", shardId, syncId);
        }

        return results;
    }

    /**
     * send presync requests to all started copies of the given shard
     */
    Map<String, byte[]> sendPreSyncRequests(final List<ShardRouting> shards, final ClusterState state, final ShardId shardId) {
        final CountDownLatch countDownLatch = new CountDownLatch(shards.size());
        final Map<String, byte[]> commitIds = ConcurrentCollections.newConcurrentMap();
        for (final ShardRouting shard : shards) {
            logger.trace("{} sending pre-synced flush request to {}", shardId, shard);
            final DiscoveryNode node = state.nodes().get(shard.currentNodeId());
            if (node == null) {
                logger.trace("{} shard routing {} refers to an unknown node. skipping.", shardId, shard);
                countDownLatch.countDown();
                continue;
            }
            transportService.sendRequest(node, PRE_SYNCED_FLUSH_ACTION_NAME, new PreSyncedFlushRequest(shard.shardId()), new BaseTransportResponseHandler<PreSyncedFlushResponse>() {
                @Override
                public PreSyncedFlushResponse newInstance() {
                    return new PreSyncedFlushResponse();
                }

                @Override
                public void handleResponse(PreSyncedFlushResponse response) {
                    byte[] existing = commitIds.put(node.id(), response.commitId());
                    assert existing == null : "got two answers for node [" + node + "]";
                    // count after the assert so we won't decrement twice in handleException
                    countDownLatch.countDown();
                }

                @Override
                public void handleException(TransportException exp) {
                    logger.trace("{} error while performing pre synced flush on [{}], skipping", shardId, exp, shard);
                    countDownLatch.countDown();
                }

                @Override
                public String executor() {
                    return ThreadPool.Names.SAME;
                }
            });
        }
        try {
            if (countDownLatch.await(preSyncTimeout.millis(), TimeUnit.MILLISECONDS) == false) {
                logger.debug("{} waiting for pre sync flush requests timed out after [{}]. pending ops [{}]", shardId, preSyncTimeout, countDownLatch.getCount());
            }
        } catch (InterruptedException e) {
            logger.debug("{} interrupted while waiting for presync requests", shardId);
        }

        return commitIds;
    }

    private PreSyncedFlushResponse performPreSyncedFlush(PreSyncedFlushRequest request) {
        IndexShard indexShard = indicesService.indexServiceSafe(request.shardId().getIndex()).shardSafe(request.shardId().id());
        FlushRequest flushRequest = new FlushRequest().force(false).waitIfOngoing(true);
        logger.trace("{} performing pre sync flush", request.shardId());
        byte[] id = indexShard.flush(flushRequest);
        logger.trace("{} pre sync flush done. commit id {}", request.shardId(), id);
        return new PreSyncedFlushResponse(id);
    }

    private SyncedFlushResponse performSyncedFlush(SyncedFlushRequest request) {
        IndexService indexService = indicesService.indexServiceSafe(request.shardId().getIndex());
        IndexShard indexShard = indexService.shardSafe(request.shardId().id());
        logger.trace("{} performing sync flush. sync id [{}], expected commit id {}", request.shardId(), request.syncId(), request.expectedCommitId());
        Engine.SyncedFlushResult result = indexShard.syncFlushIfNoPendingChanges(request.syncId(), request.expectedCommitId());
        logger.trace("{} sync flush done. sync id [{}], result  [{}]", request.shardId(), request.syncId(), result);
        switch (result) {
            case SUCCESS:
                return new SyncedFlushResponse();
            case FAILED_COMMIT_MISMATCH:
                return new SyncedFlushResponse("commit has changed");
            case FAILED_PENDING_OPERATIONS:
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

    public static class SyncedFlushResult {
        private final String failureReason;
        private final Map<ShardRouting, SyncedFlushResponse> shardResponses;
        private final String syncId;

        /**
         * failure constructor
         */
        SyncedFlushResult(String failureReason) {
            this.syncId = null;
            this.failureReason = failureReason;
            this.shardResponses = new HashMap<>();
        }

        /**
         * success constructor
         */
        SyncedFlushResult(String syncId, Map<ShardRouting, SyncedFlushResponse> shardResponses) {
            this.failureReason = null;
            this.shardResponses = shardResponses;
            this.syncId = syncId;
        }

        public boolean success() {
            return syncId != null;
        }

        public String failureReason() {
            return failureReason;
        }

        public String syncId() {
            return syncId;
        }

        /**
         * total number of shards for which a sync attempt was made
         */
        public int totalShards() {
            return shardResponses.size();
        }

        public int successfulShards() {
            int i = 0;
            for (SyncedFlushResponse result : shardResponses.values()) {
                if (result.success()) {
                    i++;
                }
            }
            return i;
        }

        public Map<ShardRouting, SyncedFlushResponse> shardResponses() {
            return shardResponses;
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

    final static class PreSyncedFlushResponse extends TransportResponse {

        private byte[] commitId;

        PreSyncedFlushResponse() {
        }

        PreSyncedFlushResponse(byte[] commitId) {
            this.commitId = commitId;
        }

        public byte[] commitId() {
            return commitId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            commitId = in.readByteArray();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeByteArray(commitId);
        }
    }

    static final class SyncedFlushRequest extends TransportRequest {

        private String syncId;
        private byte[] expectedCommitId;
        private ShardId shardId;

        public SyncedFlushRequest() {
        }

        public SyncedFlushRequest(ShardId shardId, String syncId, byte[] expectedCommitId) {
            this.expectedCommitId = expectedCommitId;
            this.shardId = shardId;
            this.syncId = syncId;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardId = ShardId.readShardId(in);
            expectedCommitId = in.readByteArray();
            syncId = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            shardId.writeTo(out);
            out.writeByteArray(expectedCommitId);
            out.writeString(syncId);
        }

        public ShardId shardId() {
            return shardId;
        }

        public String syncId() {
            return syncId;
        }

        public byte[] expectedCommitId() {
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

    static final class SyncedFlushResponse extends TransportResponse {

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


    private class PreSyncedFlushTransportHandler implements TransportRequestHandler<PreSyncedFlushRequest> {

        @Override
        public void messageReceived(PreSyncedFlushRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(performPreSyncedFlush(request));
        }
    }


    private class SyncedFlushTransportHandler implements TransportRequestHandler<SyncedFlushRequest> {

        @Override
        public void messageReceived(SyncedFlushRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(performSyncedFlush(request));
        }
    }

    private class InFlightOpCountTransportHandler implements TransportRequestHandler<InFlightOpsRequest> {

        @Override
        public void messageReceived(InFlightOpsRequest request, TransportChannel channel) throws Exception {
            channel.sendResponse(performInFlightOps(request));
        }
    }

}
