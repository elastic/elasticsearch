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

package org.elasticsearch.indices.store;

import org.apache.lucene.store.StoreRateLimiting;
import org.elasticsearch.cluster.*;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.node.settings.NodeSettingsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.*;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class IndicesStore extends AbstractComponent implements ClusterStateListener, Closeable {

    public static final String INDICES_STORE_THROTTLE_TYPE = "indices.store.throttle.type";
    public static final String INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC = "indices.store.throttle.max_bytes_per_sec";
    public static final String INDICES_STORE_DELETE_SHARD_TIMEOUT = "indices.store.delete.shard.timeout";

    public static final String ACTION_SHARD_EXISTS = "internal:index/shard/exists";

    private static final EnumSet<IndexShardState> ACTIVE_STATES = EnumSet.of(IndexShardState.STARTED, IndexShardState.RELOCATED);

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            String rateLimitingType = settings.get(INDICES_STORE_THROTTLE_TYPE, IndicesStore.this.rateLimitingType);
            // try and parse the type
            StoreRateLimiting.Type.fromString(rateLimitingType);
            if (!rateLimitingType.equals(IndicesStore.this.rateLimitingType)) {
                logger.info("updating indices.store.throttle.type from [{}] to [{}]", IndicesStore.this.rateLimitingType, rateLimitingType);
                IndicesStore.this.rateLimitingType = rateLimitingType;
                IndicesStore.this.rateLimiting.setType(rateLimitingType);
            }

            ByteSizeValue rateLimitingThrottle = settings.getAsBytesSize(INDICES_STORE_THROTTLE_MAX_BYTES_PER_SEC, IndicesStore.this.rateLimitingThrottle);
            if (!rateLimitingThrottle.equals(IndicesStore.this.rateLimitingThrottle)) {
                logger.info("updating indices.store.throttle.max_bytes_per_sec from [{}] to [{}], note, type is [{}]", IndicesStore.this.rateLimitingThrottle, rateLimitingThrottle, IndicesStore.this.rateLimitingType);
                IndicesStore.this.rateLimitingThrottle = rateLimitingThrottle;
                IndicesStore.this.rateLimiting.setMaxRate(rateLimitingThrottle);
            }
        }
    }

    private final NodeSettingsService nodeSettingsService;

    private final IndicesService indicesService;

    private final ClusterService clusterService;
    private final TransportService transportService;

    private volatile String rateLimitingType;
    private volatile ByteSizeValue rateLimitingThrottle;
    private final StoreRateLimiting rateLimiting = new StoreRateLimiting();

    private final ApplySettings applySettings = new ApplySettings();

    private TimeValue deleteShardTimeout;

    @Inject
    public IndicesStore(Settings settings, NodeSettingsService nodeSettingsService, IndicesService indicesService,
                        ClusterService clusterService, TransportService transportService) {
        super(settings);
        this.nodeSettingsService = nodeSettingsService;
        this.indicesService = indicesService;
        this.clusterService = clusterService;
        this.transportService = transportService;
        transportService.registerRequestHandler(ACTION_SHARD_EXISTS, ShardActiveRequest::new, ThreadPool.Names.SAME, new ShardActiveRequestHandler());

        // we don't limit by default (we default to CMS's auto throttle instead):
        this.rateLimitingType = settings.get("indices.store.throttle.type", StoreRateLimiting.Type.NONE.name());
        rateLimiting.setType(rateLimitingType);
        this.rateLimitingThrottle = settings.getAsBytesSize("indices.store.throttle.max_bytes_per_sec", new ByteSizeValue(10240, ByteSizeUnit.MB));
        rateLimiting.setMaxRate(rateLimitingThrottle);

        this.deleteShardTimeout = settings.getAsTime(INDICES_STORE_DELETE_SHARD_TIMEOUT, new TimeValue(30, TimeUnit.SECONDS));

        logger.debug("using indices.store.throttle.type [{}], with index.store.throttle.max_bytes_per_sec [{}]", rateLimitingType, rateLimitingThrottle);

        nodeSettingsService.addListener(applySettings);
        clusterService.addLast(this);
    }

    IndicesStore() {
        super(Settings.EMPTY);
        nodeSettingsService = null;
        indicesService = null;
        this.clusterService = null;
        this.transportService = null;
    }

    public StoreRateLimiting rateLimiting() {
        return this.rateLimiting;
    }

    @Override
    public void close() {
        nodeSettingsService.removeListener(applySettings);
        clusterService.remove(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (!event.routingTableChanged()) {
            return;
        }

        if (event.state().blocks().disableStatePersistence()) {
            return;
        }

        for (IndexRoutingTable indexRoutingTable : event.state().routingTable()) {
            // Note, closed indices will not have any routing information, so won't be deleted
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                if (shardCanBeDeleted(event.state(), indexShardRoutingTable)) {
                    ShardId shardId = indexShardRoutingTable.shardId();
                    if (indicesService.canDeleteShardContent(shardId, event.state().getMetaData().index(shardId.getIndex()))) {
                        deleteShardIfExistElseWhere(event.state(), indexShardRoutingTable);
                    }
                }
            }
        }
    }

    boolean shardCanBeDeleted(ClusterState state, IndexShardRoutingTable indexShardRoutingTable) {
        // a shard can be deleted if all its copies are active, and its not allocated on this node
        if (indexShardRoutingTable.size() == 0) {
            // should not really happen, there should always be at least 1 (primary) shard in a
            // shard replication group, in any case, protected from deleting something by mistake
            return false;
        }

        for (ShardRouting shardRouting : indexShardRoutingTable) {
            // be conservative here, check on started, not even active
            if (!shardRouting.started()) {
                return false;
            }

            // if the allocated or relocation node id doesn't exists in the cluster state  it may be a stale node,
            // make sure we don't do anything with this until the routing table has properly been rerouted to reflect
            // the fact that the node does not exists
            DiscoveryNode node = state.nodes().get(shardRouting.currentNodeId());
            if (node == null) {
                return false;
            }
            if (shardRouting.relocatingNodeId() != null) {
                node = state.nodes().get(shardRouting.relocatingNodeId());
                if (node == null) {
                    return false;
                }
            }

            // check if shard is active on the current node or is getting relocated to the our node
            String localNodeId = state.getNodes().localNode().id();
            if (localNodeId.equals(shardRouting.currentNodeId()) || localNodeId.equals(shardRouting.relocatingNodeId())) {
                return false;
            }
        }

        return true;
    }

    // TODO will have to ammend this for shadow replicas so we don't delete the shared copy...
    private void deleteShardIfExistElseWhere(ClusterState state, IndexShardRoutingTable indexShardRoutingTable) {
        List<Tuple<DiscoveryNode, ShardActiveRequest>> requests = new ArrayList<>(indexShardRoutingTable.size());
        String indexUUID = state.getMetaData().index(indexShardRoutingTable.shardId().getIndex()).getIndexUUID();
        ClusterName clusterName = state.getClusterName();
        for (ShardRouting shardRouting : indexShardRoutingTable) {
            // Node can't be null, because otherwise shardCanBeDeleted() would have returned false
            DiscoveryNode currentNode = state.nodes().get(shardRouting.currentNodeId());
            assert currentNode != null;

            requests.add(new Tuple<>(currentNode, new ShardActiveRequest(clusterName, indexUUID, shardRouting.shardId(), deleteShardTimeout)));
            if (shardRouting.relocatingNodeId() != null) {
                DiscoveryNode relocatingNode = state.nodes().get(shardRouting.relocatingNodeId());
                assert relocatingNode != null;
                requests.add(new Tuple<>(relocatingNode, new ShardActiveRequest(clusterName, indexUUID, shardRouting.shardId(), deleteShardTimeout)));
            }
        }

        ShardActiveResponseHandler responseHandler = new ShardActiveResponseHandler(indexShardRoutingTable.shardId(), state, requests.size());
        for (Tuple<DiscoveryNode, ShardActiveRequest> request : requests) {
            logger.trace("{} sending shard active check to {}", request.v2().shardId, request.v1());
            transportService.sendRequest(request.v1(), ACTION_SHARD_EXISTS, request.v2(), responseHandler);
        }
    }

    private class ShardActiveResponseHandler implements TransportResponseHandler<ShardActiveResponse> {

        private final ShardId shardId;
        private final int expectedActiveCopies;
        private final ClusterState clusterState;
        private final AtomicInteger awaitingResponses;
        private final AtomicInteger activeCopies;

        public ShardActiveResponseHandler(ShardId shardId, ClusterState clusterState, int expectedActiveCopies) {
            this.shardId = shardId;
            this.expectedActiveCopies = expectedActiveCopies;
            this.clusterState = clusterState;
            this.awaitingResponses = new AtomicInteger(expectedActiveCopies);
            this.activeCopies = new AtomicInteger();
        }

        @Override
        public ShardActiveResponse newInstance() {
            return new ShardActiveResponse();
        }

        @Override
        public void handleResponse(ShardActiveResponse response) {
            logger.trace("{} is {}active on node {}", shardId, response.shardActive ? "" : "not ", response.node);
            if (response.shardActive) {
                activeCopies.incrementAndGet();
            }

            if (awaitingResponses.decrementAndGet() == 0) {
                allNodesResponded();
            }
        }

        @Override
        public void handleException(TransportException exp) {
            logger.debug("shards active request failed for {}", exp, shardId);
            if (awaitingResponses.decrementAndGet() == 0) {
                allNodesResponded();
            }
        }

        @Override
        public String executor() {
            return ThreadPool.Names.SAME;
        }

        private void allNodesResponded() {
            if (activeCopies.get() != expectedActiveCopies) {
                logger.trace("not deleting shard {}, expected {} active copies, but only {} found active copies", shardId, expectedActiveCopies, activeCopies.get());
                return;
            }

            ClusterState latestClusterState = clusterService.state();
            if (clusterState.getVersion() != latestClusterState.getVersion()) {
                logger.trace("not deleting shard {}, the latest cluster state version[{}] is not equal to cluster state before shard active api call [{}]", shardId, latestClusterState.getVersion(), clusterState.getVersion());
                return;
            }

            clusterService.submitStateUpdateTask("indices_store ([" + shardId + "] active fully on other nodes)", new ClusterStateNonMasterUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    if (clusterState.getVersion() != currentState.getVersion()) {
                        logger.trace("not deleting shard {}, the update task state version[{}] is not equal to cluster state before shard active api call [{}]", shardId, currentState.getVersion(), clusterState.getVersion());
                        return currentState;
                    }
                    try {
                        indicesService.deleteShardStore("no longer used", shardId, currentState);
                    } catch (Throwable ex) {
                        logger.debug("{} failed to delete unallocated shard, ignoring", ex, shardId);
                    }
                    return currentState;
                }

                @Override
                public void onFailure(String source, Throwable t) {
                    logger.error("{} unexpected error during deletion of unallocated shard", t, shardId);
                }
            });
        }

    }

    private class ShardActiveRequestHandler implements TransportRequestHandler<ShardActiveRequest> {

        @Override
        public void messageReceived(final ShardActiveRequest request, final TransportChannel channel) throws Exception {
            IndexShard indexShard = getShard(request);
            // make sure shard is really there before register cluster state observer
            if (indexShard == null) {
                channel.sendResponse(new ShardActiveResponse(false, clusterService.localNode()));
            } else {
                // create observer here. we need to register it here because we need to capture the current cluster state
                // which will then be compared to the one that is applied when we call waitForNextChange(). if we create it
                // later we might miss an update and wait forever in case no new cluster state comes in.
                // in general, using a cluster state observer here is a workaround for the fact that we cannot listen on shard state changes explicitly.
                // instead we wait for the cluster state changes because we know any shard state change will trigger or be
                // triggered by a cluster state change.
                ClusterStateObserver observer = new ClusterStateObserver(clusterService, request.timeout, logger);
                // check if shard is active. if so, all is good
                boolean shardActive = shardActive(indexShard);
                if (shardActive) {
                    channel.sendResponse(new ShardActiveResponse(true, clusterService.localNode()));
                } else {
                    // shard is not active, might be POST_RECOVERY so check if cluster state changed inbetween or wait for next change
                    observer.waitForNextChange(new ClusterStateObserver.Listener() {
                        @Override
                        public void onNewClusterState(ClusterState state) {
                            sendResult(shardActive(getShard(request)));
                        }

                        @Override
                        public void onClusterServiceClose() {
                            sendResult(false);
                        }

                        @Override
                        public void onTimeout(TimeValue timeout) {
                            sendResult(shardActive(getShard(request)));
                        }

                        public void sendResult(boolean shardActive) {
                            try {
                                channel.sendResponse(new ShardActiveResponse(shardActive, clusterService.localNode()));
                            } catch (IOException e) {
                                logger.error("failed send response for shard active while trying to delete shard {} - shard will probably not be removed", e, request.shardId);
                            } catch (EsRejectedExecutionException e) {
                                logger.error("failed send response for shard active while trying to delete shard {} - shard will probably not be removed", e, request.shardId);
                            }
                        }
                    }, new ClusterStateObserver.ValidationPredicate() {
                        @Override
                        protected boolean validate(ClusterState newState) {
                            // the shard is not there in which case we want to send back a false (shard is not active), so the cluster state listener must be notified
                            // or the shard is active in which case we want to send back that the shard is active
                            // here we could also evaluate the cluster state and get the information from there. we
                            // don't do it because we would have to write another method for this that would have the same effect
                            IndexShard indexShard = getShard(request);
                            return indexShard == null || shardActive(indexShard);
                        }
                    });
                }
            }
        }

        private boolean shardActive(IndexShard indexShard) {
            if (indexShard != null) {
                return ACTIVE_STATES.contains(indexShard.state());
            }
            return false;
        }

        private IndexShard getShard(ShardActiveRequest request) {
            ClusterName thisClusterName = clusterService.state().getClusterName();
            if (!thisClusterName.equals(request.clusterName)) {
                logger.trace("shard exists request meant for cluster[{}], but this is cluster[{}], ignoring request", request.clusterName, thisClusterName);
                return null;
            }

            ShardId shardId = request.shardId;
            IndexService indexService = indicesService.indexService(shardId.index().getName());
            if (indexService != null && indexService.indexUUID().equals(request.indexUUID)) {
                return indexService.shard(shardId.id());
            }
            return null;
        }
    }

    public static class ShardActiveRequest extends TransportRequest {
        protected TimeValue timeout = null;
        private ClusterName clusterName;
        private String indexUUID;
        private ShardId shardId;

        public ShardActiveRequest() {
        }

        ShardActiveRequest(ClusterName clusterName, String indexUUID, ShardId shardId, TimeValue timeout) {
            this.shardId = shardId;
            this.indexUUID = indexUUID;
            this.clusterName = clusterName;
            this.timeout = timeout;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            clusterName = ClusterName.readClusterName(in);
            indexUUID = in.readString();
            shardId = ShardId.readShardId(in);
            timeout = new TimeValue(in.readLong(), TimeUnit.MILLISECONDS);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            clusterName.writeTo(out);
            out.writeString(indexUUID);
            shardId.writeTo(out);
            out.writeLong(timeout.millis());
        }
    }

    private static class ShardActiveResponse extends TransportResponse {

        private boolean shardActive;
        private DiscoveryNode node;

        ShardActiveResponse() {
        }

        ShardActiveResponse(boolean shardActive, DiscoveryNode node) {
            this.shardActive = shardActive;
            this.node = node;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            shardActive = in.readBoolean();
            node = DiscoveryNode.readNode(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeBoolean(shardActive);
            node.writeTo(out);
        }
    }
}
