/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.reshard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterShardHealth;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;

import java.util.concurrent.ConcurrentHashMap;

public class SplitTargetService {

    public static final Setting<TimeValue> RESHARD_SPLIT_SEARCH_SHARDS_ONLINE_TIMEOUT = Setting.positiveTimeSetting(
        "reshard.split.search_shards_online_timeout",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope
    );

    private static final Logger logger = LogManager.getLogger(SplitTargetService.class);

    private final Client client;
    private final ClusterService clusterService;
    private final TimeValue searchShardsOnlineTimeout;
    private final ConcurrentHashMap<IndexShard, Split> onGoingSplits = new ConcurrentHashMap<>();

    public SplitTargetService(Settings settings, Client client, ClusterService clusterService) {
        this.client = client;
        this.clusterService = clusterService;
        this.searchShardsOnlineTimeout = RESHARD_SPLIT_SEARCH_SHARDS_ONLINE_TIMEOUT.get(settings);
    }

    public void startSplitRecovery(IndexShard indexShard, ActionListener<Void> listener) {
        ShardId shardId = indexShard.shardId();

        long targetPrimaryTerm = indexShard.getOperationPrimaryTerm();

        ClusterState state = clusterService.state();
        final ProjectState projectState = state.projectState(state.metadata().projectFor(shardId.getIndex()).id());
        final IndexMetadata indexMetadata = projectState.metadata().getIndexSafe(shardId.getIndex());
        final IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();
        final IndexRoutingTable indexRoutingTable = projectState.routingTable().index(shardId.getIndex());
        final DiscoveryNode sourceNode = state.nodes()
            .get(indexRoutingTable.shard(reshardingMetadata.getSplit().sourceShard(shardId.id())).primaryShard().currentNodeId());
        long sourcePrimaryTerm = indexMetadata.primaryTerm(reshardingMetadata.getSplit().sourceShard(shardId.id()));

        Split split = new Split(shardId, sourceNode, clusterService.localNode(), sourcePrimaryTerm, targetPrimaryTerm);
        onGoingSplits.put(indexShard, split);

        if (reshardingMetadata.getSplit().targetStateAtLeast(shardId.id(), IndexReshardingState.Split.TargetShardState.HANDOFF)) {
            listener.onResponse(null);
        } else {
            client.execute(TransportReshardSplitAction.TYPE, new TransportReshardSplitAction.SplitRequest(split), listener.map(r -> null));
        }
    }

    public void afterIndexShardSplitRecovery(IndexShard indexShard, ActionListener<Void> listener) {
        ShardId shardId = indexShard.shardId();
        Split split = onGoingSplits.get(indexShard);
        if (split == null) {
            listener.onFailure(new IllegalStateException("No on-going split found for shard " + shardId));
            return;
        }

        ClusterState state = clusterService.state();
        final ProjectState projectState = state.projectState(state.metadata().projectFor(shardId.getIndex()).id());
        final IndexReshardingMetadata reshardingMetadata = projectState.metadata().getIndexSafe(shardId.getIndex()).getReshardingMetadata();

        switch (reshardingMetadata.getSplit().getTargetShardState(shardId.id())) {
            case CLONE -> throw new IllegalStateException("Cannot make it here is still CLONE");
            case HANDOFF -> moveToSplitStep(indexShard, split);
            case SPLIT -> moveToDoneStep(indexShard, split);
            case DONE -> onGoingSplits.remove(indexShard);
        }

        listener.onResponse(null);
    }

    private void moveToSplitStep(IndexShard indexShard, Split split) {
        ShardId shardId = indexShard.shardId();
        ClusterStateObserver observer = new ClusterStateObserver(
            clusterService,
            searchShardsOnlineTimeout,
            logger,
            clusterService.threadPool().getThreadContext()
        );
        observer.waitForNextChange(new ClusterStateObserver.Listener() {
            @Override
            public void onNewClusterState(ClusterState state) {
                final Index index = shardId.getIndex();
                final ProjectMetadata projectMetadata = state.metadata().lookupProject(index).orElse(null);
                if (projectMetadata != null) {
                    if (newPrimaryTerm(index, projectMetadata, shardId, split.targetPrimaryTerm()) == false) {
                        assert isShardGreen(state, projectMetadata, shardId);
                        advancedToSplit();
                    }
                }
            }

            @Override
            public void onClusterServiceClose() {
                // Ignore. No action needed
            }

            @Override
            public void onTimeout(TimeValue timeout) {
                // After the timeout we proceed to SPLIT. The timeout is just best effort to ensure search shards are running to prevent
                // downtime.
                advancedToSplit();
            }

            private void advancedToSplit() {
                ChangeState changeState = new ChangeState(
                    indexShard,
                    split,
                    IndexReshardingState.Split.TargetShardState.SPLIT,
                    new ActionListener<>() {
                        @Override
                        public void onResponse(ActionResponse actionResponse) {
                            moveToDoneStep(indexShard, split);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            stateError(indexShard, split, IndexReshardingState.Split.TargetShardState.SPLIT, e);
                        }
                    }
                );
                changeState.run();
            }
        }, newState -> searchShardsOnlineOrNewPrimaryTerm(newState, shardId, split.targetPrimaryTerm()));
    }

    private void moveToDoneStep(IndexShard indexShard, Split split) {
        // TODO: RUN DELETE-BY-QUERY BEFORE CHANGING STATE
        ChangeState changeState = new ChangeState(
            indexShard,
            split,
            IndexReshardingState.Split.TargetShardState.DONE,
            new ActionListener<>() {
                @Override
                public void onResponse(ActionResponse actionResponse) {
                    onGoingSplits.remove(indexShard);
                }

                @Override
                public void onFailure(Exception e) {
                    stateError(indexShard, split, IndexReshardingState.Split.TargetShardState.DONE, e);
                }
            }
        );
        changeState.run();
    }

    private void stateError(IndexShard shard, Split split, IndexReshardingState.Split.TargetShardState state, Exception e) {
        if (onGoingSplits.get(shard) == split) {
            // TODO: Consider failing shard if this happens
            logger.error(Strings.format("unexpected failure to transition target state to %s", state), e);
            assert false;
        }
    }

    private class ChangeState extends RetryableAction<ActionResponse> {

        private final IndexShard indexShard;
        private final Split split;
        private final SplitStateRequest splitStateRequest;

        private ChangeState(
            IndexShard indexShard,
            Split split,
            IndexReshardingState.Split.TargetShardState state,
            ActionListener<ActionResponse> listener
        ) {
            super(
                logger,
                clusterService.threadPool(),
                TimeValue.timeValueMillis(10),
                TimeValue.timeValueSeconds(5),
                TimeValue.MAX_VALUE,
                listener,
                clusterService.threadPool().generic()
            );
            this.indexShard = indexShard;
            this.split = split;
            this.splitStateRequest = new SplitStateRequest(
                indexShard.shardId(),
                state,
                split.sourcePrimaryTerm(),
                split.targetPrimaryTerm()
            );
        }

        @Override
        public void tryAction(ActionListener<ActionResponse> listener) {
            if (onGoingSplits.get(indexShard) == split) {
                client.execute(TransportUpdateSplitStateAction.TYPE, splitStateRequest, listener);
            } else {
                listener.onFailure(new AlreadyClosedException("IndexShard has been closed."));
            }
        }

        @Override
        public boolean shouldRetry(Exception e) {
            // Retry forever unless the split is removed which happens when the shard closes
            return onGoingSplits.get(indexShard) == split;
        }
    }

    public void cancelSplits(IndexShard indexShard) {
        onGoingSplits.remove(indexShard);
    }

    private static boolean searchShardsOnlineOrNewPrimaryTerm(ClusterState state, ShardId shardId, long primaryTerm) {
        final Index index = shardId.getIndex();
        final ProjectMetadata projectMetadata = state.metadata().lookupProject(index).orElse(null);
        if (projectMetadata == null) {
            logger.debug("index not found while checking if shard {} is search shards online", shardId);
            return false;
        }

        return newPrimaryTerm(index, projectMetadata, shardId, primaryTerm) || isShardGreen(state, projectMetadata, shardId);
    }

    private static boolean newPrimaryTerm(Index index, ProjectMetadata projectMetadata, ShardId shardId, long primaryTerm) {
        long currentPrimaryTerm = projectMetadata.index(index).primaryTerm(shardId.id());
        return currentPrimaryTerm > primaryTerm;
    }

    private static boolean isShardGreen(ClusterState state, ProjectMetadata projectMetadata, ShardId shardId) {
        final Index index = shardId.getIndex();

        var indexRoutingTable = state.routingTable(projectMetadata.id()).index(index);
        if (indexRoutingTable == null) {
            // This should not be possible since https://github.com/elastic/elasticsearch/issues/33888
            logger.warn("found no index routing for {} but found it in metadata", shardId);
            assert false;
            // better safe than sorry in this case.
            return false;
        }
        var shardRoutingTable = indexRoutingTable.shard(shardId.id());
        assert shardRoutingTable != null;
        return new ClusterShardHealth(shardId.getId(), shardRoutingTable).getStatus() == ClusterHealthStatus.GREEN;
    }

    record Split(ShardId shardId, DiscoveryNode sourceNode, DiscoveryNode targetNode, long sourcePrimaryTerm, long targetPrimaryTerm) {}
}
