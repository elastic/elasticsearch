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

package co.elastic.elasticsearch.stateless.autoscaling.search;

import co.elastic.elasticsearch.serverless.constants.ServerlessSharedSettings;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSize;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSizeStatsClient;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import static co.elastic.elasticsearch.stateless.autoscaling.AutoscalingDataTransmissionLogging.getExceptionLogLevel;

/**
 * This service is responsible for collecting shard size changes on the search nodes
 * and periodically sending updates to the elected master
 */
public class SearchShardSizeCollector extends AbstractLifecycleComponent implements ShardSizeCollector, ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(SearchShardSizeCollector.class);

    public static final Setting<TimeValue> PUSH_INTERVAL_SETTING = Setting.timeSetting(
        "serverless.autoscaling.search_metrics.push_interval",
        TimeValue.timeValueSeconds(30),
        TimeValue.timeValueMillis(250),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static final Setting<ByteSizeValue> PUSH_DELTA_THRESHOLD_SETTING = Setting.byteSizeSetting(
        "serverless.autoscaling.search_metrics.push_delta_threshold",
        ByteSizeValue.ofMb(10),
        ByteSizeValue.ofBytes(1),
        ByteSizeValue.ofTb(1),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    private final ThreadPool threadPool;
    private final Executor executor;
    private final ShardSizeStatsClient shardSizeStatsClient;
    private final ShardSizesPublisher shardSizesPublisher;

    private volatile TimeValue boostWindowInterval;
    private volatile TimeValue publishInterval;
    private volatile ByteSizeValue significantSizeChangeThreshold;

    // Effectively we have a queue of tasks, but all the tasks are copies of publishTask so in fact we just need to track the queue length.
    // We use atomic operations on this field to ensure there's only ever at most one task running at once.
    private final AtomicLong publishTaskQueueLength = new AtomicLong();
    private final AbstractRunnable publishTask = new AbstractRunnable() {
        @Override
        protected void doRun() {
            doPublishShardSizes();
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("Failed to push shard sizes to elected master", e);
            assert false : e;
        }
    };

    private final PendingPublication pendingPublication = new PendingPublication();
    private final ConcurrentMap<ShardId, ShardSize> pastPublications = new ConcurrentHashMap<>();

    private volatile String nodeId;

    private class PendingPublication {
        private Map<ShardId, ShardSize> shards = new HashMap<>();
        private long interactiveSizeDiffInBytes = 0L;

        private synchronized boolean add(ShardId shardId, ShardSize size) {
            var previousSize = pastPublications.get(shardId);
            long delta = size.interactiveSizeInBytes() - (previousSize != null ? previousSize.interactiveSizeInBytes() : 0L);
            if (Objects.equals(previousSize, size) == false) {
                shards.put(shardId, size);
            }
            interactiveSizeDiffInBytes += delta;
            return interactiveSizeDiffInBytes >= significantSizeChangeThreshold.getBytes();
        }

        private synchronized void retry(Map<ShardId, ShardSize> sizes) {
            for (var entry : sizes.entrySet()) {
                shards.putIfAbsent(entry.getKey(), entry.getValue());
            }
        }

        private synchronized Map<ShardId, ShardSize> drain() {
            var shards = this.shards;
            this.shards = new HashMap<>();
            interactiveSizeDiffInBytes = 0;
            return shards;
        }
    }

    public SearchShardSizeCollector(
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        ShardSizeStatsClient shardSizeStatsClient,
        ShardSizesPublisher shardSizesPublisher
    ) {
        this.threadPool = threadPool;
        this.executor = threadPool.generic();
        this.shardSizeStatsClient = shardSizeStatsClient;
        this.shardSizesPublisher = shardSizesPublisher;

        clusterSettings.initializeAndWatch(ServerlessSharedSettings.BOOST_WINDOW_SETTING, value -> {
            this.boostWindowInterval = value;
            if (lifecycle.started()) {
                executor.execute(this::publishAllNow);
            }
        });
        clusterSettings.initializeAndWatch(PUSH_INTERVAL_SETTING, value -> this.publishInterval = value);
        clusterSettings.initializeAndWatch(PUSH_DELTA_THRESHOLD_SETTING, value -> this.significantSizeChangeThreshold = value);
    }

    @Override
    protected void doStart() {
        scheduleFutureDiffPublication();
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {}

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        assert assertNodeId(nodeId, event.state());
        if (nodeId == null) {
            setNodeId(event.state().nodes().getLocalNodeId());
        }
        if (event.nodesDelta().masterNodeChanged()) {
            executor.execute(this::publishAllNow);
        }
        if (event.metadataChanged()) {
            var metadata = event.state().metadata();
            pastPublications.keySet().removeIf(shardId -> metadata.hasIndex(shardId.getIndex()) == false);
        }
        if (event.routingTableChanged()) {
            var routingNode = event.state().getRoutingNodes().node(nodeId);
            var localShards = new HashSet<ShardId>();
            for (ShardRouting shardRouting : routingNode) {
                localShards.add(shardRouting.shardId());
            }
            pastPublications.keySet().retainAll(localShards);
        }
    }

    private static boolean assertNodeId(final String nodeId, final ClusterState clusterState) {
        if (nodeId != null) {
            var localNode = clusterState.nodes().getLocalNode();
            assert localNode.hasRole(DiscoveryNodeRole.SEARCH_ROLE.roleName());
            assert nodeId.equals(localNode.getId());
        }
        return true;
    }

    // Visible for testing
    void setNodeId(String nodeId) {
        this.nodeId = nodeId;
        scheduleFutureDiffPublication();
    }

    @Override
    public void collectShardSize(ShardId shardId) {
        shardSizeStatsClient.getShardSize(shardId, boostWindowInterval, new ActionListener<>() {
            @Override
            public void onResponse(ShardSize shardSize) {
                logger.debug("Detected size {} for shard {}", shardSize, shardId);
                if (shardSize != null && pendingPublication.add(shardId, shardSize)) {
                    publishDiffNow();
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug(() -> "Failed to detected size for shard " + shardId, e);
            }
        });
    }

    private void publishAllNow() {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
        if (nodeId == null) {
            return;
        }
        pendingPublication.drain(); // all shards are going to be published from scratch
        pastPublications.clear();

        shardSizeStatsClient.getAllShardSizes(boostWindowInterval, new ActionListener<>() {
            @Override
            public void onResponse(Map<ShardId, ShardSize> allShardSizes) {
                logger.debug("Publishing all shard sized {}", allShardSizes);
                shardSizesPublisher.publishSearchShardDiskUsage(nodeId, allShardSizes, new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        pastPublications.putAll(allShardSizes);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.log(getExceptionLogLevel(e), () -> "Failed to publish all nodes shard sizes", e);
                        pendingPublication.retry(allShardSizes);
                    }
                });
                publishDiffNow();
            }

            @Override
            public void onFailure(Exception e) {
                logger.debug(() -> "Failed to detect all shard sizes", e);
            }
        });
    }

    private void scheduleFutureDiffPublication() {
        threadPool.scheduleUnlessShuttingDown(publishInterval, EsExecutors.DIRECT_EXECUTOR_SERVICE, this::publishDiffNow);
    }

    private void publishDiffNow() {
        if (lifecycle.started() && publishTaskQueueLength.getAndIncrement() == 0L) {
            executor.execute(publishTask);
        }
    }

    /**
     * Task to publish the shard sizes - publishTaskQueueLength ensures there's only ever at most one of these running at once.
     */
    private void doPublishShardSizes() {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);

        // Get number of tasks to process before doing the publication - publishTask is idempotent, so we can process many items from
        // the queue with a single invocation.
        final var batchSize = publishTaskQueueLength.get();
        assert batchSize > 0L;

        if (nodeId == null) {
            completeTasks(batchSize);
            return;
        }

        var shards = pendingPublication.drain();
        logger.debug("Publishing shard sizes diff {}", shards);
        shardSizesPublisher.publishSearchShardDiskUsage(nodeId, shards, ActionListener.runAfter(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                pastPublications.putAll(shards);
            }

            @Override
            public void onFailure(Exception e) {
                logger.log(getExceptionLogLevel(e), () -> "Unable to publish nodes shard sizes", e);
                pendingPublication.retry(shards);
            }
        }, () -> completeTasks(batchSize)));
    }

    private void completeTasks(long batchSize) {
        if (0L < publishTaskQueueLength.addAndGet(-batchSize)) {
            executor.execute(publishTask);
        } else {
            scheduleFutureDiffPublication();
        }
    }

    // visible for testing
    ConcurrentMap<ShardId, ShardSize> getPastPublications() {
        return pastPublications;
    }
}
