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

import co.elastic.elasticsearch.stateless.lucene.IndexDirectory;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSize;
import co.elastic.elasticsearch.stateless.lucene.stats.ShardSizeStatsReader;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 * This service is responsible for collecting shard size changes on the search nodes
 * and periodically sending updates to the elected master
 */
public class ShardSizesCollector implements ClusterStateListener {

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

    private static final Logger logger = LogManager.getLogger(IndexDirectory.class);

    private final ThreadPool threadPool;
    private final ShardSizeStatsReader shardSizeStatsReader;
    private final ShardSizesPublisher shardSizesPublisher;
    private final Supplier<String> nodeIdSupplier;
    private final boolean isSearchNode;

    private volatile TimeValue publishInterval = TimeValue.timeValueSeconds(1);
    private volatile ByteSizeValue significantSizeChangeThreshold = ByteSizeValue.ofMb(1);

    private volatile PublishTask publishTask;

    private final PendingPublication pendingPublication = new PendingPublication();
    private final ConcurrentMap<ShardId, ShardSize> pastPublications = new ConcurrentHashMap<>();

    private class PendingPublication {
        private Map<ShardId, ShardSize> shards = new HashMap<>();
        private long interactiveSizeDiffInBytes = 0L;

        private synchronized boolean add(ShardId shardId, ShardSize size) {
            var previousSize = pastPublications.getOrDefault(shardId, ShardSize.EMPTY);
            long delta = size.interactiveSizeInBytes() - previousSize.interactiveSizeInBytes();
            if (previousSize == ShardSize.EMPTY || delta > 0) {
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

    public static ShardSizesCollector create(
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        ClusterService clusterService,
        ShardSizeStatsReader shardSizeStatsReader,
        ShardSizesPublisher shardSizesPublisher,
        boolean isSearchNode
    ) {
        var collector = new ShardSizesCollector(
            clusterSettings,
            threadPool,
            shardSizeStatsReader,
            shardSizesPublisher,
            () -> clusterService.state().nodes().getLocalNodeId(),
            isSearchNode
        );
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                collector.doStart();
            }

            @Override
            public void beforeStop() {
                collector.doStop();
            }
        });
        clusterService.addListener(collector);
        return collector;
    }

    public ShardSizesCollector(
        ClusterSettings clusterSettings,
        ThreadPool threadPool,
        ShardSizeStatsReader shardSizeStatsReader,
        ShardSizesPublisher shardSizesPublisher,
        Supplier<String> nodeIdSupplier,
        boolean isSearchNode
    ) {
        this.threadPool = threadPool;
        this.shardSizeStatsReader = shardSizeStatsReader;
        this.shardSizesPublisher = shardSizesPublisher;
        this.nodeIdSupplier = nodeIdSupplier;
        this.isSearchNode = isSearchNode;
        clusterSettings.initializeAndWatch(PUSH_INTERVAL_SETTING, value -> this.publishInterval = value);
        clusterSettings.initializeAndWatch(PUSH_DELTA_THRESHOLD_SETTING, value -> this.significantSizeChangeThreshold = value);
    }

    protected void doStart() {
        if (isSearchNode) {
            scheduleNextDiffPublication();
        }
    }

    protected void doStop() {
        publishTask = null;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (isSearchNode == false) {
            return;
        }
        if (event.nodesDelta().masterNodeChanged()) {
            threadPool.executor(ThreadPool.Names.GENERIC).submit(this::publishAllNow);
        }
        if (event.metadataChanged()) {
            var metadata = event.state().metadata();
            pastPublications.keySet().removeIf(shardId -> metadata.hasIndex(shardId.getIndex()) == false);
        }
        if (event.routingTableChanged()) {
            var routingNode = event.state().getRoutingNodes().node(nodeIdSupplier.get());
            var localShards = new HashSet<ShardId>();
            for (ShardRouting shardRouting : routingNode) {
                localShards.add(shardRouting.shardId());
            }
            pastPublications.keySet().retainAll(localShards);
        }
    }

    public void detectShardSize(ShardId shardId) {
        assert isSearchNode : "Should be executed only on search nodes";
        var shardSize = shardSizeStatsReader.getShardSize(shardId);
        if (shardSize != null && pendingPublication.add(shardId, shardSize)) {
            publishDiffNow();
        }
    }

    private void publishAllNow() {
        pendingPublication.drain(); // all shards are going to be published from scratch
        pastPublications.clear();

        var allShardSizes = shardSizeStatsReader.getAllShardSizes();
        shardSizesPublisher.publishSearchShardDiskUsage(allShardSizes, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                pastPublications.putAll(allShardSizes);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("Failed to publish all nodes shard sizes", e);
                pendingPublication.retry(allShardSizes);
            }
        });
        scheduleNextDiffPublication();
    }

    private void publishDiffNow() {
        PublishTask newPublishTask = new PublishTask();
        publishTask = newPublishTask;
        newPublishTask.run();
    }

    private void scheduleNextDiffPublication() {
        PublishTask newPublishTask = new PublishTask();
        publishTask = newPublishTask;
        newPublishTask.scheduleNext();
    }

    class PublishTask extends AbstractRunnable {

        @Override
        protected void doRun() {
            if (publishTask != PublishTask.this) {
                return;
            }
            var shards = pendingPublication.drain();
            shardSizesPublisher.publishSearchShardDiskUsage(shards, new ActionListener<>() {
                @Override
                public void onResponse(Void unused) {
                    pastPublications.putAll(shards);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("Failed to publish nodes shard sizes", e);
                    pendingPublication.retry(shards);
                }
            });
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("Failed to push shard sizes to elected master", e);
        }

        @Override
        public void onAfter() {
            scheduleNext();
        }

        private void scheduleNext() {
            threadPool.scheduleUnlessShuttingDown(publishInterval, ThreadPool.Names.GENERIC, PublishTask.this);
        }
    }

    // visible for testing
    ConcurrentMap<ShardId, ShardSize> getPastPublications() {
        return pastPublications;
    }
}
