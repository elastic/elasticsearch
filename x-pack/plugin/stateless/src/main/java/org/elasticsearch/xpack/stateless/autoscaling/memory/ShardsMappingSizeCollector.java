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

package co.elastic.elasticsearch.stateless.autoscaling.memory;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.AutoscalingMissedIndicesUpdateException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import static co.elastic.elasticsearch.stateless.autoscaling.AutoscalingDataTransmissionLogging.getExceptionLogLevel;

public class ShardsMappingSizeCollector implements ClusterStateListener, IndexEventListener {

    public static final Setting<TimeValue> PUBLISHING_FREQUENCY_SETTING = Setting.timeSetting(
        "serverless.autoscaling.memory_metrics.indices_mapping_size.publication.frequency",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<TimeValue> CUT_OFF_TIMEOUT_SETTING = Setting.timeSetting(
        "serverless.autoscaling.memory_metrics.indices_mapping_size.publication.cut_off_timeout",
        // Safe timeout value, all mappings will eventually be synced by the periodic task
        TimeValue.timeValueMinutes(2),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    public static final Setting<TimeValue> RETRY_INITIAL_DELAY_SETTING = Setting.timeSetting(
        "serverless.autoscaling.memory_metrics.indices_mapping_size.publication.retry_initial_delay",
        TimeValue.timeValueSeconds(5),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    private static final Logger logger = LogManager.getLogger(ShardsMappingSizeCollector.class);
    private static final org.apache.logging.log4j.Logger log4jLogger = org.apache.logging.log4j.LogManager.getLogger(
        ShardsMappingSizeCollector.class
    );
    private final boolean isIndexNode;
    private final IndicesService indicesService;
    private final HeapMemoryUsagePublisher publisher;
    private final ThreadPool threadPool;
    private final Executor executor;
    private final TimeValue publicationFrequency;
    private final TimeValue cutOffTimeout;
    private final TimeValue retryInitialDelay;
    private final AtomicLong seqNo = new AtomicLong();
    private volatile PublishTask publishTask;

    public ShardsMappingSizeCollector(
        final boolean isIndexNode,
        final IndicesService indicesService,
        final HeapMemoryUsagePublisher publisher,
        final ThreadPool threadPool,
        final Settings settings
    ) {
        this(
            isIndexNode,
            indicesService,
            publisher,
            threadPool,
            PUBLISHING_FREQUENCY_SETTING.get(settings),
            CUT_OFF_TIMEOUT_SETTING.get(settings),
            RETRY_INITIAL_DELAY_SETTING.get(settings)
        );
    }

    ShardsMappingSizeCollector(
        final boolean isIndexNode,
        final IndicesService indicesService,
        final HeapMemoryUsagePublisher publisher,
        final ThreadPool threadPool,
        final TimeValue publicationFrequency,
        final TimeValue cutOffTimeout,
        final TimeValue retryInitialDelay
    ) {
        this.isIndexNode = isIndexNode;
        this.indicesService = indicesService;
        this.publisher = publisher;
        this.threadPool = threadPool;
        this.executor = threadPool.generic();
        this.publicationFrequency = publicationFrequency;
        this.cutOffTimeout = cutOffTimeout;
        this.retryInitialDelay = retryInitialDelay;
    }

    public static ShardsMappingSizeCollector create(
        final boolean isIndexNode,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final HeapMemoryUsagePublisher publisher,
        final ThreadPool threadPool,
        final Settings settings
    ) {
        final ShardsMappingSizeCollector instance = new ShardsMappingSizeCollector(
            isIndexNode,
            indicesService,
            publisher,
            threadPool,
            settings
        );
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                instance.start();
            }

            @Override
            public void beforeStop() {
                instance.stop();
            }
        });
        clusterService.addListener(instance);
        return instance;
    }

    public void publishHeapUsage(HeapMemoryUsage heapUsage) {
        publishHeapUsage(heapUsage, cutOffTimeout, new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse.Empty empty) {}

            @Override
            public void onFailure(Exception e) {
                logger.log(getExceptionLogLevel(e), () -> "Unable to publish indices mapping size", e);
            }
        });
    }

    void publishHeapUsage(HeapMemoryUsage heapUsage, TimeValue cutOffTimeout, ActionListener<ActionResponse.Empty> actionListener) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);

        new RetryableAction<>(log4jLogger, threadPool, retryInitialDelay, cutOffTimeout, actionListener, threadPool.generic()) {

            @Override
            public void tryAction(ActionListener<ActionResponse.Empty> listener) {
                publisher.publishIndicesMappingSize(heapUsage, listener);
            }

            @Override
            public boolean shouldRetry(Exception e) {
                final var cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof AutoscalingMissedIndicesUpdateException) {
                    logger.trace("Retry publishing mapping sizes: " + heapUsage, e);
                    return true;
                }
                return false;
            }
        }.run();
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (isIndexNode == false) {
            return;
        }
        if (event.nodesDelta().masterNodeChanged()) {
            // new master does not have any mapping size estimation data
            updateMappingMetricsForAllIndices();
        }
        if (event.metadataChanged()) {
            // handle index metadata mapping updates
            for (final IndexService indexService : indicesService) {
                final Index index = indexService.index();
                final IndexMetadata oldIndexMetadata = event.previousState().metadata().index(index);
                final IndexMetadata newIndexMetadata = event.state().metadata().index(index);
                if (oldIndexMetadata != null
                    && newIndexMetadata != null
                    && ClusterChangedEvent.indexMetadataChanged(oldIndexMetadata, newIndexMetadata)) { // ignore all unrelated events
                    if (oldIndexMetadata.getMappingVersion() < newIndexMetadata.getMappingVersion()) {
                        updateMappingMetricsForIndex(index);
                    }
                }
            }
        }
    }

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        if (isIndexNode == false) {
            return;
        }
        updateMappingMetricsForShard(indexShard.shardId());
    }

    private void addShardMappingSizes(long indexMappingSizeInBytes, Iterable<IndexShard> shards, Map<ShardId, ShardMappingSize> map) {
        for (IndexShard shard : shards) {
            final var shardState = Assertions.ENABLED ? shard.state() : null;
            final var shardFieldStats = shard.getShardFieldStats();
            if (shardFieldStats == null) {
                assert shardState != IndexShardState.POST_RECOVERY && shardState != IndexShardState.STARTED
                    : "started or post_recovery shard must have shard_field_stats ready";
                continue;
            }
            final String nodeId = shard.routingEntry().currentNodeId();
            final ShardMappingSize shardMappingSize = new ShardMappingSize(
                indexMappingSizeInBytes,
                shardFieldStats.numSegments(),
                shardFieldStats.totalFields(),
                nodeId
            );
            map.put(shard.shardId(), shardMappingSize);
        }
    }

    void updateMappingMetricsForAllIndices() {
        final long publishSeqNo = seqNo.incrementAndGet(); // generate seq_no before capturing data
        final Map<ShardId, ShardMappingSize> shardMappingSizes = new HashMap<>();
        for (IndexService indexService : indicesService) {
            var nodeMappingStats = indexService.getNodeMappingStats();
            if (nodeMappingStats != null) {
                addShardMappingSizes(nodeMappingStats.getTotalEstimatedOverhead().getBytes(), indexService, shardMappingSizes);
            }
        }
        HeapMemoryUsage heapUsage = new HeapMemoryUsage(publishSeqNo, shardMappingSizes);
        threadPool.generic().execute(() -> publishHeapUsage(heapUsage));
    }

    void updateMappingMetricsForIndex(Index index) {
        final long publishSeqNo = seqNo.incrementAndGet(); // generate seq_no before capturing data
        final IndexService indexService = indicesService.indexService(index);
        if (indexService != null) {
            final var nodeMappingStats = indexService.getNodeMappingStats();
            if (nodeMappingStats != null) {
                Map<ShardId, ShardMappingSize> shardMappingSizes = new HashMap<>();
                addShardMappingSizes(nodeMappingStats.getTotalEstimatedOverhead().getBytes(), indexService, shardMappingSizes);
                HeapMemoryUsage heapUsage = new HeapMemoryUsage(publishSeqNo, shardMappingSizes);
                threadPool.generic().execute(() -> publishHeapUsage(heapUsage));
            }
        }
    }

    public void updateMappingMetricsForShard(ShardId shardId) {
        final long publishSeqNo = seqNo.incrementAndGet(); // generate seq_no before capturing data
        final IndexService indexService = indicesService.indexService(shardId.getIndex());
        if (indexService != null) {
            final IndexShard shard = indexService.getShardOrNull(shardId.id());
            final var nodeMappingStats = indexService.getNodeMappingStats();
            if (shard != null && nodeMappingStats != null) {
                Map<ShardId, ShardMappingSize> shardMappingSizes = new HashMap<>();
                addShardMappingSizes(nodeMappingStats.getTotalEstimatedOverhead().getBytes(), List.of(shard), shardMappingSizes);
                HeapMemoryUsage heapUsage = new HeapMemoryUsage(publishSeqNo, shardMappingSizes);
                threadPool.generic().execute(() -> publishHeapUsage(heapUsage));
            }
        }
    }

    void start() {
        if (isIndexNode == false) {
            return;
        }
        publishTask = new PublishTask();
        publishTask.scheduleNext();
    }

    void stop() {
        if (isIndexNode == false) {
            return;
        }
        publishTask = null;
    }

    // Run a periodic task at long intervals just as a safeguard in case individual updates get lost
    class PublishTask extends AbstractRunnable {

        @Override
        protected void doRun() {
            if (publishTask != PublishTask.this) {
                return;
            }
            updateMappingMetricsForAllIndices();
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("Unexpected error during publishing indices memory mapping size metric", e);
        }

        @Override
        public void onAfter() {
            scheduleNext();
        }

        private void scheduleNext() {
            threadPool.scheduleUnlessShuttingDown(publicationFrequency, executor, PublishTask.this);
        }
    }
}
