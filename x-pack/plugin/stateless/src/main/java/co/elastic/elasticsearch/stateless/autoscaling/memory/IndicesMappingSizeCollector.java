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
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.NodeMappingStats;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.AutoscalingMissedIndicesUpdateException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import static co.elastic.elasticsearch.stateless.autoscaling.AutoscalingDataTransmissionLogging.getExceptionLogLevel;

public class IndicesMappingSizeCollector implements ClusterStateListener, IndexEventListener {

    private static final int SENDING_PRIMARY_SHARD_ID = 0;
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
    private static final Logger logger = LogManager.getLogger(IndicesMappingSizeCollector.class);
    private static final org.apache.logging.log4j.Logger log4jLogger = org.apache.logging.log4j.LogManager.getLogger(
        IndicesMappingSizeCollector.class
    );
    private final boolean isIndexNode;
    private final IndicesService indicesService;
    private final IndicesMappingSizePublisher publisher;
    private final ThreadPool threadPool;
    private final Executor executor;
    private final TimeValue publicationFrequency;
    private final TimeValue cutOffTimeout;
    private final TimeValue retryInitialDelay;
    private final AtomicLong seqNo = new AtomicLong();
    private volatile PublishTask publishTask;

    public IndicesMappingSizeCollector(
        final boolean isIndexNode,
        final IndicesService indicesService,
        final IndicesMappingSizePublisher publisher,
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

    IndicesMappingSizeCollector(
        final boolean isIndexNode,
        final IndicesService indicesService,
        final IndicesMappingSizePublisher publisher,
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

    public static IndicesMappingSizeCollector create(
        final boolean isIndexNode,
        final ClusterService clusterService,
        final IndicesService indicesService,
        final IndicesMappingSizePublisher publisher,
        final ThreadPool threadPool,
        final Settings settings
    ) {
        final IndicesMappingSizeCollector instance = new IndicesMappingSizeCollector(
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

    public void publishIndicesMappingSize(Map<Index, IndexMappingSize> indicesMappingSize) {
        publishIndicesMappingSize(indicesMappingSize, cutOffTimeout, new ActionListener<>() {
            @Override
            public void onResponse(ActionResponse.Empty empty) {}

            @Override
            public void onFailure(Exception e) {
                logger.log(getExceptionLogLevel(e), () -> "Unable to publish indices mapping size", e);
            }
        });
    }

    void publishIndicesMappingSize(
        Map<Index, IndexMappingSize> indicesMappingSize,
        TimeValue cutOffTimeout,
        ActionListener<ActionResponse.Empty> actionListener
    ) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);

        final HeapMemoryUsage heapMemoryUsage = new HeapMemoryUsage(seqNo.incrementAndGet(), indicesMappingSize);

        new RetryableAction<>(log4jLogger, threadPool, retryInitialDelay, cutOffTimeout, actionListener, threadPool.generic()) {

            @Override
            public void tryAction(ActionListener<ActionResponse.Empty> listener) {
                publisher.publishIndicesMappingSize(heapMemoryUsage, listener);
            }

            @Override
            public boolean shouldRetry(Exception e) {
                final var cause = ExceptionsHelper.unwrapCause(e);
                if (cause instanceof AutoscalingMissedIndicesUpdateException) {
                    logger.trace("Retry publishing mapping sizes: " + heapMemoryUsage, e);
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
            updateMappingSizeMetricsForAllIndices();
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
                        updateMappingSizeMetrics(newIndexMetadata);
                    }
                }
            }
        }
    }

    @Override
    public void afterIndexShardStarted(final IndexShard indexShard) {
        if (isIndexNode == false) {
            return;
        }
        if (indexShard.shardId().id() != SENDING_PRIMARY_SHARD_ID) {
            return;
        }
        IndexService indexService = indicesService.indexServiceSafe(indexShard.shardId().getIndex());
        updateMappingSizeMetrics(indexService.getMetadata());
    }

    void updateMappingSizeMetricsForAllIndices() {
        Map<Index, IndexMappingSize> indexMappingSizeMap = new HashMap<>();
        for (IndexService indexService : indicesService) {
            var indexMetadata = indexService.getMetadata();
            var indexMappingSize = indexMappingSizeMetrics(indexMetadata);
            if (indexMappingSize != null) {
                indexMappingSizeMap.put(indexMetadata.getIndex(), indexMappingSize);
            }
        }
        threadPool.generic().execute(() -> publishIndicesMappingSize(indexMappingSizeMap));
    }

    void updateMappingSizeMetrics(IndexMetadata indexMetadata) {
        IndexMappingSize indexMappingSize = indexMappingSizeMetrics(indexMetadata);
        if (indexMappingSize != null) {
            Map<Index, IndexMappingSize> indexMappingSizeMap = Map.of(indexMetadata.getIndex(), indexMappingSize);
            threadPool.generic().execute(() -> publishIndicesMappingSize(indexMappingSizeMap));
        }
    }

    @Nullable
    IndexMappingSize indexMappingSizeMetrics(final IndexMetadata indexMetadata) {
        if (isIndexNode == false) {
            return null;
        }
        final Index index = indexMetadata.getIndex();
        final IndexService indexService = indicesService.indexServiceSafe(index);
        final IndexShard indexShard = indexService.getShardOrNull(SENDING_PRIMARY_SHARD_ID);
        if (indexShard == null) {
            return null;
        }
        final NodeMappingStats nodeMappingStats = indexService.getNodeMappingStats();
        if (nodeMappingStats != null) {
            final ByteSizeValue estimatedSizeInBytes = nodeMappingStats.getTotalEstimatedOverhead();
            final long newValue = estimatedSizeInBytes.getBytes();
            final String shardNodeId = indexShard.routingEntry().currentNodeId();
            assert shardNodeId != null : "Publishing index mapping size metrics only from allocated shard";
            return new IndexMappingSize(newValue, shardNodeId);
        }
        return null;
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
            updateMappingSizeMetricsForAllIndices();
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
