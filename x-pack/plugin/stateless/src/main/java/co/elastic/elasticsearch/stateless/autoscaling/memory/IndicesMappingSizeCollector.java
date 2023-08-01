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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.NodeMappingStats;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class IndicesMappingSizeCollector implements ClusterStateListener, IndexEventListener {

    private static final int SENDING_PRIMARY_SHARD_ID = 0;
    public static final Setting<TimeValue> PUBLISHING_FREQUENCY_SETTING = Setting.timeSetting(
        "serverless.autoscaling.memory_metrics.indices_mapping_size.publication.frequency",
        TimeValue.timeValueSeconds(30),
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );
    private static final Logger logger = LogManager.getLogger(IndicesMappingSizeCollector.class);
    private final boolean isIndexNode;
    private final IndicesService indicesService;
    private final IndicesMappingSizePublisher publisher;
    private final ThreadPool threadPool;
    private final TimeValue publicationFrequency;
    private final Map<Index, IndexMappingSize> indexToMappingSizeMetrics = new ConcurrentHashMap<>();
    private final AtomicLong seqNo = new AtomicLong();
    private volatile PublishTask publishTask;

    public IndicesMappingSizeCollector(
        final boolean isIndexNode,
        final IndicesService indicesService,
        final IndicesMappingSizePublisher publisher,
        final ThreadPool threadPool,
        final Settings settings
    ) {
        this(isIndexNode, indicesService, publisher, threadPool, PUBLISHING_FREQUENCY_SETTING.get(settings));
    }

    IndicesMappingSizeCollector(
        final boolean isIndexNode,
        final IndicesService indicesService,
        final IndicesMappingSizePublisher publisher,
        final ThreadPool threadPool,
        final TimeValue publicationFrequency
    ) {
        this.isIndexNode = isIndexNode;
        this.indicesService = indicesService;
        this.publisher = publisher;
        this.threadPool = threadPool;
        this.publicationFrequency = publicationFrequency;
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

    public void publishIndicesMappingSize() {

        if (indexToMappingSizeMetrics.isEmpty()) {
            return;
        }

        final HeapMemoryUsage heapMemoryUsage = new HeapMemoryUsage(seqNo.incrementAndGet(), Map.copyOf(indexToMappingSizeMetrics));

        // TODO: (Optimization) Do not try to publish new metrics if previous sending has not completed yet
        publisher.publishIndicesMappingSize(heapMemoryUsage, new ActionListener<>() {
            @Override
            public void onResponse(final ActionResponse.Empty ignore) {}

            @Override
            public void onFailure(final Exception e) {
                logger.error("Error while pushing indices mapping size metrics to master node", e);
            }
        });
    }

    // Visible for testing
    Map<Index, IndexMappingSize> getIndexToMappingSizeMetrics() {
        return indexToMappingSizeMetrics;
    }

    @Override
    public void clusterChanged(final ClusterChangedEvent event) {
        if (isIndexNode == false) {
            return;
        }
        if (event.nodesDelta().masterNodeChanged()) {
            // new master does not have any mapping size estimation data
            // therefore each index node pushes it explicitly not waiting for the next scheduled publish task run
            threadPool.executor(ThreadPool.Names.GENERIC).execute(this::publishIndicesMappingSize);
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

    @Override
    public void beforeIndexShardClosed(final ShardId shardId, final IndexShard indexShard, final Settings indexSettings) {
        if (isIndexNode == false) {
            return;
        }
        if (shardId.id() != SENDING_PRIMARY_SHARD_ID) {
            return;
        }
        removeMappingSizeMetrics(shardId.getIndex());
    }

    private void updateMappingSizeMetrics(final IndexMetadata indexMetadata) {
        if (isIndexNode == false) {
            return;
        }
        final Index index = indexMetadata.getIndex();
        final IndexService indexService = indicesService.indexServiceSafe(index);
        final IndexShard indexShard = indexService.getShardOrNull(SENDING_PRIMARY_SHARD_ID);
        if (indexShard == null) {
            return;
        }
        updateMappingSizeMetrics(index, indexService, indexShard.routingEntry().currentNodeId());
    }

    private void updateMappingSizeMetrics(final Index index, final IndexService indexService, final String shardNodeId) {
        final NodeMappingStats nodeMappingStats = indexService.getNodeMappingStats();
        if (nodeMappingStats != null) {
            final ByteSizeValue estimatedSizeInBytes = nodeMappingStats.getTotalEstimatedOverhead();
            final long newValue = estimatedSizeInBytes.getBytes();
            assert shardNodeId != null : "Publishing index mapping size metrics only from allocated shard";
            indexToMappingSizeMetrics.put(index, new IndexMappingSize(newValue, shardNodeId));
        }
    }

    private void removeMappingSizeMetrics(final Index index) {
        indexToMappingSizeMetrics.remove(index);
    }

    void start() {
        if (isIndexNode == false) {
            return;
        }
        publishTask = new PublishTask();
        publishTask.run();
    }

    void stop() {
        if (isIndexNode == false) {
            return;
        }
        publishTask = null;
    }

    // TODO: send updates only if there is a change in metrics
    class PublishTask implements Runnable {
        @Override
        public void run() {
            if (publishTask != PublishTask.this) {
                // Estimator component is tearing down (publishTask == null)
                // do not schedule the next run
                return;
            }
            try {
                publishIndicesMappingSize();
            } catch (final RuntimeException e) {
                logger.error("Unexpected error during publishing indices memory mapping size metric", e);
            } finally {
                threadPool.scheduleUnlessShuttingDown(publicationFrequency, ThreadPool.Names.GENERIC, PublishTask.this);
            }
        }
    }

}
