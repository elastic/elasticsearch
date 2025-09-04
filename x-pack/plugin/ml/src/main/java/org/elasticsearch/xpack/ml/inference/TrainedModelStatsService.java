/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.inference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.InvalidAliasNameException;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.ElasticsearchMappings;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

public class TrainedModelStatsService {

    private static final Logger logger = LogManager.getLogger(TrainedModelStatsService.class);
    private static final TimeValue PERSISTENCE_INTERVAL = TimeValue.timeValueSeconds(1);

    private static final String STATS_UPDATE_SCRIPT_TEMPLATE = """
        ctx._source.{0} += params.{0};
        ctx._source.{1} += params.{1};
        ctx._source.{2} += params.{2};
        ctx._source.{3} += params.{3};
        ctx._source.{4} = params.{4};""".indent(4);
    // Script to only update if stats have increased since last persistence
    private static final String STATS_UPDATE_SCRIPT = Messages.getMessage(
        STATS_UPDATE_SCRIPT_TEMPLATE,
        InferenceStats.MISSING_ALL_FIELDS_COUNT.getPreferredName(),
        InferenceStats.INFERENCE_COUNT.getPreferredName(),
        InferenceStats.FAILURE_COUNT.getPreferredName(),
        InferenceStats.CACHE_MISS_COUNT.getPreferredName(),
        InferenceStats.TIMESTAMP.getPreferredName()
    );
    private static final ToXContent.Params FOR_INTERNAL_STORAGE_PARAMS = new ToXContent.MapParams(
        Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true")
    );

    private final Map<String, InferenceStats> statsQueue;
    private final ResultsPersisterService resultsPersisterService;
    private final OriginSettingClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ThreadPool threadPool;
    private volatile Scheduler.Cancellable scheduledFuture;
    private volatile boolean stopped;
    private volatile ClusterState clusterState;

    public TrainedModelStatsService(
        ResultsPersisterService resultsPersisterService,
        OriginSettingClient client,
        IndexNameExpressionResolver indexNameExpressionResolver,
        ClusterService clusterService,
        ThreadPool threadPool
    ) {
        this.resultsPersisterService = resultsPersisterService;
        this.client = client;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.threadPool = threadPool;
        this.statsQueue = new ConcurrentHashMap<>();

        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStart() {
                start();
            }

            @Override
            public void beforeStop() {
                stop();
            }
        });
        clusterService.addListener(this::setClusterState);
    }

    // visible for testing
    void setClusterState(ClusterChangedEvent event) {
        clusterState = event.state();
    }

    /**
     * Queues the stats for storing.
     * @param stats The stats to store or increment
     * @param flush When `true`, this indicates that stats should be written as soon as possible.
     *              If `false`, stats are not persisted until the next periodic persistence action.
     */
    public void queueStats(InferenceStats stats, boolean flush) {
        if (stats.hasStats()) {
            statsQueue.compute(
                InferenceStats.docId(stats.getModelId(), stats.getNodeId()),
                (k, previousStats) -> previousStats == null
                    ? stats
                    : InferenceStats.accumulator(stats).merge(previousStats).currentStats(stats.getTimeStamp())
            );
        }
        if (flush) {
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME).execute(this::updateStats);
        }
    }

    void stop() {
        logger.debug("About to stop TrainedModelStatsService");
        stopped = true;
        statsQueue.clear();

        ThreadPool.Cancellable cancellable = this.scheduledFuture;
        if (cancellable != null) {
            cancellable.cancel();
        }
    }

    private boolean shouldStop() {
        return stopped || MlMetadata.getMlMetadata(clusterState).isResetMode() || MlMetadata.getMlMetadata(clusterState).isUpgradeMode();
    }

    void start() {
        logger.debug("About to start TrainedModelStatsService");
        stopped = false;
        scheduledFuture = threadPool.scheduleWithFixedDelay(
            this::updateStats,
            PERSISTENCE_INTERVAL,
            threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
        );
    }

    void updateStats() {
        if (clusterState == null || statsQueue.isEmpty() || stopped) {
            return;
        }

        boolean isInUpgradeMode = MlMetadata.getMlMetadata(clusterState).isUpgradeMode();
        if (isInUpgradeMode) {
            logger.debug("Model stats not persisted as ml upgrade mode is enabled");
            return;
        }

        if (MlMetadata.getMlMetadata(clusterState).isResetMode()) {
            logger.debug("Model stats not persisted as ml reset_mode is enabled");
            return;
        }

        if (verifyIndicesExistAndPrimaryShardsAreActive(clusterState, indexNameExpressionResolver) == false) {
            try {
                logger.debug("About to create the stats index as it does not exist yet");
                createStatsIndexIfNecessary();
            } catch (Exception e) {
                // This exception occurs if, for some reason, the `createStatsIndexAndAliasIfNecessary` fails due to
                // a concrete index of the alias name already existing. This error is recoverable eventually, but
                // should NOT cause us to lose statistics.
                if ((e instanceof InvalidAliasNameException) == false) {
                    logger.error("failure creating ml stats index for storing model stats", e);
                    return;
                }
            }
        }

        List<InferenceStats> stats = new ArrayList<>(statsQueue.size());
        // We want a copy as the underlying concurrent map could be changed while iterating
        // We don't want to accidentally grab updates twice
        Set<String> keys = new HashSet<>(statsQueue.keySet());
        for (String k : keys) {
            InferenceStats inferenceStats = statsQueue.remove(k);
            if (inferenceStats != null) {
                stats.add(inferenceStats);
            }
        }
        if (stats.isEmpty()) {
            return;
        }
        BulkRequest bulkRequest = new BulkRequest();
        stats.stream().map(TrainedModelStatsService::buildUpdateRequest).filter(Objects::nonNull).forEach(bulkRequest::add);
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        if (bulkRequest.requests().isEmpty()) {
            return;
        }
        if (shouldStop()) {
            return;
        }
        String jobPattern = stats.stream().map(InferenceStats::getModelId).collect(Collectors.joining(","));
        try {
            resultsPersisterService.bulkIndexWithRetry(bulkRequest, jobPattern, () -> shouldStop() == false, (msg) -> {});
        } catch (ElasticsearchException ex) {
            logger.warn(() -> "failed to store stats for [" + jobPattern + "]", ex);
        }
    }

    static boolean verifyIndicesExistAndPrimaryShardsAreActive(ClusterState clusterState, IndexNameExpressionResolver expressionResolver) {
        String[] indices = expressionResolver.concreteIndexNames(
            clusterState,
            IndicesOptions.LENIENT_EXPAND_OPEN_HIDDEN,
            MlStatsIndex.writeAlias()
        );
        // If there are no indices, we need to make sure we attempt to create it properly
        if (indices.length == 0) {
            return false;
        }
        for (String index : indices) {
            if (clusterState.metadata().getProject().hasIndex(index) == false) {
                return false;
            }
            IndexRoutingTable routingTable = clusterState.getRoutingTable().index(index);
            if (routingTable == null || routingTable.allPrimaryShardsActive() == false || routingTable.readyForSearch() == false) {
                return false;
            }
        }
        return true;
    }

    private void createStatsIndexIfNecessary() {
        final PlainActionFuture<Boolean> listener = new PlainActionFuture<>();
        MlStatsIndex.createStatsIndexAndAliasIfNecessary(
            client,
            clusterState,
            indexNameExpressionResolver,
            MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT,
            ActionListener.wrap(
                r -> ElasticsearchMappings.addDocMappingIfMissing(
                    MlStatsIndex.writeAlias(),
                    MlStatsIndex::wrappedMapping,
                    client,
                    clusterState,
                    MachineLearning.HARD_CODED_MACHINE_LEARNING_MASTER_NODE_TIMEOUT,
                    listener,
                    MlStatsIndex.STATS_INDEX_MAPPINGS_VERSION
                ),
                listener::onFailure
            )
        );
        listener.actionGet();
        logger.debug("Created stats index");
    }

    static UpdateRequest buildUpdateRequest(InferenceStats stats) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            Map<String, Object> params = new HashMap<>();
            params.put(InferenceStats.FAILURE_COUNT.getPreferredName(), stats.getFailureCount());
            params.put(InferenceStats.MISSING_ALL_FIELDS_COUNT.getPreferredName(), stats.getMissingAllFieldsCount());
            params.put(InferenceStats.TIMESTAMP.getPreferredName(), stats.getTimeStamp().toEpochMilli());
            params.put(InferenceStats.INFERENCE_COUNT.getPreferredName(), stats.getInferenceCount());
            params.put(InferenceStats.CACHE_MISS_COUNT.getPreferredName(), stats.getCacheMissCount());
            stats.toXContent(builder, FOR_INTERNAL_STORAGE_PARAMS);
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.upsert(builder)
                .index(MlStatsIndex.writeAlias())
                // Usually, there shouldn't be a conflict, but if there is, only around a single update should have happened
                // out of band. If there is MANY more than that, something strange is happening and it should fail.
                .retryOnConflict(3)
                .id(InferenceStats.docId(stats.getModelId(), stats.getNodeId()))
                .script(new Script(ScriptType.INLINE, "painless", STATS_UPDATE_SCRIPT, params))
                .setRequireAlias(true);
            return updateRequest;
        } catch (IOException ex) {
            logger.error(() -> format("[%s] [%s] failed to serialize stats for update.", stats.getModelId(), stats.getNodeId()), ex);
        }
        return null;
    }
}
