/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.inference;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceStats;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.utils.persistence.ResultsPersisterService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;


public class TrainedModelStatsService {

    private static final Logger logger = LogManager.getLogger(TrainedModelStatsService.class);
    private static final TimeValue PERSISTENCE_INTERVAL = TimeValue.timeValueSeconds(1);

    private static final String STATS_UPDATE_SCRIPT_TEMPLATE = "" +
        "    ctx._source.{0} += params.{0};\n" +
        "    ctx._source.{1} += params.{1};\n" +
        "    ctx._source.{2} += params.{2};\n" +
        "    ctx._source.{3} = params.{3};";
    // Script to only update if stats have increased since last persistence
    private static final String STATS_UPDATE_SCRIPT = Messages.getMessage(STATS_UPDATE_SCRIPT_TEMPLATE,
        InferenceStats.MISSING_ALL_FIELDS_COUNT.getPreferredName(),
        InferenceStats.INFERENCE_COUNT.getPreferredName(),
        InferenceStats.FAILURE_COUNT.getPreferredName(),
        InferenceStats.TIMESTAMP.getPreferredName());
    private static final ToXContent.Params FOR_INTERNAL_STORAGE_PARAMS =
        new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"));

    private final Map<String, InferenceStats> statsQueue;
    private final ResultsPersisterService resultsPersisterService;
    private final OriginSettingClient client;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final ThreadPool threadPool;
    private volatile Scheduler.Cancellable scheduledFuture;
    private volatile boolean verifiedStatsIndexCreated;
    private volatile boolean stopped;
    private volatile ClusterState clusterState;

    public TrainedModelStatsService(ResultsPersisterService resultsPersisterService,
                                    OriginSettingClient client,
                                    IndexNameExpressionResolver indexNameExpressionResolver,
                                    ClusterService clusterService,
                                    ThreadPool threadPool) {
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
        clusterService.addListener((event) -> this.clusterState = event.state());
    }

    public void queueStats(InferenceStats stats) {
        statsQueue.compute(InferenceStats.docId(stats.getModelId(), stats.getNodeId()),
            (k, previousStats) -> previousStats == null ?
                stats :
                InferenceStats.accumulator(stats).merge(previousStats).currentStats(stats.getTimeStamp()));
    }

    void stop() {
        stopped = true;
        statsQueue.clear();

        ThreadPool.Cancellable cancellable = this.scheduledFuture;
        if (cancellable != null) {
            cancellable.cancel();
        }
    }

    void start() {
        stopped = false;
        scheduledFuture = threadPool.scheduleWithFixedDelay(this::updateStats,
            PERSISTENCE_INTERVAL,
            MachineLearning.UTILITY_THREAD_POOL_NAME);
    }

    void updateStats() {
        if (clusterState == null || statsQueue.isEmpty()) {
            return;
        }
        if (verifiedStatsIndexCreated == false) {
            try {
                PlainActionFuture<Boolean> listener = new PlainActionFuture<>();
                MlStatsIndex.createStatsIndexAndAliasIfNecessary(client, clusterState, indexNameExpressionResolver, listener);
                listener.actionGet();
                verifiedStatsIndexCreated = true;
            } catch (Exception e) {
                logger.error("failure creating ml stats index for storing model stats", e);
                return;
            }
        }

        List<InferenceStats> stats = new ArrayList<>(statsQueue.size());
        for(String k : statsQueue.keySet()) {
            InferenceStats inferenceStats = statsQueue.remove(k);
            if (inferenceStats != null && inferenceStats.hasStats()) {
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
        resultsPersisterService.bulkIndexWithRetry(bulkRequest,
            stats.stream().map(InferenceStats::getModelId).collect(Collectors.joining(",")),
            () -> stopped == false,
            (msg) -> {});
    }

    static UpdateRequest buildUpdateRequest(InferenceStats stats) {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            Map<String, Object> params = new HashMap<>();
            params.put(InferenceStats.FAILURE_COUNT.getPreferredName(), stats.getFailureCount());
            params.put(InferenceStats.MISSING_ALL_FIELDS_COUNT.getPreferredName(), stats.getMissingAllFieldsCount());
            params.put(InferenceStats.TIMESTAMP.getPreferredName(), stats.getTimeStamp().toEpochMilli());
            params.put(InferenceStats.INFERENCE_COUNT.getPreferredName(), stats.getInferenceCount());
            stats.toXContent(builder, FOR_INTERNAL_STORAGE_PARAMS);
            UpdateRequest updateRequest = new UpdateRequest();
            updateRequest.upsert(builder)
                .index(MlStatsIndex.writeAlias())
                .id(InferenceStats.docId(stats.getModelId(), stats.getNodeId()))
                .script(new Script(ScriptType.INLINE, "painless", STATS_UPDATE_SCRIPT, params));
            return updateRequest;
        } catch (IOException ex) {
            logger.error(
                () -> new ParameterizedMessage("[{}] [{}] failed to serialize stats for update.",
                    stats.getModelId(),
                    stats.getNodeId()),
                ex);
        }
        return null;
    }

}
