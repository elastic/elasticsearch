/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.DeleteDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.dataframe.stats.Fields;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.dataframe.StoredProgress;
import org.elasticsearch.xpack.ml.dataframe.persistence.DataFrameAnalyticsConfigProvider;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.utils.MlIndicesUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * The action is a master node action to ensure it reads an up-to-date cluster
 * state in order to determine whether there is a persistent task for the analytics
 * to delete.
 */
public class TransportDeleteDataFrameAnalyticsAction
    extends TransportMasterNodeAction<DeleteDataFrameAnalyticsAction.Request, AcknowledgedResponse> {

    private static final Logger logger = LogManager.getLogger(TransportDeleteDataFrameAnalyticsAction.class);

    private final Client client;
    private final MlMemoryTracker memoryTracker;
    private final DataFrameAnalyticsConfigProvider configProvider;
    private final DataFrameAnalyticsAuditor auditor;

    @Inject
    public TransportDeleteDataFrameAnalyticsAction(TransportService transportService, ClusterService clusterService,
                                                   ThreadPool threadPool, ActionFilters actionFilters,
                                                   IndexNameExpressionResolver indexNameExpressionResolver, Client client,
                                                   MlMemoryTracker memoryTracker, DataFrameAnalyticsConfigProvider configProvider,
                                                   DataFrameAnalyticsAuditor auditor) {
        super(DeleteDataFrameAnalyticsAction.NAME, transportService, clusterService, threadPool, actionFilters,
            DeleteDataFrameAnalyticsAction.Request::new, indexNameExpressionResolver);
        this.client = client;
        this.memoryTracker = memoryTracker;
        this.configProvider = configProvider;
        this.auditor = Objects.requireNonNull(auditor);
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse read(StreamInput in) throws IOException {
        return new AcknowledgedResponse(in);
    }

    @Override
    protected void masterOperation(Task task, DeleteDataFrameAnalyticsAction.Request request, ClusterState state,
                                   ActionListener<AcknowledgedResponse> listener) {
        String id = request.getId();
        TaskId taskId = new TaskId(clusterService.localNode().getId(), task.getId());
        ParentTaskAssigningClient parentTaskClient = new ParentTaskAssigningClient(client, taskId);

        if (request.isForce()) {
            forceDelete(parentTaskClient, id, listener);
        } else {
            normalDelete(parentTaskClient, state, id, listener);
        }
    }

    private void forceDelete(ParentTaskAssigningClient parentTaskClient, String id,
                             ActionListener<AcknowledgedResponse> listener) {
        logger.debug("[{}] Force deleting data frame analytics job", id);

        ActionListener<StopDataFrameAnalyticsAction.Response> stopListener = ActionListener.wrap(
            stopResponse -> normalDelete(parentTaskClient, clusterService.state(), id, listener),
            listener::onFailure
        );

        StopDataFrameAnalyticsAction.Request request = new StopDataFrameAnalyticsAction.Request(id);
        request.setForce(true);
        executeAsyncWithOrigin(parentTaskClient, ML_ORIGIN, StopDataFrameAnalyticsAction.INSTANCE, request, stopListener);
    }

    private void normalDelete(ParentTaskAssigningClient parentTaskClient, ClusterState state, String id,
                              ActionListener<AcknowledgedResponse> listener) {
        PersistentTasksCustomMetadata tasks = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        DataFrameAnalyticsState taskState = MlTasks.getDataFrameAnalyticsState(id, tasks);
        if (taskState != DataFrameAnalyticsState.STOPPED) {
            listener.onFailure(ExceptionsHelper.conflictStatusException("Cannot delete data frame analytics [{}] while its status is [{}]",
                id, taskState));
            return;
        }

        // We clean up the memory tracker on delete because there is no stop; the task stops by itself
        memoryTracker.removeDataFrameAnalyticsJob(id);

        // Step 4. Delete the config
        ActionListener<BulkByScrollResponse> deleteStatsHandler = ActionListener.wrap(
            bulkByScrollResponse -> {
                if (bulkByScrollResponse.isTimedOut()) {
                    logger.warn("[{}] DeleteByQuery for stats timed out", id);
                }
                if (bulkByScrollResponse.getBulkFailures().isEmpty() == false) {
                    logger.warn("[{}] {} failures and {} conflicts encountered while running DeleteByQuery for stats", id,
                        bulkByScrollResponse.getBulkFailures().size(), bulkByScrollResponse.getVersionConflicts());
                    for (BulkItemResponse.Failure failure : bulkByScrollResponse.getBulkFailures()) {
                        logger.warn("[{}] DBQ failure: {}", id, failure);
                    }
                }
                deleteConfig(parentTaskClient, id, listener);
            },
            listener::onFailure
        );

        // Step 3. Delete job docs from stats index
        ActionListener<BulkByScrollResponse> deleteStateHandler = ActionListener.wrap(
            bulkByScrollResponse -> {
                if (bulkByScrollResponse.isTimedOut()) {
                    logger.warn("[{}] DeleteByQuery for state timed out", id);
                }
                if (bulkByScrollResponse.getBulkFailures().isEmpty() == false) {
                    logger.warn("[{}] {} failures and {} conflicts encountered while running DeleteByQuery for state", id,
                        bulkByScrollResponse.getBulkFailures().size(), bulkByScrollResponse.getVersionConflicts());
                    for (BulkItemResponse.Failure failure : bulkByScrollResponse.getBulkFailures()) {
                        logger.warn("[{}] DBQ failure: {}", id, failure);
                    }
                }
                deleteStats(parentTaskClient, id, deleteStatsHandler);
            },
            listener::onFailure
        );

        // Step 2. Delete state
        ActionListener<DataFrameAnalyticsConfig> configListener = ActionListener.wrap(
            config -> deleteState(parentTaskClient, config, deleteStateHandler),
            listener::onFailure
        );

        // Step 1. Get the config to check if it exists
        configProvider.get(id, configListener);
    }

    private void deleteConfig(ParentTaskAssigningClient parentTaskClient, String id, ActionListener<AcknowledgedResponse> listener) {
        DeleteRequest deleteRequest = new DeleteRequest(AnomalyDetectorsIndex.configIndexName());
        deleteRequest.id(DataFrameAnalyticsConfig.documentId(id));
        deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        executeAsyncWithOrigin(parentTaskClient, ML_ORIGIN, DeleteAction.INSTANCE, deleteRequest, ActionListener.wrap(
            deleteResponse -> {
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    listener.onFailure(ExceptionsHelper.missingDataFrameAnalytics(id));
                    return;
                }
                assert deleteResponse.getResult() == DocWriteResponse.Result.DELETED;
                logger.info("[{}] Deleted", id);
                auditor.info(id, Messages.DATA_FRAME_ANALYTICS_AUDIT_DELETED);
                listener.onResponse(new AcknowledgedResponse(true));
            },
            listener::onFailure
        ));
    }

    private void deleteState(ParentTaskAssigningClient parentTaskClient,
                             DataFrameAnalyticsConfig config,
                             ActionListener<BulkByScrollResponse> listener) {
        List<String> ids = new ArrayList<>();
        ids.add(StoredProgress.documentId(config.getId()));
        if (config.getAnalysis().persistsState()) {
            ids.add(config.getAnalysis().getStateDocId(config.getId()));
        }
        executeDeleteByQuery(
            parentTaskClient,
            AnomalyDetectorsIndex.jobStateIndexPattern(),
            QueryBuilders.idsQuery().addIds(ids.toArray(String[]::new)),
            listener
        );
    }

    private void deleteStats(ParentTaskAssigningClient parentTaskClient,
                             String jobId,
                             ActionListener<BulkByScrollResponse> listener) {
        executeDeleteByQuery(
            parentTaskClient,
            MlStatsIndex.indexPattern(),
            QueryBuilders.termQuery(Fields.JOB_ID.getPreferredName(), jobId),
            listener
        );
    }

    private void executeDeleteByQuery(ParentTaskAssigningClient parentTaskClient, String index, QueryBuilder query,
                                      ActionListener<BulkByScrollResponse> listener) {
        DeleteByQueryRequest request = new DeleteByQueryRequest(index);
        request.setQuery(query);
        request.setIndicesOptions(MlIndicesUtils.addIgnoreUnavailable(IndicesOptions.lenientExpandOpen()));
        request.setSlices(AbstractBulkByScrollRequest.AUTO_SLICES);
        request.setAbortOnVersionConflict(false);
        request.setRefresh(true);
        executeAsyncWithOrigin(parentTaskClient, ML_ORIGIN, DeleteByQueryAction.INSTANCE, request, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(DeleteDataFrameAnalyticsAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }
}
