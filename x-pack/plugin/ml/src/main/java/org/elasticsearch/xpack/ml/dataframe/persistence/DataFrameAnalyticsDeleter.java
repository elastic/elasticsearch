/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.delete.DeleteAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.query.IdsQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.AbstractBulkByScrollRequest;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.xpack.core.ml.MlConfigIndex;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.stats.Fields;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.dataframe.StoredProgress;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.utils.MlIndicesUtils;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public class DataFrameAnalyticsDeleter {

    private static final Logger logger = LogManager.getLogger(DataFrameAnalyticsDeleter.class);

    private final Client client;
    private final DataFrameAnalyticsAuditor auditor;

    public DataFrameAnalyticsDeleter(Client client, DataFrameAnalyticsAuditor auditor) {
        this.client = Objects.requireNonNull(client);
        this.auditor = Objects.requireNonNull(auditor);
    }

    public void deleteAllDocuments(DataFrameAnalyticsConfig config, TimeValue timeout, ActionListener<AcknowledgedResponse> listener) {
        final String id = config.getId();

        // Step 3. Delete the config
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
                deleteConfig(id, listener);
            },
            failure -> {
                logger.warn(new ParameterizedMessage("[{}] failed to remove stats", id), ExceptionsHelper.unwrapCause(failure));
                deleteConfig(id, listener);
            }
        );

        // Step 2. Delete job docs from stats index
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
                deleteStats(id, timeout, deleteStatsHandler);
            },
            listener::onFailure
        );

        // Step 1. Delete state
        deleteState(config, timeout, deleteStateHandler);
    }

    private void deleteConfig(String id, ActionListener<AcknowledgedResponse> listener) {
        DeleteRequest deleteRequest = new DeleteRequest(MlConfigIndex.indexName());
        deleteRequest.id(DataFrameAnalyticsConfig.documentId(id));
        deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteAction.INSTANCE, deleteRequest, ActionListener.wrap(
            deleteResponse -> {
                if (deleteResponse.getResult() == DocWriteResponse.Result.NOT_FOUND) {
                    listener.onFailure(ExceptionsHelper.missingDataFrameAnalytics(id));
                    return;
                }
                assert deleteResponse.getResult() == DocWriteResponse.Result.DELETED;
                logger.info("[{}] Deleted", id);
                auditor.info(id, Messages.DATA_FRAME_ANALYTICS_AUDIT_DELETED);
                listener.onResponse(AcknowledgedResponse.TRUE);
            },
            e -> {
                if (ExceptionsHelper.unwrapCause(e) instanceof IndexNotFoundException) {
                    listener.onFailure(ExceptionsHelper.missingDataFrameAnalytics(id));
                } else {
                    listener.onFailure(e);
                }
            }
        ));
    }

    private void deleteState(DataFrameAnalyticsConfig config, TimeValue timeout, ActionListener<BulkByScrollResponse> listener) {
        ActionListener<Boolean> deleteModelStateListener = ActionListener.wrap(
            r -> executeDeleteByQuery(
                AnomalyDetectorsIndex.jobStateIndexPattern(),
                QueryBuilders.idsQuery().addIds(StoredProgress.documentId(config.getId())),
                timeout,
                listener
            )
            , listener::onFailure
        );

        deleteModelState(config, timeout, 1, deleteModelStateListener);
    }

    private void deleteModelState(DataFrameAnalyticsConfig config, TimeValue timeout, int docNum, ActionListener<Boolean> listener) {
        if (config.getAnalysis().persistsState() == false) {
            listener.onResponse(true);
            return;
        }

        IdsQueryBuilder query = QueryBuilders.idsQuery().addIds(config.getAnalysis().getStateDocIdPrefix(config.getId()) + docNum);
        executeDeleteByQuery(
            AnomalyDetectorsIndex.jobStateIndexPattern(),
            query,
            timeout,
            ActionListener.wrap(
                response -> {
                    if (response.getDeleted() > 0) {
                        deleteModelState(config, timeout, docNum + 1, listener);
                        return;
                    }
                    listener.onResponse(true);
                },
                listener::onFailure
            )
        );
    }

    private void deleteStats(String jobId, TimeValue timeout, ActionListener<BulkByScrollResponse> listener) {
        executeDeleteByQuery(
            MlStatsIndex.indexPattern(),
            QueryBuilders.termQuery(Fields.JOB_ID.getPreferredName(), jobId),
            timeout,
            listener
        );
    }

    private void executeDeleteByQuery(String index, QueryBuilder query, TimeValue timeout,
                                      ActionListener<BulkByScrollResponse> listener) {
        DeleteByQueryRequest request = new DeleteByQueryRequest(index);
        request.setQuery(query);
        request.setIndicesOptions(MlIndicesUtils.addIgnoreUnavailable(IndicesOptions.lenientExpandOpen()));
        request.setSlices(AbstractBulkByScrollRequest.AUTO_SLICES);
        request.setAbortOnVersionConflict(false);
        request.setRefresh(true);
        request.setTimeout(timeout);
        executeAsyncWithOrigin(client, ML_ORIGIN, DeleteByQueryAction.INSTANCE, request, listener);
    }
}
