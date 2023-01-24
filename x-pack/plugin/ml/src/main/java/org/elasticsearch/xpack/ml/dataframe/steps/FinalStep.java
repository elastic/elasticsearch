/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.steps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.ml.MlStatsIndex;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.core.ml.dataframe.stats.common.DataCounts;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.ml.utils.ToXContentParams;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

/**
 * The final step of a data frame analytics job.
 * Allows the job to perform finalizing tasks like refresh indices,
 * persist stats, etc.
 */
public class FinalStep extends AbstractDataFrameAnalyticsStep {

    private static final Logger LOGGER = LogManager.getLogger(FinalStep.class);

    public FinalStep(NodeClient client, DataFrameAnalyticsTask task, DataFrameAnalyticsAuditor auditor, DataFrameAnalyticsConfig config) {
        super(client, task, auditor, config);
    }

    @Override
    public Name name() {
        return Name.FINAL;
    }

    @Override
    protected void doExecute(ActionListener<StepResponse> listener) {

        ActionListener<RefreshResponse> refreshListener = ActionListener.wrap(
            refreshResponse -> listener.onResponse(new StepResponse(false)),
            listener::onFailure
        );

        ActionListener<IndexResponse> dataCountsIndexedListener = ActionListener.wrap(
            indexResponse -> refreshIndices(refreshListener),
            listener::onFailure
        );

        indexDataCounts(dataCountsIndexedListener);
    }

    private void indexDataCounts(ActionListener<IndexResponse> listener) {
        DataCounts dataCounts = task.getStatsHolder().getDataCountsTracker().report();
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            dataCounts.toXContent(
                builder,
                new ToXContent.MapParams(Collections.singletonMap(ToXContentParams.FOR_INTERNAL_STORAGE, "true"))
            );
            IndexRequest indexRequest = new IndexRequest(MlStatsIndex.writeAlias()).id(DataCounts.documentId(config.getId()))
                .setRequireAlias(true)
                .source(builder);
            executeAsyncWithOrigin(parentTaskClient(), ML_ORIGIN, IndexAction.INSTANCE, indexRequest, listener);
        } catch (IOException e) {
            listener.onFailure(ExceptionsHelper.serverError("[{}] Error persisting final data counts", e, config.getId()));
        }
    }

    private void refreshIndices(ActionListener<RefreshResponse> listener) {
        RefreshRequest refreshRequest = new RefreshRequest(
            AnomalyDetectorsIndex.jobStateIndexPattern(),
            MlStatsIndex.indexPattern(),
            config.getDest().getIndex()
        );
        refreshRequest.indicesOptions(IndicesOptions.lenientExpandOpen());

        LOGGER.debug(() -> format("[%s] Refreshing indices %s", config.getId(), Arrays.toString(refreshRequest.indices())));

        executeAsyncWithOrigin(parentTaskClient(), ML_ORIGIN, RefreshAction.INSTANCE, refreshRequest, listener);
    }

    @Override
    public void cancel(String reason, TimeValue timeout) {
        // Not cancellable
    }

    @Override
    public void updateProgress(ActionListener<Void> listener) {
        // No progress to update
        listener.onResponse(null);
    }

    @Override
    protected boolean shouldSkipIfTaskIsStopping() {
        return false;
    }
}
