/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.steps;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.dataframe.extractor.DataFrameDataExtractorFactory;
import org.elasticsearch.xpack.ml.dataframe.process.AnalyticsProcessManager;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.util.Objects;

public class AnalysisStep extends AbstractDataFrameAnalyticsStep {

    private final AnalyticsProcessManager processManager;

    public AnalysisStep(NodeClient client, DataFrameAnalyticsTask task, DataFrameAnalyticsAuditor auditor, DataFrameAnalyticsConfig config,
                        AnalyticsProcessManager processManager) {
        super(client, task, auditor, config);
        this.processManager = Objects.requireNonNull(processManager);
    }

    @Override
    public Name name() {
        return Name.ANALYSIS;
    }

    @Override
    public void cancel(String reason, TimeValue timeout) {
        processManager.stop(task);
    }

    @Override
    public void updateProgress(ActionListener<Void> listener) {
        // Progress for the analysis step gets handled by the c++ process reporting it and the
        // results processor parsing the value in memory.
        listener.onResponse(null);
    }

    @Override
    protected void doExecute(ActionListener<StepResponse> listener) {
        task.getStatsHolder().getDataCountsTracker().reset();

        final ParentTaskAssigningClient parentTaskClient = parentTaskClient();
        // Update state to ANALYZING and start process
        ActionListener<DataFrameDataExtractorFactory> dataExtractorFactoryListener = ActionListener.wrap(
            dataExtractorFactory -> processManager.runJob(task, config, dataExtractorFactory, listener),
            listener::onFailure
        );

        ActionListener<RefreshResponse> refreshListener = ActionListener.wrap(
            refreshResponse -> {
                // TODO This could fail with errors. In that case we get stuck with the copied index.
                // We could delete the index in case of failure or we could try building the factory before reindexing
                // to catch the error early on.
                DataFrameDataExtractorFactory.createForDestinationIndex(parentTaskClient, config, dataExtractorFactoryListener);
            },
            dataExtractorFactoryListener::onFailure
        );

        // First we need to refresh the dest index to ensure data is searchable in case the job
        // was stopped after reindexing was complete but before the index was refreshed.
        refreshDestAsync(refreshListener);
    }
}
