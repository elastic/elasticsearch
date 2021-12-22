/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.dataframe.steps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshAction;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsTask;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.util.Objects;

import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeWithHeadersAsync;

abstract class AbstractDataFrameAnalyticsStep implements DataFrameAnalyticsStep {

    private static final Logger logger = LogManager.getLogger(AbstractDataFrameAnalyticsStep.class);

    protected final NodeClient client;
    protected final DataFrameAnalyticsTask task;
    protected final DataFrameAnalyticsAuditor auditor;
    protected final DataFrameAnalyticsConfig config;

    AbstractDataFrameAnalyticsStep(
        NodeClient client,
        DataFrameAnalyticsTask task,
        DataFrameAnalyticsAuditor auditor,
        DataFrameAnalyticsConfig config
    ) {
        this.client = Objects.requireNonNull(client);
        this.task = Objects.requireNonNull(task);
        this.auditor = Objects.requireNonNull(auditor);
        this.config = Objects.requireNonNull(config);
    }

    protected boolean isTaskStopping() {
        return task.isStopping();
    }

    protected ParentTaskAssigningClient parentTaskClient() {
        return new ParentTaskAssigningClient(client, task.getParentTaskId());
    }

    protected TaskId getParentTaskId() {
        return task.getParentTaskId();
    }

    @Override
    public final void execute(ActionListener<StepResponse> listener) {
        logger.debug(() -> new ParameterizedMessage("[{}] Executing step [{}]", config.getId(), name()));
        if (task.isStopping() && shouldSkipIfTaskIsStopping()) {
            logger.debug(() -> new ParameterizedMessage("[{}] task is stopping before starting [{}] step", config.getId(), name()));
            listener.onResponse(new StepResponse(true));
            return;
        }
        doExecute(ActionListener.wrap(stepResponse -> {
            // We persist progress at the end of each step to ensure we do not have
            // to repeat the step in case the node goes down without getting a chance to persist progress.
            task.persistProgress(() -> listener.onResponse(stepResponse));
        }, listener::onFailure));
    }

    protected abstract void doExecute(ActionListener<StepResponse> listener);

    protected void refreshDestAsync(ActionListener<RefreshResponse> refreshListener) {
        ParentTaskAssigningClient parentTaskClient = parentTaskClient();
        executeWithHeadersAsync(
            config.getHeaders(),
            ML_ORIGIN,
            parentTaskClient,
            RefreshAction.INSTANCE,
            new RefreshRequest(config.getDest().getIndex()),
            refreshListener
        );
    }

    protected boolean shouldSkipIfTaskIsStopping() {
        return true;
    }
}
