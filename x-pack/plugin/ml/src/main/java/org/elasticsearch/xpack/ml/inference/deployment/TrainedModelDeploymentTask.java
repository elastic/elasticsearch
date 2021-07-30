/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.TaskParams;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;

import java.util.Map;

public class TrainedModelDeploymentTask extends AllocatedPersistentTask implements StartTrainedModelDeploymentAction.TaskMatcher {

    private static final Logger logger = LogManager.getLogger(TrainedModelDeploymentTask.class);

    private final TaskParams params;
    private volatile DeploymentManager manager;

    public TrainedModelDeploymentTask(long id, String type, String action, TaskId parentTask, Map<String, String> headers,
                                      TaskParams taskParams) {
        super(id, type, action, MlTasks.TRAINED_MODEL_DEPLOYMENT_TASK_ID_PREFIX + taskParams.getModelId(), parentTask, headers);
        this.params = taskParams;
    }

    public String getModelId() {
        return params.getModelId();
    }

    public String getIndex() {
        return params.getIndex();
    }

    public long estimateMemoryUsageBytes() {
        return params.estimateMemoryUsageBytes();
    }

    public void stop(String reason) {
        logger.debug("[{}] Stopping due to reason [{}]", getModelId(), reason);

        assert manager != null : "manager should not be unset when stop is called";
        manager.stopDeployment(this);
        markAsCompleted();
    }

    public void setDeploymentManager(DeploymentManager manager) {
        this.manager = manager;
    }

    @Override
    protected void onCancelled() {
        String reason = getReasonCancelled();
        stop(reason);
    }

    public void infer(String input, TimeValue timeout, ActionListener<InferenceResults> listener) {
        manager.infer(this, input, timeout, listener);
    }
}
