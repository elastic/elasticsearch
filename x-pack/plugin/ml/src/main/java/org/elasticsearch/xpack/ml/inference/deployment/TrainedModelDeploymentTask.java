/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.TaskParams;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.allocation.TrainedModelAllocationNodeService;

import java.util.Map;
import java.util.Optional;

public class TrainedModelDeploymentTask extends CancellableTask implements StartTrainedModelDeploymentAction.TaskMatcher {

    private static final Logger logger = LogManager.getLogger(TrainedModelDeploymentTask.class);

    private final TaskParams params;
    private final TrainedModelAllocationNodeService trainedModelAllocationNodeService;
    private volatile boolean stopped;
    private final SetOnce<String> stoppedReason = new SetOnce<>();

    public TrainedModelDeploymentTask(
        long id,
        String type,
        String action,
        TaskId parentTask,
        Map<String, String> headers,
        TaskParams taskParams,
        TrainedModelAllocationNodeService trainedModelAllocationNodeService
    ) {
        super(id, type, action, MlTasks.trainedModelDeploymentTaskId(taskParams.getModelId()), parentTask, headers);
        this.params = taskParams;
        this.trainedModelAllocationNodeService = ExceptionsHelper.requireNonNull(
            trainedModelAllocationNodeService,
            "trainedModelAllocationNodeService"
        );
    }

    public String getModelId() {
        return params.getModelId();
    }

    public long estimateMemoryUsageBytes() {
        return params.estimateMemoryUsageBytes();
    }

    public void stop(String reason) {
        logger.debug("[{}] Stopping due to reason [{}]", getModelId(), reason);
        stopped = true;
        stoppedReason.trySet(reason);
        trainedModelAllocationNodeService.stopDeploymentAndNotify(this, reason);
    }

    public void stopWithoutNotification(String reason) {
        logger.debug("[{}] Stopping due to reason [{}]", getModelId(), reason);
        stoppedReason.trySet(reason);
        stopped = true;
    }

    public boolean isStopped() {
        return stopped;
    }

    public Optional<String> stoppedReason() {
        return Optional.ofNullable(stoppedReason.get());
    }

    @Override
    protected void onCancelled() {
        String reason = getReasonCancelled();
        stop(reason);
    }

    public void infer(Map<String, Object> doc, TimeValue timeout, ActionListener<InferenceResults> listener) {
        trainedModelAllocationNodeService.infer(this, doc, timeout, listener);
    }
}
