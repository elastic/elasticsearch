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
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.TaskParams;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.assignment.TrainedModelAssignmentNodeService;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class TrainedModelDeploymentTask extends CancellableTask implements StartTrainedModelDeploymentAction.TaskMatcher {

    private static final Logger logger = LogManager.getLogger(TrainedModelDeploymentTask.class);

    private volatile TaskParams params;
    private final TrainedModelAssignmentNodeService trainedModelAssignmentNodeService;
    private volatile boolean stopped;
    private volatile boolean failed;
    private final SetOnce<String> stoppedReasonHolder = new SetOnce<>();
    private final SetOnce<InferenceConfig> inferenceConfigHolder = new SetOnce<>();
    private final XPackLicenseState licenseState;
    private final LicensedFeature.Persistent licensedFeature;

    public TrainedModelDeploymentTask(
        long id,
        String type,
        String action,
        TaskId parentTask,
        Map<String, String> headers,
        TaskParams taskParams,
        TrainedModelAssignmentNodeService trainedModelAssignmentNodeService,
        XPackLicenseState licenseState,
        LicensedFeature.Persistent licensedFeature
    ) {
        super(id, type, action, MlTasks.trainedModelAssignmentTaskDescription(taskParams.getModelId()), parentTask, headers);
        this.params = Objects.requireNonNull(taskParams);
        this.trainedModelAssignmentNodeService = ExceptionsHelper.requireNonNull(
            trainedModelAssignmentNodeService,
            "trainedModelAssignmentNodeService"
        );
        this.licenseState = licenseState;
        this.licensedFeature = licensedFeature;
    }

    void init(InferenceConfig inferenceConfig) {
        if (this.inferenceConfigHolder.trySet(inferenceConfig)) {
            licensedFeature.startTracking(licenseState, "model-" + params.getModelId());
        }
    }

    public void updateNumberOfAllocations(int numberOfAllocations) {
        params = new TaskParams(
            params.getModelId(),
            params.getModelBytes(),
            numberOfAllocations,
            params.getThreadsPerAllocation(),
            params.getQueueCapacity(),
            params.getCacheSize().orElse(null)
        );
    }

    public String getModelId() {
        return params.getModelId();
    }

    public long estimateMemoryUsageBytes() {
        return params.estimateMemoryUsageBytes();
    }

    public TaskParams getParams() {
        return params;
    }

    public void stop(String reason, ActionListener<AcknowledgedResponse> listener) {
        trainedModelAssignmentNodeService.stopDeploymentAndNotify(this, reason, listener);
    }

    public void markAsStopped(String reason) {
        licensedFeature.stopTracking(licenseState, "model-" + params.getModelId());
        logger.debug("[{}] Stopping due to reason [{}]", getModelId(), reason);
        stoppedReasonHolder.trySet(reason);
        stopped = true;
    }

    public boolean isStopped() {
        return stopped;
    }

    public Optional<String> stoppedReason() {
        return Optional.ofNullable(stoppedReasonHolder.get());
    }

    @Override
    protected void onCancelled() {
        String reason = getReasonCancelled();
        logger.info("[{}] task cancelled due to reason [{}]", getModelId(), reason);
        stop(
            reason,
            ActionListener.wrap(
                acknowledgedResponse -> {},
                e -> logger.error(() -> "[" + getModelId() + "] error stopping the model after task cancellation", e)
            )
        );
    }

    public void infer(
        NlpInferenceInput input,
        InferenceConfigUpdate update,
        boolean skipQueue,
        TimeValue timeout,
        Task parentActionTask,
        ActionListener<InferenceResults> listener
    ) {
        if (inferenceConfigHolder.get() == null) {
            listener.onFailure(ExceptionsHelper.conflictStatusException("Trained model [{}] is not initialized", params.getModelId()));
            return;
        }
        if (update.isSupported(inferenceConfigHolder.get()) == false) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Trained model [{}] is configured for task [{}] but called with task [{}]",
                    RestStatus.FORBIDDEN,
                    params.getModelId(),
                    inferenceConfigHolder.get().getName(),
                    update.getName()
                )
            );
            return;
        }
        trainedModelAssignmentNodeService.infer(
            this,
            update.apply(inferenceConfigHolder.get()),
            input,
            skipQueue,
            timeout,
            parentActionTask,
            listener
        );
    }

    public Optional<ModelStats> modelStats() {
        return trainedModelAssignmentNodeService.modelStats(this);
    }

    public void clearCache(ActionListener<AcknowledgedResponse> listener) {
        trainedModelAssignmentNodeService.clearCache(this, listener);
    }

    public void setFailed(String reason) {
        failed = true;
        trainedModelAssignmentNodeService.failAssignment(this, reason);
    }

    public boolean isFailed() {
        return failed;
    }
}
