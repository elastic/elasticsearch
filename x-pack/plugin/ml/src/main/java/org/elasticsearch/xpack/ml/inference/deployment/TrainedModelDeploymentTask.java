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
import org.elasticsearch.inference.InferenceResults;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.TaskParams;
import org.elasticsearch.xpack.core.ml.inference.TrainedModelPrefixStrings;
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
        super(id, type, action, MlTasks.trainedModelAssignmentTaskDescription(taskParams.getDeploymentId()), parentTask, headers);
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
            // track the model usage not the deployment Id
            licensedFeature.startTracking(licenseState, "model-" + params.getModelId());
        }
    }

    public void updateNumberOfAllocations(int numberOfAllocations) {
        params = new TaskParams(
            params.getModelId(),
            params.getDeploymentId(),
            params.getModelBytes(),
            numberOfAllocations,
            params.getThreadsPerAllocation(),
            params.getQueueCapacity(),
            params.getCacheSize().orElse(null),
            params.getPriority(),
            params.getPerDeploymentMemoryBytes(),
            params.getPerAllocationMemoryBytes()
        );
    }

    public String getModelId() {
        return params.getModelId();
    }

    public String getDeploymentId() {
        return params.getDeploymentId();
    }

    public long estimateMemoryUsageBytes() {
        return params.estimateMemoryUsageBytes();
    }

    public TaskParams getParams() {
        return params;
    }

    public void stop(String reason, boolean finishPendingWork, ActionListener<AcknowledgedResponse> listener) {

        if (finishPendingWork) {
            trainedModelAssignmentNodeService.gracefullyStopDeploymentAndNotify(this, reason, listener);
        } else {
            trainedModelAssignmentNodeService.stopDeploymentAndNotify(this, reason, listener);
        }
    }

    public void markAsStopped(String reason) {
        // stop tracking the model usage
        licensedFeature.stopTracking(licenseState, "model-" + params.getModelId());
        logger.debug("[{}] Stopping due to reason [{}]", getDeploymentId(), reason);
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
        logger.info("[{}] task cancelled due to reason [{}]", getDeploymentId(), reason);
        stop(
            reason,
            true,
            ActionListener.wrap(
                acknowledgedResponse -> {},
                e -> logger.error(() -> "[" + getDeploymentId() + "] error stopping the deployment after task cancellation", e)
            )
        );
    }

    public void infer(
        NlpInferenceInput input,
        InferenceConfigUpdate update,
        boolean skipQueue,
        TimeValue timeout,
        TrainedModelPrefixStrings.PrefixType prefixType,
        CancellableTask parentActionTask,
        boolean chunkResponse,
        ActionListener<InferenceResults> listener
    ) {
        if (inferenceConfigHolder.get() == null) {
            listener.onFailure(
                ExceptionsHelper.conflictStatusException("Trained model deployment [{}] is not initialized", params.getDeploymentId())
            );
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
        var updatedConfig = update.isEmpty() ? inferenceConfigHolder.get() : inferenceConfigHolder.get().apply(update);
        trainedModelAssignmentNodeService.infer(
            this,
            updatedConfig,
            input,
            skipQueue,
            timeout,
            prefixType,
            parentActionTask,
            chunkResponse,
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
