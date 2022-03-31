/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.deployment;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.license.LicensedFeature;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction.TaskParams;
import org.elasticsearch.xpack.core.ml.inference.results.InferenceResults;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfig;
import org.elasticsearch.xpack.core.ml.inference.trainedmodel.InferenceConfigUpdate;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.inference.allocation.TrainedModelAllocationNodeService;

import java.util.Map;
import java.util.Optional;

public class TrainedModelDeploymentTask extends CancellableTask implements StartTrainedModelDeploymentAction.TaskMatcher {

    private static final Logger logger = LogManager.getLogger(TrainedModelDeploymentTask.class);

    private final TaskParams params;
    private final TrainedModelAllocationNodeService trainedModelAllocationNodeService;
    private volatile boolean stopped;
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
        TrainedModelAllocationNodeService trainedModelAllocationNodeService,
        XPackLicenseState licenseState,
        LicensedFeature.Persistent licensedFeature
    ) {
        super(id, type, action, MlTasks.trainedModelAllocationTaskDescription(taskParams.getModelId()), parentTask, headers);
        this.params = taskParams;
        this.trainedModelAllocationNodeService = ExceptionsHelper.requireNonNull(
            trainedModelAllocationNodeService,
            "trainedModelAllocationNodeService"
        );
        this.licenseState = licenseState;
        this.licensedFeature = licensedFeature;
    }

    void init(InferenceConfig inferenceConfig) {
        if (this.inferenceConfigHolder.trySet(inferenceConfig)) {
            licensedFeature.startTracking(licenseState, "model-" + params.getModelId());
        }
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
        trainedModelAllocationNodeService.stopDeploymentAndNotify(this, reason, listener);
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
                e -> logger.error(new ParameterizedMessage("[{}] error stopping the model after task cancellation", getModelId()), e)
            )
        );
    }

    public void infer(Map<String, Object> doc, InferenceConfigUpdate update, TimeValue timeout, ActionListener<InferenceResults> listener) {
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
        trainedModelAllocationNodeService.infer(this, update.apply(inferenceConfigHolder.get()), doc, timeout, listener);
    }

    public Optional<ModelStats> modelStats() {
        return trainedModelAllocationNodeService.modelStats(this);
    }

    public void setFailed(String reason) {
        trainedModelAllocationNodeService.failAllocation(this, reason);
    }
}
