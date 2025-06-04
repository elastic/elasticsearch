/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarAction;
import org.elasticsearch.xpack.core.ml.action.DeleteCalendarEventAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.DeleteExpiredDataAction;
import org.elasticsearch.xpack.core.ml.action.DeleteFilterAction;
import org.elasticsearch.xpack.core.ml.action.DeleteForecastAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.DeleteTrainedModelAliasAction;
import org.elasticsearch.xpack.core.ml.action.ExplainDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.FinalizeJobExecutionAction;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.action.ForecastJobAction;
import org.elasticsearch.xpack.core.ml.action.KillProcessAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PersistJobAction;
import org.elasticsearch.xpack.core.ml.action.PostCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.core.ml.action.PutCalendarAction;
import org.elasticsearch.xpack.core.ml.action.PutDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutFilterAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelAliasAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelDefinitionPartAction;
import org.elasticsearch.xpack.core.ml.action.PutTrainedModelVocabularyAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StartTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateCalendarJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateFilterAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.UpdateProcessAction;
import org.elasticsearch.xpack.core.ml.action.UpgradeJobModelSnapshotAction;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link MlUpgradeModeActionFilter} disallows certain actions if the cluster is currently in upgrade mode.
 *
 * Disallowed actions are the ones which can access/alter the state of ML internal indices.
 *
 * There is a complication that the feature reset API knows nothing about ML upgrade mode.  If the feature
 * reset API is called while ML upgrade mode is enabled then it takes precedence and resets the ML state.
 * This means that all ML entities will be deleted and upgrade mode will be disabled if the reset completes
 * successfully.
 */
class MlUpgradeModeActionFilter extends ActionFilter.Simple {

    private static final Set<String> ACTIONS_DISALLOWED_IN_UPGRADE_MODE = Set.of(
        PutJobAction.NAME,
        UpdateJobAction.NAME,
        DeleteJobAction.NAME,
        OpenJobAction.NAME,
        FlushJobAction.NAME,
        CloseJobAction.NAME,
        PersistJobAction.NAME,

        FinalizeJobExecutionAction.NAME,
        PostDataAction.NAME,

        RevertModelSnapshotAction.NAME,
        UpdateModelSnapshotAction.NAME,
        DeleteModelSnapshotAction.NAME,
        UpgradeJobModelSnapshotAction.NAME,

        PutDatafeedAction.NAME,
        UpdateDatafeedAction.NAME,
        DeleteDatafeedAction.NAME,
        StartDatafeedAction.NAME,
        StopDatafeedAction.NAME,

        PutFilterAction.NAME,
        UpdateFilterAction.NAME,
        DeleteFilterAction.NAME,

        PutCalendarAction.NAME,
        UpdateCalendarJobAction.NAME,
        PostCalendarEventsAction.NAME,
        DeleteCalendarAction.NAME,
        DeleteCalendarEventAction.NAME,

        UpdateProcessAction.NAME,
        KillProcessAction.NAME,

        DeleteExpiredDataAction.NAME,

        ForecastJobAction.NAME,
        DeleteForecastAction.NAME,

        PutDataFrameAnalyticsAction.NAME,
        DeleteDataFrameAnalyticsAction.NAME,
        StartDataFrameAnalyticsAction.NAME,
        StopDataFrameAnalyticsAction.NAME,
        ExplainDataFrameAnalyticsAction.NAME,

        PutTrainedModelAliasAction.NAME,
        PutTrainedModelAction.NAME,
        PutTrainedModelDefinitionPartAction.NAME,
        PutTrainedModelVocabularyAction.NAME,
        // NOTE: StopTrainedModelDeploymentAction doesn't mutate internal indices, and technically neither does this action.
        // But, preventing new deployments from being created while upgrading is for safety.
        StartTrainedModelDeploymentAction.NAME,
        DeleteTrainedModelAction.NAME,
        DeleteTrainedModelAliasAction.NAME
    );

    private static final Set<String> RESET_MODE_EXEMPTIONS = Set.of(
        DeleteJobAction.NAME,
        CloseJobAction.NAME,

        DeleteDatafeedAction.NAME,
        StopDatafeedAction.NAME,

        KillProcessAction.NAME,

        DeleteDataFrameAnalyticsAction.NAME,
        StopDataFrameAnalyticsAction.NAME,

        // No other trained model APIs need to be exempted as `StopTrainedModelDeploymentAction` isn't filtered during upgrade mode
        DeleteTrainedModelAction.NAME
    );

    // At the time the action filter is installed no cluster state is available, so
    // initialise to false/false and let the first change event set the real values
    private final AtomicReference<UpgradeResetFlags> upgradeResetFlags = new AtomicReference<>(new UpgradeResetFlags(false, false));

    MlUpgradeModeActionFilter(ClusterService clusterService) {
        Objects.requireNonNull(clusterService);
        clusterService.addListener(this::setUpgradeResetFlags);
    }

    @Override
    protected boolean apply(String action, ActionRequest request, ActionListener<?> listener) {
        // Ensure the same object is used for both tests
        UpgradeResetFlags localUpgradeResetFlags = upgradeResetFlags.get();
        assert localUpgradeResetFlags != null;
        // If we are in upgrade mode but a reset is being done then allow the destructive actions that reset mode uses
        if (localUpgradeResetFlags.isResetMode && RESET_MODE_EXEMPTIONS.contains(action)) {
            return true;
        }
        if (localUpgradeResetFlags.isUpgradeMode && ACTIONS_DISALLOWED_IN_UPGRADE_MODE.contains(action)) {
            throw new ElasticsearchStatusException(
                "Cannot perform {} action while upgrade mode is enabled",
                RestStatus.TOO_MANY_REQUESTS,
                action
            );
        }
        return true;
    }

    /**
     * To prevent leaking information to unauthorized users, it is extremely important that this filter is executed *after* the
     * {@code SecurityActionFilter}.
     * To achieve that, the number returned by this method must be greater than the number returned by the
     * {@code SecurityActionFilter::order} method.
     */
    @Override
    public int order() {
        return Integer.MAX_VALUE;
    }

    // Visible for testing
    void setUpgradeResetFlags(ClusterChangedEvent event) {
        MlMetadata mlMetadata = MlMetadata.getMlMetadata(event.state());
        upgradeResetFlags.set(new UpgradeResetFlags(mlMetadata.isUpgradeMode(), mlMetadata.isResetMode()));
    }

    /**
     * Class to allow both upgrade and reset flags to be recorded atomically so that code that checks both
     * one after the other doesn't see inconsistent values.
     */
    private static class UpgradeResetFlags {

        final boolean isUpgradeMode;
        final boolean isResetMode;

        UpgradeResetFlags(boolean isUpgradeMode, boolean isResetMode) {
            this.isUpgradeMode = isUpgradeMode;
            this.isResetMode = isResetMode;
        }
    }
}
