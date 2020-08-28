/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.service.ClusterService;
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
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateCalendarJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateFilterAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.action.UpdateModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.UpdateProcessAction;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link MlUpgradeModeActionFilter} disallows certain actions if the cluster is currently in upgrade mode.
 *
 * Disallowed actions are the ones which can access/alter the state of ML internal indices.
 */
class MlUpgradeModeActionFilter extends ActionFilter.Simple {

    private static final Set<String> ACTIONS_DISALLOWED_IN_UPGRADE_MODE =
        Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
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

            PutTrainedModelAction.NAME,
            DeleteTrainedModelAction.NAME
        )));

    private final AtomicBoolean isUpgradeMode = new AtomicBoolean();

    MlUpgradeModeActionFilter(ClusterService clusterService) {
        Objects.requireNonNull(clusterService);
        clusterService.addListener(this::setIsUpgradeMode);
    }

    @Override
    protected boolean apply(String action, ActionRequest request, ActionListener<?> listener) {
        if (isUpgradeMode.get() && ACTIONS_DISALLOWED_IN_UPGRADE_MODE.contains(action)) {
            throw new ElasticsearchStatusException(
                "Cannot perform {} action while upgrade mode is enabled", RestStatus.TOO_MANY_REQUESTS, action);
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
    void setIsUpgradeMode(ClusterChangedEvent event) {
        isUpgradeMode.set(MlMetadata.getMlMetadata(event.state()).isUpgradeMode());
    }
}
