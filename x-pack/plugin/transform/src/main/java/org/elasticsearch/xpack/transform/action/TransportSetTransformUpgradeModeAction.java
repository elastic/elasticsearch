/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.AbstractTransportSetUpgradeModeAction;
import org.elasticsearch.xpack.core.action.SetUpgradeModeActionRequest;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.elasticsearch.xpack.core.transform.action.SetTransformUpgradeModeAction;

import java.util.Comparator;
import java.util.stream.Collectors;

import static org.elasticsearch.exception.ExceptionsHelper.rethrowAndSuppress;
import static org.elasticsearch.xpack.core.ClientHelper.TRANSFORM_ORIGIN;
import static org.elasticsearch.xpack.core.transform.TransformField.AWAITING_UPGRADE;

public class TransportSetTransformUpgradeModeAction extends AbstractTransportSetUpgradeModeAction {
    private static final Logger logger = LogManager.getLogger(TransportSetTransformUpgradeModeAction.class);
    private final PersistentTasksClusterService persistentTasksClusterService;
    private final PersistentTasksService persistentTasksService;
    private final OriginSettingClient client;

    @Inject
    public TransportSetTransformUpgradeModeAction(
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters,
        PersistentTasksClusterService persistentTasksClusterService,
        PersistentTasksService persistentTasksService,
        Client client
    ) {
        super(SetTransformUpgradeModeAction.NAME, "transform", transportService, clusterService, threadPool, actionFilters);
        this.persistentTasksClusterService = persistentTasksClusterService;
        this.persistentTasksService = persistentTasksService;
        this.client = new OriginSettingClient(client, TRANSFORM_ORIGIN);
    }

    @Override
    protected String featureName() {
        return "transform-set-upgrade-mode";
    }

    @Override
    protected boolean upgradeMode(ClusterState state) {
        return TransformMetadata.upgradeMode(state);
    }

    @Override
    protected ClusterState createUpdatedState(SetUpgradeModeActionRequest request, ClusterState state) {
        var updatedTransformMetadata = TransformMetadata.getTransformMetadata(state).builder().upgradeMode(request.enabled()).build();
        var updatedClusterMetadata = state.metadata().copyAndUpdate(b -> b.putCustom(TransformMetadata.TYPE, updatedTransformMetadata));
        return state.copyAndUpdate(b -> b.metadata(updatedClusterMetadata));
    }

    @Override
    protected void upgradeModeSuccessfullyChanged(
        Task task,
        SetUpgradeModeActionRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) {
        PersistentTasksCustomMetadata tasksCustomMetadata = state.metadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);
        if (tasksCustomMetadata == null
            || tasksCustomMetadata.tasks().isEmpty()
            || tasksCustomMetadata.tasks().stream().noneMatch(this::isTransformTask)) {
            logger.info("No tasks to worry about after state update");
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        if (request.enabled()) {
            stopTransforms(request, state, listener);
        } else {
            waitForTransformsToRestart(request, listener);
        }
    }

    private void stopTransforms(SetUpgradeModeActionRequest request, ClusterState state, ActionListener<AcknowledgedResponse> listener) {
        unassignTransforms(state, listener.delegateFailureAndWrap((l, r) -> waitForTransformsToStop(request, l)));
    }

    private void unassignTransforms(ClusterState state, ActionListener<Void> listener) {
        PersistentTasksCustomMetadata tasksCustomMetadata = state.metadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);
        if (tasksCustomMetadata == null) {
            listener.onResponse(null);
            return;
        }
        var transformTasks = tasksCustomMetadata.tasks()
            .stream()
            .filter(this::isTransformTask)
            // We want to always have the same ordering of which tasks we un-allocate first.
            // However, the order in which the distributed tasks handle the un-allocation event is not guaranteed.
            .sorted(Comparator.comparing(PersistentTasksCustomMetadata.PersistentTask::getTaskName))
            .toList();

        logger.info(
            "Un-assigning persistent tasks : {}",
            () -> transformTasks.stream()
                .map(PersistentTasksCustomMetadata.PersistentTask::getId)
                .collect(Collectors.joining(", ", "[ ", " ]"))
        );

        // chain each call one at a time
        // because that is what we are doing for ML, and that is all that is supported in the persistentTasksClusterService (for now)
        SubscribableListener<PersistentTasksCustomMetadata.PersistentTask<?>> chainListener = SubscribableListener.nullSuccess();
        for (var task : transformTasks) {
            @FixForMultiProject
            final var projectId = Metadata.DEFAULT_PROJECT_ID;
            chainListener = chainListener.andThen(executor, threadPool.getThreadContext(), (l, unused) -> {
                persistentTasksClusterService.unassignPersistentTask(
                    projectId,
                    task.getId(),
                    task.getAllocationId(),
                    AWAITING_UPGRADE.getExplanation(),
                    l.delegateResponse((ll, ex) -> {
                        // ignore ResourceNotFoundException since a delete task is already unassigned
                        if (ExceptionsHelper.unwrapCause(ex) instanceof ResourceNotFoundException) {
                            ll.onResponse(null);
                        } else {
                            ll.onFailure(ex);
                        }
                    })
                );
            });
        }
        chainListener.addListener(listener.delegateFailure((l, ignored) -> l.onResponse(null)), executor, threadPool.getThreadContext());
    }

    private void waitForTransformsToStop(SetUpgradeModeActionRequest request, ActionListener<AcknowledgedResponse> listener) {
        client.admin()
            .cluster()
            .prepareListTasks()
            .setActions(TransformField.TASK_NAME + "[c]")
            // There is a chance that we failed un-allocating a task due to allocation_id being changed
            // This call will timeout in that case and return an error
            .setWaitForCompletion(true)
            .setTimeout(request.ackTimeout())
            .execute(listener.delegateFailureAndWrap((l, r) -> {
                try {
                    // Handle potential node timeouts,
                    // these should be considered failures as tasks as still potentially executing
                    logger.info("Waited for tasks to be unassigned");
                    if (r.getNodeFailures().isEmpty() == false) {
                        logger.info("There were node failures waiting for tasks", r.getNodeFailures().get(0));
                    }
                    rethrowAndSuppress(r.getNodeFailures());
                    l.onResponse(AcknowledgedResponse.TRUE);
                } catch (ElasticsearchException ex) {
                    logger.info("Caught node failures waiting for tasks to be unassigned", ex);
                    l.onFailure(ex);
                }
            }));
    }

    private boolean isTransformTask(PersistentTasksCustomMetadata.PersistentTask<?> task) {
        return TransformField.TASK_NAME.equals(task.getTaskName());
    }

    private void waitForTransformsToRestart(SetUpgradeModeActionRequest request, ActionListener<AcknowledgedResponse> listener) {
        logger.info("Disabling upgrade mode for Transforms, must wait for tasks to not have AWAITING_UPGRADE assignment");
        @FixForMultiProject
        final var projectId = Metadata.DEFAULT_PROJECT_ID;
        persistentTasksService.waitForPersistentTasksCondition(projectId, persistentTasksCustomMetadata -> {
            if (persistentTasksCustomMetadata == null) {
                return true;
            }
            return persistentTasksCustomMetadata.tasks()
                .stream()
                .noneMatch(t -> isTransformTask(t) && t.getAssignment().equals(AWAITING_UPGRADE));
        }, request.ackTimeout(), listener.delegateFailureAndWrap((d, r) -> d.onResponse(AcknowledgedResponse.TRUE)));
    }
}
