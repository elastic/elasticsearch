/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.AcknowledgedTransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractTransportSetUpgradeModeAction extends AcknowledgedTransportMasterNodeAction<SetUpgradeModeActionRequest> {

    private static final Logger logger = LogManager.getLogger(AbstractTransportSetUpgradeModeAction.class);
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final MasterServiceTaskQueue<UpdateModeStateListener> taskQueue;

    public AbstractTransportSetUpgradeModeAction(
        String actionName,
        String taskQueuePrefix,
        TransportService transportService,
        ClusterService clusterService,
        ThreadPool threadPool,
        ActionFilters actionFilters
    ) {
        super(
            actionName,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            SetUpgradeModeActionRequest::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );

        this.taskQueue = clusterService.createTaskQueue(taskQueuePrefix + " upgrade mode", Priority.NORMAL, new UpdateModeExecutor());
    }

    @Override
    protected void masterOperation(
        Task task,
        SetUpgradeModeActionRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    ) throws Exception {
        // Don't want folks spamming this endpoint while it is in progress, only allow one request to be handled at a time
        if (isRunning.compareAndSet(false, true) == false) {
            String msg = Strings.format(
                "Attempted to set [upgrade_mode] for feature name [%s] to [%s] from [%s] while previous request was processing.",
                featureName(),
                request.enabled(),
                upgradeMode(state)
            );
            logger.info(msg);
            Exception detail = new IllegalStateException(msg);
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Cannot change [upgrade_mode] for feature name [{}]. Previous request is still being processed.",
                    RestStatus.TOO_MANY_REQUESTS,
                    detail,
                    featureName()
                )
            );
            return;
        }

        // Noop, nothing for us to do, simply return fast to the caller
        var upgradeMode = upgradeMode(state);
        if (request.enabled() == upgradeMode) {
            logger.info("Upgrade mode noop");
            isRunning.set(false);
            listener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        logger.info(
            "Starting to set [upgrade_mode] for feature name [{}] to [{}] from [{}]",
            featureName(),
            request.enabled(),
            upgradeMode
        );

        ActionListener<AcknowledgedResponse> wrappedListener = ActionListener.wrap(r -> {
            logger.info("Finished setting [upgrade_mode] for feature name [{}]", featureName());
            isRunning.set(false);
            listener.onResponse(r);
        }, e -> {
            logger.info("Failed to set [upgrade_mode] for feature name [{}]", featureName());
            isRunning.set(false);
            listener.onFailure(e);
        });

        ActionListener<AcknowledgedResponse> setUpgradeModeListener = wrappedListener.delegateFailure((delegate, ack) -> {
            if (ack.isAcknowledged()) {
                upgradeModeSuccessfullyChanged(task, request, state, delegate);
            } else {
                logger.info("Cluster state update is NOT acknowledged");
                wrappedListener.onFailure(new ElasticsearchTimeoutException("Unknown error occurred while updating cluster state"));
            }
        });

        taskQueue.submitTask(featureName(), new UpdateModeStateListener(request, setUpgradeModeListener), request.ackTimeout());
    }

    /**
     * Define the feature name, used in log messages and naming the task on the task queue.
     */
    protected abstract String featureName();

    /**
     * Parse the ClusterState for the implementation's {@link org.elasticsearch.cluster.metadata.Metadata.ProjectCustom}
     * and find the upgradeMode boolean stored there.
     * We will compare this boolean with the request's desired state to determine if we should change the metadata.
     */
    protected abstract boolean upgradeMode(ClusterState state);

    /**
     * This is called from the ClusterState updater and is expected to return quickly.
     */
    protected abstract ClusterState createUpdatedState(SetUpgradeModeActionRequest request, ClusterState state);

    /**
     * This method is only called when the cluster state was successfully changed.
     * If we failed to update for any reason, this will not be called.
     * The ClusterState param is the previous ClusterState before we called update.
     */
    protected abstract void upgradeModeSuccessfullyChanged(
        Task task,
        SetUpgradeModeActionRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> listener
    );

    @Override
    protected ClusterBlockException checkBlock(SetUpgradeModeActionRequest request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private record UpdateModeStateListener(SetUpgradeModeActionRequest request, ActionListener<AcknowledgedResponse> listener)
        implements
            ClusterStateTaskListener {

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    private class UpdateModeExecutor extends SimpleBatchedExecutor<UpdateModeStateListener, Void> {
        @Override
        public Tuple<ClusterState, Void> executeTask(UpdateModeStateListener clusterStateListener, ClusterState clusterState) {
            return Tuple.tuple(createUpdatedState(clusterStateListener.request(), clusterState), null);
        }

        @Override
        public void taskSucceeded(UpdateModeStateListener clusterStateListener, Void unused) {
            clusterStateListener.listener().onResponse(AcknowledgedResponse.TRUE);
        }
    }
}
