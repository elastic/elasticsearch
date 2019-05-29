/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;
import org.elasticsearch.xpack.ml.utils.TypedChainTaskExecutor;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.ExceptionsHelper.rethrowAndSuppress;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;
import static org.elasticsearch.xpack.core.ml.MlTasks.AWAITING_UPGRADE;
import static org.elasticsearch.xpack.core.ml.MlTasks.DATAFEED_TASK_NAME;
import static org.elasticsearch.xpack.core.ml.MlTasks.JOB_TASK_NAME;

public class TransportSetUpgradeModeAction extends TransportMasterNodeAction<SetUpgradeModeAction.Request, AcknowledgedResponse> {

    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private final PersistentTasksClusterService persistentTasksClusterService;
    private final PersistentTasksService persistentTasksService;
    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public TransportSetUpgradeModeAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                         ClusterService clusterService, PersistentTasksClusterService persistentTasksClusterService,
                                         ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                         Client client, PersistentTasksService persistentTasksService) {
        super(settings, SetUpgradeModeAction.NAME, transportService, clusterService, threadPool, actionFilters, indexNameExpressionResolver,
            SetUpgradeModeAction.Request::new);
        this.persistentTasksClusterService = persistentTasksClusterService;
        this.clusterService = clusterService;
        this.client = client;
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected String executor() {
        return ThreadPool.Names.SAME;
    }

    @Override
    protected AcknowledgedResponse newResponse() {
        return new AcknowledgedResponse();
    }

    @Override
    protected void masterOperation(SetUpgradeModeAction.Request request, ClusterState state, ActionListener<AcknowledgedResponse> listener)
        throws Exception {

        // Don't want folks spamming this endpoint while it is in progress, only allow one request to be handled at a time
        if (isRunning.compareAndSet(false, true) == false) {
            String msg = "Attempted to set [upgrade_mode] to [" +
                request.isEnabled() + "] from [" + MlMetadata.getMlMetadata(state).isUpgradeMode() +
                "] while previous request was processing.";
            Exception detail = new IllegalStateException(msg);
            listener.onFailure(new ElasticsearchStatusException(
                "Cannot change [upgrade_mode]. Previous request is still being processed.",
                RestStatus.TOO_MANY_REQUESTS,
                detail));
            return;
        }

        // Noop, nothing for us to do, simply return fast to the caller
        if (request.isEnabled() == MlMetadata.getMlMetadata(state).isUpgradeMode()) {
            isRunning.set(false);
            listener.onResponse(new AcknowledgedResponse(true));
            return;
        }

        ActionListener<AcknowledgedResponse> wrappedListener = ActionListener.wrap(
            r -> {
                isRunning.set(false);
                listener.onResponse(r);
            },
            e -> {
                isRunning.set(false);
                listener.onFailure(e);
            }
        );
        final PersistentTasksCustomMetaData tasksCustomMetaData = state.metaData().custom(PersistentTasksCustomMetaData.TYPE);

        // <4> We have unassigned the tasks, respond to the listener.
        ActionListener<List<PersistentTask<?>>> unassignPersistentTasksListener = ActionListener.wrap(
            unassigndPersistentTasks -> {
                // Wait for our tasks to all stop
                client.admin()
                    .cluster()
                    .prepareListTasks()
                    .setActions(DATAFEED_TASK_NAME + "[c]", JOB_TASK_NAME + "[c]")
                    // There is a chance that we failed un-allocating a task due to allocation_id being changed
                    // This call will timeout in that case and return an error
                    .setWaitForCompletion(true)
                    .setTimeout(request.timeout()).execute(ActionListener.wrap(
                        r -> {
                            try {
                                // Handle potential node timeouts,
                                // these should be considered failures as tasks as still potentially executing
                                rethrowAndSuppress(r.getNodeFailures());
                                wrappedListener.onResponse(new AcknowledgedResponse(true));
                            } catch (ElasticsearchException ex) {
                                wrappedListener.onFailure(ex);
                            }
                        },
                        wrappedListener::onFailure));
            },
            wrappedListener::onFailure
        );

        // <3> After isolating the datafeeds, unassign the tasks
        ActionListener<List<IsolateDatafeedAction.Response>> isolateDatafeedListener = ActionListener.wrap(
            isolatedDatafeeds -> unassignPersistentTasks(tasksCustomMetaData, unassignPersistentTasksListener),
            wrappedListener::onFailure
        );

        /*
          <2> Handle the cluster response and act accordingly
          <.1>
              If we are enabling the option, we need to isolate the datafeeds so we can unassign the ML Jobs
          </.1>
          <.2>
              If we are disabling the option, we need to wait to make sure all the job and datafeed tasks no longer have the upgrade mode
              assignment


              We make no guarantees around which tasks will be running again once upgrade_mode is disabled.
              Scenario:
                 * Before `upgrade_mode: true`, there were unassigned tasks because node task assignment was maxed out (tasks A, B)
                 * There were assigned tasks executing fine (tasks C, D)
                 * While `upgrade_mode: true` all are attempting to be re-assigned, but cannot and end up with the AWAITING_UPGRADE reason
                 * `upgrade_mode: false` opens the flood gates, all tasks are still attempting to re-assign
                 * A or B could be re-assigned before either C or D. Thus, previously erred tasks are now executing fine, and previously
                   executing tasks are now unassigned due to resource exhaustion.

              We make no promises which tasks are executing if resources of the cluster are exhausted.
          </.2>
          </2>
         */
        ActionListener<AcknowledgedResponse> clusterStateUpdateListener = ActionListener.wrap(
            acknowledgedResponse -> {
                // State change was not acknowledged, we either timed out or ran into some exception
                // We should not continue and alert failure to the end user
                if (acknowledgedResponse.isAcknowledged() == false) {
                    wrappedListener.onFailure(new ElasticsearchTimeoutException("Unknown error occurred while updating cluster state"));
                    return;
                }

                // There are no tasks to worry about starting/stopping
                if (tasksCustomMetaData == null || tasksCustomMetaData.tasks().isEmpty()) {
                    wrappedListener.onResponse(new AcknowledgedResponse(true));
                    return;
                }

                // Did we change from disabled -> enabled?
                if (request.isEnabled()) {
                    isolateDatafeeds(tasksCustomMetaData, isolateDatafeedListener);
                } else {
                    persistentTasksService.waitForPersistentTasksCondition(
                        (persistentTasksCustomMetaData) ->
                            // Wait for jobs to not be "Awaiting upgrade"
                            persistentTasksCustomMetaData.findTasks(JOB_TASK_NAME,
                                (t) -> t.getAssignment().equals(AWAITING_UPGRADE))
                                .isEmpty() &&

                            // Wait for datafeeds to not be "Awaiting upgrade"
                            persistentTasksCustomMetaData.findTasks(DATAFEED_TASK_NAME,
                                (t) -> t.getAssignment().equals(AWAITING_UPGRADE))
                                .isEmpty(),
                        request.timeout(),
                        ActionListener.wrap(r -> wrappedListener.onResponse(new AcknowledgedResponse(true)), wrappedListener::onFailure)
                    );
                }
            },
            wrappedListener::onFailure
        );

        //<1> Change MlMetadata to indicate that upgrade_mode is now enabled
        clusterService.submitStateUpdateTask("ml-set-upgrade-mode",
            new AckedClusterStateUpdateTask<AcknowledgedResponse>(request, clusterStateUpdateListener) {

                @Override
                protected AcknowledgedResponse newResponse(boolean acknowledged) {
                    return new AcknowledgedResponse(acknowledged);
                }

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    MlMetadata.Builder builder = new MlMetadata.Builder(currentState.metaData().custom(MlMetadata.TYPE));
                    builder.isUpgradeMode(request.isEnabled());
                    ClusterState.Builder newState = ClusterState.builder(currentState);
                    newState.metaData(MetaData.builder(currentState.getMetaData()).putCustom(MlMetadata.TYPE, builder.build()).build());
                    return newState.build();
                }
            });
    }

    @Override
    protected ClusterBlockException checkBlock(SetUpgradeModeAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    /**
     * Unassigns all Job and Datafeed tasks.
     * <p>
     * The reason for unassigning both types is that we want the Datafeed to attempt re-assignment once `upgrade_mode` is
     * disabled.
     * <p>
     * If we do not force an allocation change for the Datafeed tasks, they will never start again, since they were isolated.
     * <p>
     * Datafeed tasks keep the state as `started` and Jobs stay `opened`
     *
     * @param tasksCustomMetaData Current state of persistent tasks
     * @param listener            Alerted when tasks are unassignd
     */
    private void unassignPersistentTasks(PersistentTasksCustomMetaData tasksCustomMetaData,
                                         ActionListener<List<PersistentTask<?>>> listener) {
        List<PersistentTask<?>> datafeedAndJobTasks = tasksCustomMetaData
            .tasks()
            .stream()
            .filter(persistentTask -> (persistentTask.getTaskName().equals(MlTasks.JOB_TASK_NAME) ||
                persistentTask.getTaskName().equals(MlTasks.DATAFEED_TASK_NAME)))
            // We want to always have the same ordering of which tasks we un-allocate first.
            // However, the order in which the distributed tasks handle the un-allocation event is not guaranteed.
            .sorted(Comparator.comparing(PersistentTask::getTaskName))
            .collect(Collectors.toList());

        logger.info("Un-assigning persistent tasks : " +
            datafeedAndJobTasks.stream().map(PersistentTask::getId).collect(Collectors.joining(", ", "[ ", " ]")));

        TypedChainTaskExecutor<PersistentTask<?>> chainTaskExecutor =
            new TypedChainTaskExecutor<>(client.threadPool().executor(executor()),
                r -> true,
                // Another process could modify tasks and thus we cannot find them via the allocation_id and name
                // If the task was removed from the node, all is well
                // We handle the case of allocation_id changing later in this transport class by timing out waiting for task completion
                // Consequently, if the exception is ResourceNotFoundException, continue execution; circuit break otherwise.
                ex -> ex instanceof ResourceNotFoundException == false);

        for (PersistentTask<?> task : datafeedAndJobTasks) {
            chainTaskExecutor.add(
                chainedTask -> persistentTasksClusterService.unassignPersistentTask(task.getId(),
                    task.getAllocationId(),
                    AWAITING_UPGRADE.getExplanation(),
                    chainedTask)
            );
        }
        chainTaskExecutor.execute(listener);
    }

    private void isolateDatafeeds(PersistentTasksCustomMetaData tasksCustomMetaData,
                                  ActionListener<List<IsolateDatafeedAction.Response>> listener) {
        Set<String> datafeedsToIsolate = MlTasks.startedDatafeedIds(tasksCustomMetaData);

        logger.info("Isolating datafeeds: " + datafeedsToIsolate.toString());
        TypedChainTaskExecutor<IsolateDatafeedAction.Response> isolateDatafeedsExecutor =
            new TypedChainTaskExecutor<>(client.threadPool().executor(executor()), r -> true, ex -> true);

        datafeedsToIsolate.forEach(datafeedId -> {
            IsolateDatafeedAction.Request isolationRequest = new IsolateDatafeedAction.Request(datafeedId);
            isolateDatafeedsExecutor.add(isolateListener ->
                executeAsyncWithOrigin(client, ML_ORIGIN, IsolateDatafeedAction.INSTANCE, isolationRequest, isolateListener)
            );
        });

        isolateDatafeedsExecutor.execute(listener);
    }
}
