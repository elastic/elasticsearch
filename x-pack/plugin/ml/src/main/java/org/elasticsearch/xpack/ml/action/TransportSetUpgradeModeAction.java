/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.action.AbstractTransportSetUpgradeModeAction;
import org.elasticsearch.xpack.core.action.SetUpgradeModeActionRequest;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.SetUpgradeModeAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.utils.TypedChainTaskExecutor;

import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.exception.ExceptionsHelper.rethrowAndSuppress;
import static org.elasticsearch.xpack.core.ClientHelper.ML_ORIGIN;
import static org.elasticsearch.xpack.core.ml.MlTasks.AWAITING_UPGRADE;
import static org.elasticsearch.xpack.core.ml.MlTasks.DATAFEED_TASK_NAME;
import static org.elasticsearch.xpack.core.ml.MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME;
import static org.elasticsearch.xpack.core.ml.MlTasks.JOB_TASK_NAME;

public class TransportSetUpgradeModeAction extends AbstractTransportSetUpgradeModeAction {

    private static final Set<String> ML_TASK_NAMES = Set.of(JOB_TASK_NAME, DATAFEED_TASK_NAME, DATA_FRAME_ANALYTICS_TASK_NAME);

    private static final Logger logger = LogManager.getLogger(TransportSetUpgradeModeAction.class);
    private final PersistentTasksClusterService persistentTasksClusterService;
    private final PersistentTasksService persistentTasksService;
    private final OriginSettingClient client;

    @Inject
    public TransportSetUpgradeModeAction(
        TransportService transportService,
        ThreadPool threadPool,
        ClusterService clusterService,
        PersistentTasksClusterService persistentTasksClusterService,
        ActionFilters actionFilters,
        Client client,
        PersistentTasksService persistentTasksService
    ) {
        super(SetUpgradeModeAction.NAME, "ml", transportService, clusterService, threadPool, actionFilters);
        this.persistentTasksClusterService = persistentTasksClusterService;
        this.client = new OriginSettingClient(client, ML_ORIGIN);
        this.persistentTasksService = persistentTasksService;
    }

    @Override
    protected String featureName() {
        return "ml-set-upgrade-mode";
    }

    @Override
    protected boolean upgradeMode(ClusterState state) {
        return MlMetadata.getMlMetadata(state).isUpgradeMode();
    }

    @Override
    protected ClusterState createUpdatedState(SetUpgradeModeActionRequest request, ClusterState currentState) {
        logger.trace("Executing cluster state update");
        MlMetadata.Builder builder = new MlMetadata.Builder(currentState.metadata().getProject().custom(MlMetadata.TYPE));
        builder.isUpgradeMode(request.enabled());
        ClusterState.Builder newState = ClusterState.builder(currentState);
        newState.metadata(Metadata.builder(currentState.getMetadata()).putCustom(MlMetadata.TYPE, builder.build()).build());
        return newState.build();
    }

    protected void upgradeModeSuccessfullyChanged(
        Task task,
        SetUpgradeModeActionRequest request,
        ClusterState state,
        ActionListener<AcknowledgedResponse> wrappedListener
    ) {
        final PersistentTasksCustomMetadata tasksCustomMetadata = state.metadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);

        // <4> We have unassigned the tasks, respond to the listener.
        ActionListener<List<PersistentTask<?>>> unassignPersistentTasksListener = ActionListener.wrap(unassignedPersistentTasks -> {
            // Wait for our tasks to all stop
            client.admin()
                .cluster()
                .prepareListTasks()
                .setActions(ML_TASK_NAMES.stream().map(taskName -> taskName + "[c]").toArray(String[]::new))
                // There is a chance that we failed un-allocating a task due to allocation_id being changed
                // This call will timeout in that case and return an error
                .setWaitForCompletion(true)
                .setTimeout(request.ackTimeout())
                .execute(ActionListener.wrap(r -> {
                    try {
                        // Handle potential node timeouts,
                        // these should be considered failures as tasks as still potentially executing
                        logger.info("Waited for tasks to be unassigned");
                        if (r.getNodeFailures().isEmpty() == false) {
                            logger.info("There were node failures waiting for tasks", r.getNodeFailures().get(0));
                        }
                        rethrowAndSuppress(r.getNodeFailures());
                        wrappedListener.onResponse(AcknowledgedResponse.TRUE);
                    } catch (ElasticsearchException ex) {
                        logger.info("Caught node failures waiting for tasks to be unassigned", ex);
                        wrappedListener.onFailure(ex);
                    }
                }, wrappedListener::onFailure));
        }, wrappedListener::onFailure);

        // <3> After isolating the datafeeds, unassign the tasks
        ActionListener<List<IsolateDatafeedAction.Response>> isolateDatafeedListener = ActionListener.wrap(isolatedDatafeeds -> {
            logger.info("Isolated the datafeeds");
            unassignPersistentTasks(tasksCustomMetadata, unassignPersistentTasksListener);
        }, wrappedListener::onFailure);

        /*
          <2> Handle the cluster response and act accordingly
          <.1>
              If we are enabling the option, we need to isolate the datafeeds so we can unassign the ML Jobs
          </.1>
          <.2>
              If we are disabling the option, we need to wait to make sure all the job, datafeed and analytics tasks no longer have the
              upgrade mode assignment


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
        if (tasksCustomMetadata == null || tasksCustomMetadata.tasks().isEmpty()) {
            logger.info("No tasks to worry about after state update");
            wrappedListener.onResponse(AcknowledgedResponse.TRUE);
            return;
        }

        if (request.enabled()) {
            logger.info("Enabling upgrade mode, must isolate datafeeds");
            isolateDatafeeds(tasksCustomMetadata, isolateDatafeedListener);
        } else {
            logger.info("Disabling upgrade mode, must wait for tasks to not have AWAITING_UPGRADE assignment");
            @FixForMultiProject
            final var projectId = Metadata.DEFAULT_PROJECT_ID;
            persistentTasksService.waitForPersistentTasksCondition(
                // Wait for jobs, datafeeds and analytics not to be "Awaiting upgrade"
                projectId,
                persistentTasksCustomMetadata -> {
                    if (persistentTasksCustomMetadata == null) {
                        return true;
                    }
                    return persistentTasksCustomMetadata.tasks()
                        .stream()
                        .noneMatch(t -> ML_TASK_NAMES.contains(t.getTaskName()) && t.getAssignment().equals(AWAITING_UPGRADE));
                },
                request.ackTimeout(),
                ActionListener.wrap(r -> {
                    logger.info("Done waiting for tasks to be out of AWAITING_UPGRADE");
                    wrappedListener.onResponse(AcknowledgedResponse.TRUE);
                }, wrappedListener::onFailure)
            );
        }
    }

    /**
     * Unassigns all Job, Datafeed and Data Frame Analytics tasks.
     * <p>
     * The reason for unassigning both Job and Datafeed is that we want the Datafeed to attempt re-assignment once `upgrade_mode` is
     * disabled.
     * <p>
     * If we do not force an allocation change for the Datafeed tasks, they will never start again, since they were isolated.
     * <p>
     * Datafeed tasks keep the state as `started` and Jobs stay `opened`
     *
     * @param tasksCustomMetadata Current state of persistent tasks
     * @param listener            Alerted when tasks are unassignd
     */
    private void unassignPersistentTasks(
        PersistentTasksCustomMetadata tasksCustomMetadata,
        ActionListener<List<PersistentTask<?>>> listener
    ) {
        List<PersistentTask<?>> mlTasks = tasksCustomMetadata.tasks()
            .stream()
            .filter(persistentTask -> ML_TASK_NAMES.contains(persistentTask.getTaskName()))
            // We want to always have the same ordering of which tasks we un-allocate first.
            // However, the order in which the distributed tasks handle the un-allocation event is not guaranteed.
            .sorted(Comparator.comparing(PersistentTask::getTaskName))
            .toList();

        logger.info(
            "Un-assigning persistent tasks : " + mlTasks.stream().map(PersistentTask::getId).collect(Collectors.joining(", ", "[ ", " ]"))
        );

        TypedChainTaskExecutor<PersistentTask<?>> chainTaskExecutor = new TypedChainTaskExecutor<>(
            executor,
            Predicates.always(),
            // Another process could modify tasks and thus we cannot find them via the allocation_id and name
            // If the task was removed from the node, all is well
            // We handle the case of allocation_id changing later in this transport class by timing out waiting for task completion
            // Consequently, if the exception is ResourceNotFoundException, continue execution; circuit break otherwise.
            ex -> ExceptionsHelper.unwrapCause(ex) instanceof ResourceNotFoundException == false
        );

        for (PersistentTask<?> task : mlTasks) {
            @FixForMultiProject
            final var projectId = Metadata.DEFAULT_PROJECT_ID;
            chainTaskExecutor.add(
                chainedTask -> persistentTasksClusterService.unassignPersistentTask(
                    projectId,
                    task.getId(),
                    task.getAllocationId(),
                    AWAITING_UPGRADE.getExplanation(),
                    chainedTask
                )
            );
        }
        chainTaskExecutor.execute(listener);
    }

    private void isolateDatafeeds(
        PersistentTasksCustomMetadata tasksCustomMetadata,
        ActionListener<List<IsolateDatafeedAction.Response>> listener
    ) {
        Set<String> datafeedsToIsolate = MlTasks.startedDatafeedIds(tasksCustomMetadata);

        logger.info("Isolating datafeeds: " + datafeedsToIsolate.toString());
        TypedChainTaskExecutor<IsolateDatafeedAction.Response> isolateDatafeedsExecutor = new TypedChainTaskExecutor<>(
            executor,
            Predicates.always(),
            Predicates.always()
        );

        datafeedsToIsolate.forEach(datafeedId -> {
            IsolateDatafeedAction.Request isolationRequest = new IsolateDatafeedAction.Request(datafeedId);
            isolateDatafeedsExecutor.add(
                isolateListener -> client.execute(IsolateDatafeedAction.INSTANCE, isolationRequest, isolateListener)
            );
        });

        isolateDatafeedsExecutor.execute(listener);
    }
}
