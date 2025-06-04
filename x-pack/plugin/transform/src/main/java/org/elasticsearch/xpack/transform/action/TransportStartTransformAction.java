/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.exception.ElasticsearchStatusException;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationState;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformEffectiveSettings;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.Transform;
import org.elasticsearch.xpack.transform.TransformExtensionHolder;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.AuthorizationStatePersistenceUtils;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformIndex;
import org.elasticsearch.xpack.transform.transforms.TransformNodes;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.elasticsearch.action.ValidateActions.addValidationError;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.transform.TransformMessages.CANNOT_START_FAILED_TRANSFORM;

public class TransportStartTransformAction extends TransportMasterNodeAction<StartTransformAction.Request, StartTransformAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportStartTransformAction.class);
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final TransformConfigManager transformConfigManager;
    private final PersistentTasksService persistentTasksService;
    private final Client client;
    private final TransformAuditor auditor;
    private final Settings destIndexSettings;

    @Inject
    public TransportStartTransformAction(
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ThreadPool threadPool,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TransformServices transformServices,
        PersistentTasksService persistentTasksService,
        Client client,
        TransformExtensionHolder transformExtensionHolder
    ) {
        this(
            StartTransformAction.NAME,
            transportService,
            actionFilters,
            clusterService,
            threadPool,
            indexNameExpressionResolver,
            transformServices,
            persistentTasksService,
            client,
            transformExtensionHolder
        );
    }

    protected TransportStartTransformAction(
        String name,
        TransportService transportService,
        ActionFilters actionFilters,
        ClusterService clusterService,
        ThreadPool threadPool,
        IndexNameExpressionResolver indexNameExpressionResolver,
        TransformServices transformServices,
        PersistentTasksService persistentTasksService,
        Client client,
        TransformExtensionHolder transformExtensionHolder
    ) {
        super(
            name,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            StartTransformAction.Request::new,
            StartTransformAction.Response::new,
            EsExecutors.DIRECT_EXECUTOR_SERVICE
        );
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.transformConfigManager = transformServices.configManager();
        this.persistentTasksService = persistentTasksService;
        this.client = client;
        this.auditor = transformServices.auditor();
        this.destIndexSettings = transformExtensionHolder.getTransformExtension().getTransformDestinationIndexSettings();
    }

    @Override
    protected void masterOperation(
        Task task,
        StartTransformAction.Request request,
        ClusterState state,
        ActionListener<StartTransformAction.Response> listener
    ) {
        TransformNodes.warnIfNoTransformNodes(state);
        if (TransformMetadata.upgradeMode(state)) {
            listener.onFailure(
                new ElasticsearchStatusException(
                    "Cannot start any Transform while the Transform feature is upgrading.",
                    RestStatus.CONFLICT
                )
            );
            return;
        }

        var transformTaskParamsHolder = new SetOnce<TransformTaskParams>();
        var transformConfigHolder = new SetOnce<TransformConfig>();
        var parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        var parentClient = new ParentTaskAssigningClient(client, parentTaskId);

        // <5> Wait for the allocated task's state to STARTED
        ActionListener<PersistentTasksCustomMetadata.PersistentTask<TransformTaskParams>> newPersistentTaskActionListener = ActionListener
            .wrap(t -> {
                TransformTaskParams transformTask = transformTaskParamsHolder.get();
                assert transformTask != null;
                waitForTransformTaskStarted(
                    t.getId(),
                    transformTask,
                    request.ackTimeout(),
                    ActionListener.wrap(taskStarted -> listener.onResponse(new StartTransformAction.Response(true)), listener::onFailure)
                );
            }, listener::onFailure);

        // <4> Create the task in cluster state so that it will start executing on the node
        ActionListener<Boolean> createOrGetIndexListener = ActionListener.wrap(unused -> {
            TransformTaskParams transformTask = transformTaskParamsHolder.get();
            assert transformTask != null;
            PersistentTasksCustomMetadata.PersistentTask<?> existingTask = TransformTask.getTransformTask(transformTask.getId(), state);
            if (existingTask == null) {
                // Create the allocated task and wait for it to be started
                persistentTasksService.sendStartRequest(
                    transformTask.getId(),
                    TransformTaskParams.NAME,
                    transformTask,
                    request.masterNodeTimeout(),
                    newPersistentTaskActionListener
                );
            } else {
                TransformState transformState = (TransformState) existingTask.getState();
                if (transformState != null && transformState.getTaskState() == TransformTaskState.FAILED) {
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            TransformMessages.getMessage(CANNOT_START_FAILED_TRANSFORM, request.getId(), transformState.getReason()),
                            RestStatus.CONFLICT
                        )
                    );
                } else {
                    // If the task already exists that means that it is either running or failed
                    // Since it is not failed, that means it is running, we return a conflict.
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            "Cannot start transform [{}] as it is already started.",
                            RestStatus.CONFLICT,
                            request.getId()
                        )
                    );
                }
            }
        }, listener::onFailure);

        // <3> If the destination index exists, start the task, otherwise deduce our mappings for the destination index and create it
        ActionListener<ValidateTransformAction.Response> validationListener = ActionListener.wrap(validationResponse -> {
            if (TransformEffectiveSettings.isUnattended(transformConfigHolder.get().getSettings())) {
                logger.debug(
                    () -> format("[%s] Skip dest index creation as this is an unattended transform", transformConfigHolder.get().getId())
                );
                createOrGetIndexListener.onResponse(true);
                return;
            }
            TransformIndex.createDestinationIndex(
                parentClient,
                auditor,
                indexNameExpressionResolver,
                state,
                transformConfigHolder.get(),
                destIndexSettings,
                validationResponse.getDestIndexMappings(),
                createOrGetIndexListener
            );
        }, e -> {
            if (TransformEffectiveSettings.isUnattended(transformConfigHolder.get().getSettings())) {
                logger.debug(
                    () -> format("[%s] Skip dest index creation as this is an unattended transform", transformConfigHolder.get().getId())
                );
                createOrGetIndexListener.onResponse(true);
                return;
            }
            listener.onFailure(e);
        });

        // <2> run transform validations
        ActionListener<AuthorizationState> fetchAuthStateListener = ActionListener.wrap(authState -> {
            if (authState != null && HealthStatus.RED.equals(authState.getStatus())) {
                // AuthorizationState status is RED which means there was permission check error during PUT or _update.
                // Since this transform is *not* unattended (otherwise authState would be null), we fail immediately.
                listener.onFailure(new ElasticsearchSecurityException(authState.getLastAuthError(), RestStatus.FORBIDDEN));
                return;
            }

            TransformConfig config = transformConfigHolder.get();

            ActionRequestValidationException validationException = config.validate(null);
            if (request.from() != null && config.getSyncConfig() == null) {
                validationException = addValidationError(
                    "[from] parameter is currently not supported for batch (non-continuous) transforms",
                    validationException
                );
            }
            if (validationException != null) {
                listener.onFailure(
                    new ElasticsearchStatusException(
                        TransformMessages.getMessage(
                            TransformMessages.TRANSFORM_CONFIGURATION_INVALID,
                            request.getId(),
                            validationException.getMessage()
                        ),
                        RestStatus.BAD_REQUEST
                    )
                );
                return;
            }
            transformTaskParamsHolder.set(
                new TransformTaskParams(
                    config.getId(),
                    config.getVersion(),
                    request.from(),
                    config.getFrequency(),
                    config.getSource().requiresRemoteCluster()
                )
            );
            ClientHelper.executeAsyncWithOrigin(
                parentClient,
                ClientHelper.TRANSFORM_ORIGIN,
                ValidateTransformAction.INSTANCE,
                new ValidateTransformAction.Request(config, false, request.ackTimeout()),
                validationListener
            );
        }, listener::onFailure);

        // <1> Check if there is an auth error stored for this transform
        ActionListener<TransformConfig> getTransformListener = ActionListener.wrap(config -> {
            transformConfigHolder.set(config);

            if (TransformEffectiveSettings.isUnattended(config.getSettings())) {
                // We do not fail the _start request of the unattended transform due to permission issues,
                // we just let it run
                fetchAuthStateListener.onResponse(null);
            } else {
                AuthorizationStatePersistenceUtils.fetchAuthState(transformConfigManager, request.getId(), fetchAuthStateListener);
            }
        }, listener::onFailure);

        // <0> Get the config to verify it exists and is valid
        transformConfigManager.getTransformConfiguration(request.getId(), getTransformListener);
    }

    @Override
    protected ClusterBlockException checkBlock(StartTransformAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private void cancelTransformTask(String taskId, String transformId, Exception exception, Consumer<Exception> onFailure) {
        persistentTasksService.sendRemoveRequest(taskId, Transform.HARD_CODED_TRANSFORM_MASTER_NODE_TIMEOUT, new ActionListener<>() {
            @Override
            public void onResponse(PersistentTasksCustomMetadata.PersistentTask<?> task) {
                // We succeeded in canceling the persistent task, but the
                // problem that caused us to cancel it is the overall result
                onFailure.accept(exception);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(
                    "["
                        + transformId
                        + "] Failed to cancel persistent task that could "
                        + "not be assigned due to ["
                        + exception.getMessage()
                        + "]",
                    e
                );
                onFailure.accept(exception);
            }
        });
    }

    private void waitForTransformTaskStarted(
        String taskId,
        TransformTaskParams params,
        TimeValue timeout,
        ActionListener<Boolean> listener
    ) {
        TransformPredicate predicate = new TransformPredicate();
        persistentTasksService.waitForPersistentTaskCondition(
            taskId,
            predicate,
            timeout,
            new PersistentTasksService.WaitForPersistentTaskListener<TransformTaskParams>() {
                @Override
                public void onResponse(PersistentTasksCustomMetadata.PersistentTask<TransformTaskParams> persistentTask) {
                    if (predicate.exception != null) {
                        // We want to return to the caller without leaving an unassigned persistent task
                        cancelTransformTask(taskId, params.getId(), predicate.exception, listener::onFailure);
                    } else {
                        listener.onResponse(true);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }

                @Override
                public void onTimeout(TimeValue timeout) {
                    listener.onFailure(
                        new ElasticsearchStatusException(
                            "Starting transform [{}] timed out after [{}]",
                            RestStatus.REQUEST_TIMEOUT,
                            params.getId(),
                            timeout
                        )
                    );
                }
            }
        );
    }

    /**
     * Important: the methods of this class must NOT throw exceptions.  If they did then the callers
     * of endpoints waiting for a condition tested by this predicate would never get a response.
     */
    private static class TransformPredicate implements Predicate<PersistentTasksCustomMetadata.PersistentTask<?>> {

        private volatile Exception exception;

        @Override
        public boolean test(PersistentTasksCustomMetadata.PersistentTask<?> persistentTask) {
            if (persistentTask == null) {
                return false;
            }
            PersistentTasksCustomMetadata.Assignment assignment = persistentTask.getAssignment();
            if (assignment != null
                && assignment.equals(PersistentTasksCustomMetadata.INITIAL_ASSIGNMENT) == false
                && assignment.isAssigned() == false) {
                // For some reason, the task is not assigned to a node, but is no longer in the `INITIAL_ASSIGNMENT` state
                // Consider this a failure.
                exception = new ElasticsearchStatusException(
                    "Could not start transform, allocation explanation [" + assignment.getExplanation() + "]",
                    RestStatus.TOO_MANY_REQUESTS
                );
                return true;
            }
            // We just want it assigned so we can tell it to start working
            return assignment != null && assignment.isAssigned() && isNotStopped(persistentTask);
        }

        // checking for `isNotStopped` as the state COULD be marked as failed for any number of reasons
        // But if it is in a failed state, _stats will show as much and give good reason to the user.
        // If it is not able to be assigned to a node all together, we should just close the task completely
        private static boolean isNotStopped(PersistentTasksCustomMetadata.PersistentTask<?> task) {
            TransformState state = (TransformState) task.getState();
            return state != null && state.getTaskState().equals(TransformTaskState.STOPPED) == false;
        }
    }
}
