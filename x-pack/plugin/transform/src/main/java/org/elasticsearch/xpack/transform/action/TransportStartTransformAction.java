/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.action.ValidateTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformDestIndexSettings;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformIndex;
import org.elasticsearch.xpack.transform.transforms.TransformNodes;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.time.Clock;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.transform.TransformMessages.CANNOT_START_FAILED_TRANSFORM;

public class TransportStartTransformAction extends TransportMasterNodeAction<StartTransformAction.Request, StartTransformAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportStartTransformAction.class);
    private final TransformConfigManager transformConfigManager;
    private final PersistentTasksService persistentTasksService;
    private final Client client;
    private final TransformAuditor auditor;
    private final IngestService ingestService;

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
        Settings settings,
        IngestService ingestService
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
            settings,
            ingestService
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
        Settings settings,
        IngestService ingestService
    ) {
        super(
            name,
            transportService,
            clusterService,
            threadPool,
            actionFilters,
            StartTransformAction.Request::new,
            indexNameExpressionResolver,
            StartTransformAction.Response::new,
            ThreadPool.Names.SAME
        );
        this.transformConfigManager = transformServices.getConfigManager();
        this.persistentTasksService = persistentTasksService;
        this.client = client;
        this.auditor = transformServices.getAuditor();
        this.ingestService = ingestService;
    }

    @Override
    protected void masterOperation(
        Task ignoredTask,
        StartTransformAction.Request request,
        ClusterState state,
        ActionListener<StartTransformAction.Response> listener
    ) throws Exception {
        TransformNodes.warnIfNoTransformNodes(state);

        final AtomicReference<TransformTaskParams> transformTaskParamsHolder = new AtomicReference<>();
        final AtomicReference<TransformConfig> transformConfigHolder = new AtomicReference<>();

        // <5> Wait for the allocated task's state to STARTED
        ActionListener<PersistentTasksCustomMetadata.PersistentTask<TransformTaskParams>> newPersistentTaskActionListener = ActionListener
            .wrap(task -> {
                TransformTaskParams transformTask = transformTaskParamsHolder.get();
                assert transformTask != null;
                waitForTransformTaskStarted(
                    task.getId(),
                    transformTask,
                    request.timeout(),
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
                    newPersistentTaskActionListener
                );
            } else {
                TransformState transformState = (TransformState) existingTask.getState();
                if (transformState.getTaskState() == TransformTaskState.FAILED) {
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

        // <2> If the destination index exists, start the task, otherwise deduce our mappings for the destination index and create it
        ActionListener<ValidateTransformAction.Response> validationListener = ActionListener.wrap(validationResponse -> {
            final String destinationIndex = transformConfigHolder.get().getDestination().getIndex();
            String[] dest = indexNameExpressionResolver.concreteIndexNames(state, IndicesOptions.lenientExpandOpen(), destinationIndex);

            if (dest.length == 0) {
                createDestinationIndex(transformConfigHolder.get(), validationResponse.getDestIndexMappings(), ActionListener.wrap(r -> {
                    String message = Boolean.FALSE.equals(transformConfigHolder.get().getSettings().getDeduceMappings())
                        ? "Created destination index [" + destinationIndex + "]."
                        : "Created destination index [" + destinationIndex + "] with deduced mappings.";
                    auditor.info(request.getId(), message);
                    createOrGetIndexListener.onResponse(r);
                }, createOrGetIndexListener::onFailure));
            } else {
                auditor.info(request.getId(), "Using existing destination index [" + destinationIndex + "].");
                ClientHelper.executeAsyncWithOrigin(
                    client.threadPool().getThreadContext(),
                    ClientHelper.TRANSFORM_ORIGIN,
                    client.admin().indices().prepareStats(dest).clear().setDocs(true).request(),
                    ActionListener.<IndicesStatsResponse>wrap(r -> {
                        long docTotal = r.getTotal().docs.getCount();
                        if (docTotal > 0L) {
                            auditor.warning(
                                request.getId(),
                                "Non-empty destination index [" + destinationIndex + "]. " + "Contains [" + docTotal + "] total documents."
                            );
                        }
                        createOrGetIndexListener.onResponse(true);
                    }, e -> {
                        String msg = "Unable to determine destination index stats, error: " + e.getMessage();
                        logger.warn(msg, e);
                        auditor.warning(request.getId(), msg);
                        createOrGetIndexListener.onResponse(true);
                    }),
                    client.admin().indices()::stats
                );
            }
        }, listener::onFailure);

        // <2> run transform validations
        ActionListener<TransformConfig> getTransformListener = ActionListener.wrap(config -> {
            ValidationException validationException = config.validate(null);
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
                    config.getFrequency(),
                    config.getSource().requiresRemoteCluster()
                )
            );
            transformConfigHolder.set(config);
            client.execute(
                ValidateTransformAction.INSTANCE,
                new ValidateTransformAction.Request(config, false, request.timeout()),
                validationListener
            );
        }, listener::onFailure);

        // <1> Get the config to verify it exists and is valid
        transformConfigManager.getTransformConfiguration(request.getId(), getTransformListener);
    }

    private void createDestinationIndex(
        final TransformConfig config,
        final Map<String, String> mappings,
        final ActionListener<Boolean> listener
    ) {

        TransformDestIndexSettings generatedDestIndexSettings = TransformIndex.createTransformDestIndexSettings(
            mappings,
            config.getId(),
            Clock.systemUTC()
        );
        TransformIndex.createDestinationIndex(client, config, generatedDestIndexSettings, listener);
    }

    @Override
    protected ClusterBlockException checkBlock(StartTransformAction.Request request, ClusterState state) {
        return state.blocks().globalBlockedException(ClusterBlockLevel.METADATA_WRITE);
    }

    private void cancelTransformTask(String taskId, String transformId, Exception exception, Consumer<Exception> onFailure) {
        persistentTasksService.sendRemoveRequest(taskId, new ActionListener<PersistentTasksCustomMetadata.PersistentTask<?>>() {
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
                        new ElasticsearchException("Starting transform [" + params.getId() + "] timed out after [" + timeout + "]")
                    );
                }
            }
        );
    }

    /**
     * Important: the methods of this class must NOT throw exceptions.  If they did then the callers
     * of endpoints waiting for a condition tested by this predicate would never get a response.
     */
    private class TransformPredicate implements Predicate<PersistentTasksCustomMetadata.PersistentTask<?>> {

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
        private boolean isNotStopped(PersistentTasksCustomMetadata.PersistentTask<?> task) {
            TransformState state = (TransformState) task.getState();
            return state != null && state.getTaskState().equals(TransformTaskState.STOPPED) == false;
        }
    }
}
