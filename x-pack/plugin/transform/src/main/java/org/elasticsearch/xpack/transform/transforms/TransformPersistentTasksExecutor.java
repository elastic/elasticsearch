/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ValidationException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.TransformDeprecations;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationState;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.Transform;
import org.elasticsearch.xpack.transform.TransformConfigAutoMigration;
import org.elasticsearch.xpack.transform.TransformExtension;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.persistence.TransformInternalIndex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.common.notifications.Level.ERROR;
import static org.elasticsearch.xpack.core.common.notifications.Level.INFO;
import static org.elasticsearch.xpack.core.transform.TransformField.AWAITING_UPGRADE;
import static org.elasticsearch.xpack.core.transform.TransformField.RESET_IN_PROGRESS;
import static org.elasticsearch.xpack.transform.transforms.TransformNodes.nodeCanRunThisTransform;

public class TransformPersistentTasksExecutor extends PersistentTasksExecutor<TransformTaskParams> {

    private static final Logger logger = LogManager.getLogger(TransformPersistentTasksExecutor.class);

    // The amount of time we wait for the cluster state to respond when being marked as failed
    private static final int MARK_AS_FAILED_TIMEOUT_SEC = 90;
    private final Client client;
    private final TransformServices transformServices;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final IndexNameExpressionResolver resolver;
    private final TransformAuditor auditor;
    private final TransformExtension transformExtension;
    private final TransformConfigAutoMigration transformConfigAutoMigration;
    private volatile int numFailureRetries;

    public TransformPersistentTasksExecutor(
        Client client,
        TransformServices transformServices,
        ThreadPool threadPool,
        ClusterService clusterService,
        Settings settings,
        TransformExtension transformExtension,
        IndexNameExpressionResolver resolver,
        TransformConfigAutoMigration transformConfigAutoMigration
    ) {
        super(TransformField.TASK_NAME, threadPool.generic());
        this.client = client;
        this.transformServices = transformServices;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.resolver = resolver;
        this.auditor = transformServices.auditor();
        this.numFailureRetries = Transform.NUM_FAILURE_RETRIES_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(Transform.NUM_FAILURE_RETRIES_SETTING, this::setNumFailureRetries);
        this.transformExtension = transformExtension;
        this.transformConfigAutoMigration = transformConfigAutoMigration;
    }

    @Override
    public PersistentTasksCustomMetadata.Assignment getAssignment(
        TransformTaskParams params,
        Collection<DiscoveryNode> candidateNodes,
        ClusterState clusterState
    ) {
        /* Note:
         *
         * This method is executed on the _master_ node. The master and transform node might be on a different version.
         * Therefore certain checks must happen on the corresponding node, e.g. the existence of the internal index.
         *
         * Operations on the transform node happen in {@link #nodeOperation()}
         */
        var transformMetadata = TransformMetadata.getTransformMetadata(clusterState);
        if (transformMetadata.upgradeMode()) {
            return AWAITING_UPGRADE;
        }
        if (transformMetadata.resetMode()) {
            return RESET_IN_PROGRESS;
        }
        List<String> unavailableIndices = verifyIndicesPrimaryShardsAreActive(clusterState, resolver);
        if (unavailableIndices.size() != 0) {
            String reason = "Not starting transform ["
                + params.getId()
                + "], "
                + "because not all primary shards are active for the following indices ["
                + String.join(",", unavailableIndices)
                + "]";
            logger.debug(reason);
            return new PersistentTasksCustomMetadata.Assignment(null, reason);
        }
        Map<String, String> explainWhyAssignmentFailed = new TreeMap<>();
        DiscoveryNode discoveryNode = selectLeastLoadedNode(
            clusterState,
            candidateNodes,
            node -> nodeCanRunThisTransform(node, params.getVersion(), params.requiresRemote(), explainWhyAssignmentFailed)
        );

        if (discoveryNode == null) {
            // clusterState can report an empty node list when the cluster health is yellow, if we have no other reason then include that
            var nodes = clusterState.getNodes();
            if (nodes.iterator().hasNext() == false && explainWhyAssignmentFailed.isEmpty()) {
                var key = Optional.ofNullable(clusterState.getMetadata()).map(Metadata::clusterUUID).orElse("");
                explainWhyAssignmentFailed.put(
                    key,
                    "No Discovery Nodes found in cluster state. Check cluster health and troubleshoot missing Discovery Nodes."
                );
            } else {
                for (DiscoveryNode node : nodes) {
                    nodeCanRunThisTransform(node, params.getVersion(), params.requiresRemote(), explainWhyAssignmentFailed);
                }
            }

            String reason = "Not starting transform ["
                + params.getId()
                + "], reasons ["
                + explainWhyAssignmentFailed.entrySet().stream().map(e -> e.getKey() + ":" + e.getValue()).collect(Collectors.joining("|"))
                + "]";

            logger.debug(reason);
            return new PersistentTasksCustomMetadata.Assignment(null, reason);
        }

        return new PersistentTasksCustomMetadata.Assignment(discoveryNode.getId(), "");
    }

    static List<String> verifyIndicesPrimaryShardsAreActive(ClusterState clusterState, IndexNameExpressionResolver resolver) {
        String[] indices = resolver.concreteIndexNames(
            clusterState,
            IndicesOptions.lenientExpandOpen(),
            TransformInternalIndexConstants.INDEX_NAME_PATTERN,
            TransformInternalIndexConstants.INDEX_NAME_PATTERN_DEPRECATED
        );
        List<String> unavailableIndices = new ArrayList<>(indices.length);
        for (String index : indices) {
            IndexRoutingTable routingTable = clusterState.getRoutingTable().index(index);
            if (routingTable == null || routingTable.allPrimaryShardsActive() == false || routingTable.readyForSearch() == false) {
                unavailableIndices.add(index);
            }
        }
        return unavailableIndices;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, @Nullable TransformTaskParams params, PersistentTaskState state) {
        /* Note:
         *
         * This method is executed on the _transform_ node. The master and transform node might be on a different version.
         * Operations on master happen in {@link #getAssignment()}
         */

        final String transformId = params.getId();
        final TransformTask buildTask = (TransformTask) task;
        final ParentTaskAssigningClient parentTaskClient = new ParentTaskAssigningClient(client, buildTask.getParentTaskId());
        // NOTE: TransformPersistentTasksExecutor#createTask pulls in the stored task state from the ClusterState when the object
        // is created. TransformTask#ctor takes into account setting the task as failed if that is passed in with the
        // persisted state.
        // TransformPersistentTasksExecutor#startTask will fail as TransformTask#start, when force == false, will return
        // a failure indicating that a failed task cannot be started.
        //
        // We want the rest of the state to be populated in the task when it is loaded on the node so that users can force start it again
        // later if they want.
        final ClientTransformIndexerBuilder indexerBuilder = new ClientTransformIndexerBuilder().setClient(parentTaskClient)
            .setClusterService(clusterService)
            .setIndexNameExpressionResolver(resolver)
            .setTransformExtension(transformExtension)
            .setTransformServices(transformServices);

        final SetOnce<TransformState> stateHolder = new SetOnce<>();

        // <8> log the start result
        ActionListener<StartTransformAction.Response> startTaskListener = ActionListener.wrap(
            response -> logger.info("[{}] successfully completed and scheduled task in node operation", transformId),
            failure -> {
                // If the transform is failed then there is no need to log an error on every node restart as the error had already been
                // logged when the transform first failed.
                boolean logErrorAsInfo = failure instanceof CannotStartFailedTransformException;
                auditor.audit(
                    logErrorAsInfo ? INFO : ERROR,
                    transformId,
                    "Failed to start transform. Please stop and attempt to start again. Failure: " + failure.getMessage()
                );
                logger.atLevel(logErrorAsInfo ? Level.INFO : Level.ERROR)
                    .withThrowable(failure)
                    .log("[{}] Failed to start task in node operation", transformId);
            }
        );

        // <7> load next checkpoint
        ActionListener<TransformCheckpoint> getTransformNextCheckpointListener = ActionListener.wrap(nextCheckpoint -> {
            // threadpool: system_read

            if (nextCheckpoint.isEmpty()) {
                // extra safety: reset position and progress if next checkpoint is empty
                // prevents a failure if for some reason the next checkpoint has been deleted
                indexerBuilder.setInitialPosition(null);
                indexerBuilder.setProgress(null);
            } else {
                logger.trace("[{}] Loaded next checkpoint [{}] found, starting the task", transformId, nextCheckpoint.getCheckpoint());
                indexerBuilder.setNextCheckpoint(nextCheckpoint);
            }

            final long lastCheckpoint = stateHolder.get().getCheckpoint();
            final AuthorizationState authState = stateHolder.get().getAuthState();

            startTask(buildTask, indexerBuilder, authState, lastCheckpoint, startTaskListener);
        }, error -> {
            // TODO: do not use the same error message as for loading the last checkpoint
            String msg = TransformMessages.getMessage(TransformMessages.FAILED_TO_LOAD_TRANSFORM_CHECKPOINT, transformId);
            logger.error(msg, error);
            markAsFailed(buildTask, error, msg);
        });

        // <6> load last checkpoint
        ActionListener<TransformCheckpoint> getTransformLastCheckpointListener = ActionListener.wrap(lastCheckpoint -> {
            // threadpool: system_read

            indexerBuilder.setLastCheckpoint(lastCheckpoint);
            logger.trace("[{}] Loaded last checkpoint [{}], looking for next checkpoint", transformId, lastCheckpoint.getCheckpoint());
            transformServices.configManager()
                .getTransformCheckpoint(transformId, lastCheckpoint.getCheckpoint() + 1, getTransformNextCheckpointListener);
        }, error -> {
            String msg = TransformMessages.getMessage(TransformMessages.FAILED_TO_LOAD_TRANSFORM_CHECKPOINT, transformId);
            logger.error(msg, error);
            markAsFailed(buildTask, error, msg);
        });

        // <5> Set the previous stats (if they exist), initialize the indexer, start the task (If it is STOPPED)
        // Since we don't create the task until `_start` is called, if we see that the task state is stopped, attempt to start
        // Schedule execution regardless
        ActionListener<Tuple<TransformStoredDoc, SeqNoPrimaryTermAndIndex>> transformStatsActionListener = ActionListener.wrap(
            stateAndStatsAndSeqNoPrimaryTermAndIndex -> {
                // threadpool: system_read

                TransformStoredDoc stateAndStats = stateAndStatsAndSeqNoPrimaryTermAndIndex.v1();
                SeqNoPrimaryTermAndIndex seqNoPrimaryTermAndIndex = stateAndStatsAndSeqNoPrimaryTermAndIndex.v2();
                // Since we have not set the value for this yet, it SHOULD be null
                logger.trace("[{}] initializing state and stats: [{}]", transformId, stateAndStats.toString());
                TransformState transformState = stateAndStats.getTransformState();
                indexerBuilder.setInitialStats(stateAndStats.getTransformStats())
                    .setInitialPosition(stateAndStats.getTransformState().getPosition())
                    .setProgress(stateAndStats.getTransformState().getProgress())
                    .setIndexerState(currentIndexerState(transformState))
                    .setSeqNoPrimaryTermAndIndex(seqNoPrimaryTermAndIndex)
                    .setShouldStopAtCheckpoint(transformState.shouldStopAtNextCheckpoint());
                logger.debug(
                    "[{}] Loading existing state: [{}], position [{}]",
                    transformId,
                    stateAndStats.getTransformState(),
                    stateAndStats.getTransformState().getPosition()
                );

                stateHolder.set(transformState);
                final long lastCheckpoint = stateHolder.get().getCheckpoint();

                if (lastCheckpoint == 0) {
                    logger.trace("[{}] No last checkpoint found, looking for next checkpoint", transformId);
                    transformServices.configManager()
                        .getTransformCheckpoint(transformId, lastCheckpoint + 1, getTransformNextCheckpointListener);
                } else {
                    logger.trace("[{}] Restore last checkpoint: [{}]", transformId, lastCheckpoint);
                    transformServices.configManager()
                        .getTransformCheckpoint(transformId, lastCheckpoint, getTransformLastCheckpointListener);
                }
            },
            error -> {
                if (error instanceof ResourceNotFoundException == false) {
                    String msg = TransformMessages.getMessage(TransformMessages.FAILED_TO_LOAD_TRANSFORM_STATE, transformId);
                    logger.error(msg, error);
                    markAsFailed(buildTask, error, msg);
                } else {
                    logger.trace("[{}] No stats found (new transform), starting the task", transformId);
                    startTask(buildTask, indexerBuilder, null, null, startTaskListener);
                }
            }
        );

        // <4> Validate the transform, assigning it to the indexer, and get the previous stats (if they exist)
        ActionListener<TransformConfig> getTransformConfigListener = transformStatsActionListener.delegateFailureAndWrap((l, config) -> {
            // threadpool: system_read

            // fail if a transform is too old, this can only happen on a rolling upgrade
            if (config.getVersion() == null || config.getVersion().before(TransformDeprecations.MIN_TRANSFORM_VERSION)) {
                String transformTooOldError = format(
                    "Transform configuration is too old [%s], use the upgrade API to fix your transform. "
                        + "Minimum required version is [%s]",
                    config.getVersion(),
                    TransformDeprecations.MIN_TRANSFORM_VERSION
                );
                auditor.error(transformId, transformTooOldError);
                markAsFailed(buildTask, null, transformTooOldError);
                return;
            }

            ValidationException validationException = config.validate(null);
            if (validationException == null) {
                indexerBuilder.setTransformConfig(config);
                transformServices.configManager().getTransformStoredDoc(transformId, false, l);
            } else {
                auditor.error(transformId, validationException.getMessage());
                markAsFailed(
                    buildTask,
                    validationException,
                    TransformMessages.getMessage(
                        TransformMessages.TRANSFORM_CONFIGURATION_INVALID,
                        transformId,
                        validationException.getMessage()
                    )
                );
            }
        });

        // <3> Automatically migrate the Transform off of deprecated features
        ActionListener<TransformConfig> autoMigrateListener = getTransformConfigListener.delegateFailureAndWrap(
            (l, currentConfig) -> transformConfigAutoMigration.migrateAndSave(currentConfig, l)
        );

        // <2> Get the transform config
        var templateCheckListener = getTransformConfig(buildTask, params, autoMigrateListener.delegateResponse((l, error) -> {
            String msg = TransformMessages.getMessage(TransformMessages.FAILED_TO_LOAD_TRANSFORM_CONFIGURATION, transformId);
            markAsFailed(buildTask, error, msg);
        }));

        // <1> Check the latest internal index (IMPORTANT: according to _this_ node, which might be newer than master) is installed
        TransformInternalIndex.createLatestVersionedIndexIfRequired(
            clusterService,
            parentTaskClient,
            transformExtension.getTransformInternalIndexAdditionalSettings(),
            templateCheckListener.delegateResponse((l, e) -> {
                Throwable cause = ExceptionsHelper.unwrapCause(e);
                String msg = "Failed to create internal index mappings";
                markAsFailed(buildTask, e, msg + "[" + cause + "]");
            })
        );
    }

    private static IndexerState currentIndexerState(TransformState previousState) {
        if (previousState == null) {
            return IndexerState.STOPPED;
        }
        return switch (previousState.getIndexerState()) {
            // If it is STARTED or INDEXING we want to make sure we revert to started
            // Otherwise, the internal indexer will never get scheduled and execute
            case STARTED, INDEXING -> IndexerState.STARTED;
            // If we are STOPPED, STOPPING, or ABORTING and just started executing on this node,
            // then it is safe to say we should be STOPPED
            case STOPPED, STOPPING, ABORTING -> IndexerState.STOPPED;
        };
    }

    private static void markAsFailed(TransformTask task, Throwable exception, String reason) {
        CountDownLatch latch = new CountDownLatch(1);

        task.fail(
            exception,
            reason,
            new LatchedActionListener<>(
                ActionListener.wrap(
                    nil -> {},
                    failure -> logger.error("Failed to set task [" + task.getTransformId() + "] to failed", failure)
                ),
                latch
            )
        );
        try {
            latch.await(MARK_AS_FAILED_TIMEOUT_SEC, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            logger.error("Timeout waiting for task [" + task.getTransformId() + "] to be marked as failed in cluster state", e);
        }
    }

    private ActionListener<Void> getTransformConfig(
        TransformTask task,
        TransformTaskParams params,
        ActionListener<TransformConfig> listener
    ) {
        return ActionListener.running(() -> {
            var transformId = params.getId();
            // if this call fails for the first time, we are going to retry it indefinitely
            // register the retry using the TransformScheduler, when the call eventually succeeds, deregister it before returning
            var scheduler = transformServices.scheduler();
            scheduler.registerTransform(
                params,
                new TransformRetryableStartUpListener<>(
                    transformId,
                    l -> transformServices.configManager().getTransformConfiguration(transformId, l),
                    ActionListener.runBefore(listener, () -> scheduler.deregisterTransform(transformId)),
                    retryListener(task),
                    () -> true, // because we can't determine if this is an unattended transform yet, retry indefinitely
                    task.getContext()
                )
            );
        });
    }

    /**
     * This listener is always called after the first execution of a {@link TransformRetryableStartUpListener}.
     *
     * When the result is true, then the first call has failed and will retry. Save the state as Started and unblock the network thread,
     * notifying the user with a 200 OK (acknowledged).
     *
     * When the result is false, then the first call has succeeded, and no further action is required for this listener.
     */
    private ActionListener<Boolean> retryListener(TransformTask task) {
        return ActionListener.wrap(isRetrying -> {
            if (isRetrying) {
                var oldState = task.getState();
                var newState = new TransformState(
                    TransformTaskState.STARTED,
                    oldState.getIndexerState(),
                    oldState.getPosition(),
                    oldState.getCheckpoint(),
                    "Retrying transform start.",
                    oldState.getProgress(),
                    oldState.getNode(),
                    oldState.shouldStopAtNextCheckpoint(),
                    oldState.getAuthState()
                );
                task.persistStateToClusterState(
                    newState,
                    ActionListener.wrap(
                        rr -> logger.debug("[{}] marked as retrying in TransformState.", task.getTransformId()),
                        ee -> logger.atWarn().withThrowable(ee).log("[{}] failed to persist state.", task.getTransformId())
                    )
                );
            }
        }, e -> markAsFailed(task, e, "Failed to initiate retries for Transform."));
    }

    private void startTask(
        TransformTask buildTask,
        ClientTransformIndexerBuilder indexerBuilder,
        AuthorizationState authState,
        Long previousCheckpoint,
        ActionListener<StartTransformAction.Response> listener
    ) {
        // switch the threadpool to generic, because the caller is on the system_read threadpool
        threadPool.generic().execute(() -> {
            buildTask.initializeIndexer(indexerBuilder);
            buildTask.setAuthState(authState);
            // TransformTask#start will fail if the task state is FAILED
            buildTask.setNumFailureRetries(numFailureRetries).start(previousCheckpoint, listener);
        });
    }

    private void setNumFailureRetries(int numFailureRetries) {
        this.numFailureRetries = numFailureRetries;
    }

    @Override
    protected AllocatedPersistentTask createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTasksCustomMetadata.PersistentTask<TransformTaskParams> persistentTask,
        Map<String, String> headers
    ) {
        return new TransformTask(
            id,
            type,
            action,
            parentTaskId,
            persistentTask.getParams(),
            (TransformState) persistentTask.getState(),
            transformServices.scheduler(),
            auditor,
            threadPool,
            headers,
            transformServices.transformNode()
        );
    }
}
