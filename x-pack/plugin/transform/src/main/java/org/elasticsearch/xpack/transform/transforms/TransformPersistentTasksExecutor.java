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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.security.cloud.PersistedCloudCredential;
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
    protected PersistentTasksCustomMetadata.Assignment doGetAssignment(
        TransformTaskParams params,
        Collection<DiscoveryNode> candidateNodes,
        ClusterState clusterState,
        @Nullable ProjectId projectId
    ) {
        /* Note:
         *
         * This method is executed on the _master_ node. The master and transform node might be on a different version.
         * Therefore certain checks must happen on the corresponding node, e.g. the existence of the internal index.
         *
         * Operations on the transform node happen in {@link #nodeOperation()}
         */
        var transformMetadata = TransformMetadata.getTransformMetadata(clusterState);
        if (transformMetadata.isUpgradeMode()) {
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
    public boolean automaticReassignmentOnShutdown() {
        return false;
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
        ActionListener<StartTransformAction.Response> startTaskListener = ActionListener.wrap(response -> {
            logger.info("[{}] successfully completed and scheduled task in node operation", transformId);
            transformServices.scheduler().registerTransform(params, buildTask);
        }, failure -> {
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
        });

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

            startTask(buildTask, params, indexerBuilder, authState, lastCheckpoint, startTaskListener);
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
                    startTask(buildTask, params, indexerBuilder, null, null, startTaskListener);
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

            var validationException = config.validate(null);

            // if we had created a transform when the feature flag was enabled, but we disabled the feature flag
            // then verify that this transform does not use CPS features
            validationException = config.validateNoCrossProjectWhenCrossProjectIsDisabled(
                transformServices.crossProjectModeDecider(),
                validationException
            );

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
        TransformTaskParams params,
        ClientTransformIndexerBuilder indexerBuilder,
        AuthorizationState authState,
        Long previousCheckpoint,
        ActionListener<StartTransformAction.Response> listener
    ) {
        // if we fail the first request, we are going to start retrying until we succeed. when start fails, it is because the cluster state
        // is not handling updates yet, but the cluster will eventually recover on its own.
        var startRetriesOnFirstFailureListener = listener.delegateResponse((l, e) -> {
            // copy the params but replace the frequency, this is to prevent every transform from starting and retrying every second,
            // potentially sending many cluster state updates at once. instead, add randomness to spread out the retry requests after the
            // first retry
            var retryTimer = TimeValue.timeValueSeconds(45 + Randomness.get().nextInt(15, 45));
            var paramsWithExtendedTimer = new TransformTaskParams(
                params.getId(),
                params.getVersion(),
                params.from(),
                retryTimer,
                params.requiresRemote()
            );
            logger.debug("Failed to start Transform, retrying in [{}] seconds.", retryTimer.seconds());
            // tell the user when and why the retries are happening and how to stop them
            // force stopping will eventually deregister this retry task from the scheduler
            auditor.warning(
                params.getId(),
                Strings.format(
                    "Failed while starting Transform. Automatically retrying every [%s] seconds. "
                        + "To cancel retries, use [_transform/%s/_stop?force] to force stop this transform. Failure: [%s]",
                    retryTimer.seconds(),
                    params.getId(),
                    e.getMessage()
                )
            );
            var scheduler = transformServices.scheduler();
            scheduler.registerTransform(
                paramsWithExtendedTimer,
                new TransformRetryableStartUpListener<>(
                    paramsWithExtendedTimer.getId(),
                    ll -> buildTask.start(previousCheckpoint, ll),
                    ActionListener.runBefore(l, () -> scheduler.deregisterTransform(paramsWithExtendedTimer.getId())),
                    ActionListener.noop(),
                    () -> true,
                    buildTask.getContext()
                )
            );
        });
        // switch the threadpool to generic, because the caller is on the system_read threadpool
        threadPool.generic().execute(() -> {
            buildTask.initializeIndexer(indexerBuilder);
            buildTask.setAuthState(authState);

            Runnable doStart = () -> buildTask.setNumFailureRetries(numFailureRetries)
                .start(previousCheckpoint, startRetriesOnFirstFailureListener);

            String credentialId = indexerBuilder.getTransformConfig() == null
                ? null
                : indexerBuilder.getTransformConfig().getCredentialId();

            // Best-effort startup sweep: revoke + delete any credential docs for this transform whose
            // tokenId is not the currently-active credentialId. This closes the gap for batch transforms
            // (which don't reload config mid-run) and cleans up any dangling credentials from prior
            // interrupted rotations. Failures are logged but do not block the transform from starting.
            if (TransformConfig.TRANSFORM_CROSS_PROJECT.isEnabled()) {
                sweepDanglingCredentials(params.getId(), credentialId, () -> {
                    if (credentialId != null) {
                        loadCloudCredentialWithRetry(buildTask, params, credentialId, doStart);
                    } else {
                        // Feature off, or this transform has no associated UIAM credential — nothing to load.
                        doStart.run();
                    }
                });
            } else {
                doStart.run();
            }
        });
    }

    /**
     * Best-effort startup sweep: lists all credential storage docs owned by {@code transformId}
     * and revokes + deletes any whose tokenId is not the currently-active {@code activeCredentialId}.
     * Designed to clean up dangling tokens left by interrupted rotations (e.g. a batch transform
     * that was updated while INDEXING) and by the {@code _update} stopped-task revoke path.
     * Failures at any stage are logged but never propagate — startup is not blocked.
     *
     * @param transformId        the transform whose credentials to sweep
     * @param activeCredentialId the tokenId of the currently-active credential (excluded from sweep);
     *                           may be null (no active credential → all found tokens are dangling)
     * @param next               called once all per-token cleanup attempts have completed
     */
    private void sweepDanglingCredentials(String transformId, @Nullable String activeCredentialId, Runnable next) {
        transformServices.configManager().forEachTransformCloudCredential(transformId, credential -> {
            if (credential.id().equals(activeCredentialId) == false) {
                transformServices.cloudCredentialManager().revokeCloseAndDelete(transformId, credential);
            }
        },
            ActionListener.runAfter(
                ActionListener.wrap(
                    ignored -> {},
                    e -> logger.warn(() -> "[" + transformId + "] failed to list credentials for startup sweep; proceeding", e)
                ),
                next
            )
        );
    }

    /**
     * Loads the persisted cloud credential for {@code credentialId} (the UIAM tokenId recorded on
     * the {@link TransformConfig}) and sets it on the task context before running {@code doStart}.
     * The first attempt is direct; if it fails (system index unavailable, cluster state still
     * recovering, ...) we hand off to a {@link TransformRetryableStartUpListener} registered with
     * the {@link org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler} that
     * retries indefinitely — same shape as {@link #startTask}'s post-start retry. The user can
     * abort with {@code _stop?force=true}, which deregisters the scheduler entry.
     */
    private void loadCloudCredentialWithRetry(TransformTask buildTask, TransformTaskParams params, String credentialId, Runnable doStart) {
        var transformId = params.getId();
        ActionListener<PersistedCloudCredential> setCredentialAndStart = ActionListener.wrap(credential -> {
            if (credential != null) {
                logger.debug("[{}] loaded cloud credential [{}] for task start", transformId, credential.id());
            }
            buildTask.getContext().setPersistedCloudCredential(credential);
            doStart.run();
        },
            // shouldRetry==() -> true so this only fires if the task was stopped while retries were pending
            e -> logger.debug(() -> "[" + transformId + "] cloud credential load aborted", e)
        );
        transformServices.configManager()
            .getTransformCloudCredentialByTokenId(credentialId, true, setCredentialAndStart.delegateResponse((l, e) -> {
                // First attempt failed. Failures here are almost always transient; hand off to the
                // scheduler so we retry indefinitely until the system index is back. The user can
                // _stop?force=true to abort.
                logger.warn(
                    () -> "[" + transformId + "] failed to load cloud credential [" + credentialId + "], retrying via scheduler",
                    e
                );
                var scheduler = transformServices.scheduler();
                scheduler.registerTransform(
                    params,
                    new TransformRetryableStartUpListener<>(
                        transformId,
                        ll -> transformServices.configManager().getTransformCloudCredentialByTokenId(credentialId, true, ll),
                        ActionListener.runBefore(l, () -> scheduler.deregisterTransform(transformId)),
                        retryListener(buildTask),
                        () -> true,
                        buildTask.getContext()
                    )
                );
            }));
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
