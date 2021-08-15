/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
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
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.TransformMessages;
import org.elasticsearch.xpack.core.transform.TransformMetadata;
import org.elasticsearch.xpack.core.transform.action.StartTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformStoredDoc;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;
import org.elasticsearch.xpack.transform.Transform;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.SeqNoPrimaryTermAndIndex;
import org.elasticsearch.xpack.transform.persistence.TransformInternalIndex;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.transform.transforms.TransformNodes.nodeCanRunThisTransform;
import static org.elasticsearch.xpack.transform.transforms.TransformNodes.nodeCanRunThisTransformPre77;

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
    private volatile int numFailureRetries;

    public TransformPersistentTasksExecutor(
        Client client,
        TransformServices transformServices,
        ThreadPool threadPool,
        ClusterService clusterService,
        Settings settings,
        IndexNameExpressionResolver resolver
    ) {
        super(TransformField.TASK_NAME, Transform.TASK_THREAD_POOL_NAME);
        this.client = client;
        this.transformServices = transformServices;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.resolver = resolver;
        this.auditor = transformServices.getAuditor();
        this.numFailureRetries = Transform.NUM_FAILURE_RETRIES_SETTING.get(settings);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(Transform.NUM_FAILURE_RETRIES_SETTING, this::setNumFailureRetries);
    }

    @Override
    public PersistentTasksCustomMetadata.Assignment getAssignment(
        TransformTaskParams params,
        Collection<DiscoveryNode> candidateNodes,
        ClusterState clusterState
    ) {
        if (TransformMetadata.getTransformMetadata(clusterState).isResetMode()) {
            return new PersistentTasksCustomMetadata.Assignment(
                null,
                "Transform task will not be assigned as a feature reset is in progress."
            );
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

        // see gh#48019 disable assignment if any node is using 7.2 or 7.3
        if (clusterState.getNodes().getMinNodeVersion().before(Version.V_7_4_0)) {
            String reason = "Not starting transform ["
                + params.getId()
                + "], "
                + "because cluster contains nodes with version older than 7.4.0";
            logger.debug(reason);
            return new PersistentTasksCustomMetadata.Assignment(null, reason);
        }

        DiscoveryNode discoveryNode = selectLeastLoadedNode(
            clusterState,
            candidateNodes,
            node -> node.getVersion().onOrAfter(Version.V_7_7_0)
                ? nodeCanRunThisTransform(node, params.getVersion(), params.requiresRemote(), null)
                : nodeCanRunThisTransformPre77(node, params.getVersion(), null)
        );

        if (discoveryNode == null) {
            Map<String, String> explainWhyAssignmentFailed = new TreeMap<>();
            for (DiscoveryNode node : clusterState.getNodes()) {
                if (node.getVersion().onOrAfter(Version.V_7_7_0)) {
                    nodeCanRunThisTransform(node, params.getVersion(), params.requiresRemote(), explainWhyAssignmentFailed);
                } else {
                    nodeCanRunThisTransformPre77(node, params.getVersion(), explainWhyAssignmentFailed);
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
            if (routingTable == null || routingTable.allPrimaryShardsActive() == false) {
                unavailableIndices.add(index);
            }
        }
        return unavailableIndices;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, @Nullable TransformTaskParams params, PersistentTaskState state) {
        final String transformId = params.getId();
        final TransformTask buildTask = (TransformTask) task;
        // NOTE: TransformPersistentTasksExecutor#createTask pulls in the stored task state from the ClusterState when the object
        // is created. TransformTask#ctor takes into account setting the task as failed if that is passed in with the
        // persisted state.
        // TransformPersistentTasksExecutor#startTask will fail as TransformTask#start, when force == false, will return
        // a failure indicating that a failed task cannot be started.
        //
        // We want the rest of the state to be populated in the task when it is loaded on the node so that users can force start it again
        // later if they want.
        final ClientTransformIndexerBuilder indexerBuilder = new ClientTransformIndexerBuilder().setClient(buildTask.getParentTaskClient())
            .setTransformServices(transformServices);


        final SetOnce<TransformState> stateHolder = new SetOnce<>();

        ActionListener<StartTransformAction.Response> startTaskListener = ActionListener.wrap(
            response -> logger.info("[{}] successfully completed and scheduled task in node operation", transformId),
            failure -> {
                auditor.error(
                    transformId,
                    "Failed to start transform. " + "Please stop and attempt to start again. Failure: " + failure.getMessage()
                );
                logger.error("Failed to start task [" + transformId + "] in node operation", failure);
            }
        );

        // <6> load next checkpoint
        ActionListener<TransformCheckpoint> getTransformNextCheckpointListener = ActionListener.wrap(nextCheckpoint -> {

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

            startTask(buildTask, indexerBuilder, lastCheckpoint, startTaskListener);
        }, error -> {
            // TODO: do not use the same error message as for loading the last checkpoint
            String msg = TransformMessages.getMessage(TransformMessages.FAILED_TO_LOAD_TRANSFORM_CHECKPOINT, transformId);
            logger.error(msg, error);
            markAsFailed(buildTask, msg);
        });

        // <5> load last checkpoint
        ActionListener<TransformCheckpoint> getTransformLastCheckpointListener = ActionListener.wrap(lastCheckpoint -> {
            indexerBuilder.setLastCheckpoint(lastCheckpoint);

            logger.trace("[{}] Loaded last checkpoint [{}], looking for next checkpoint", transformId, lastCheckpoint.getCheckpoint());
            transformServices.getConfigManager()
                .getTransformCheckpoint(transformId, lastCheckpoint.getCheckpoint() + 1, getTransformNextCheckpointListener);
        }, error -> {
            String msg = TransformMessages.getMessage(TransformMessages.FAILED_TO_LOAD_TRANSFORM_CHECKPOINT, transformId);
            logger.error(msg, error);
            markAsFailed(buildTask, msg);
        });

        // <4> Set the previous stats (if they exist), initialize the indexer, start the task (If it is STOPPED)
        // Since we don't create the task until `_start` is called, if we see that the task state is stopped, attempt to start
        // Schedule execution regardless
        ActionListener<Tuple<TransformStoredDoc, SeqNoPrimaryTermAndIndex>> transformStatsActionListener = ActionListener.wrap(
            stateAndStatsAndSeqNoPrimaryTermAndIndex -> {
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
                    transformServices.getConfigManager()
                        .getTransformCheckpoint(transformId, lastCheckpoint + 1, getTransformNextCheckpointListener);
                } else {
                    logger.trace("[{}] Restore last checkpoint: [{}]", transformId, lastCheckpoint);
                    transformServices.getConfigManager()
                        .getTransformCheckpoint(transformId, lastCheckpoint, getTransformLastCheckpointListener);
                }
            },
            error -> {
                if (error instanceof ResourceNotFoundException == false) {
                    String msg = TransformMessages.getMessage(TransformMessages.FAILED_TO_LOAD_TRANSFORM_STATE, transformId);
                    logger.error(msg, error);
                    markAsFailed(buildTask, msg);
                } else {
                    logger.trace("[{}] No stats found (new transform), starting the task", transformId);
                    startTask(buildTask, indexerBuilder, null, startTaskListener);
                }
            }
        );

        // <3> Validate the transform, assigning it to the indexer, and get the previous stats (if they exist)
        ActionListener<TransformConfig> getTransformConfigListener = ActionListener.wrap(config -> {
            ValidationException validationException = config.validate(null);
            if (validationException == null) {
                indexerBuilder.setTransformConfig(config);
                transformServices.getConfigManager().getTransformStoredDoc(transformId, transformStatsActionListener);
            } else {
                auditor.error(transformId, validationException.getMessage());
                markAsFailed(
                    buildTask,
                    TransformMessages.getMessage(
                        TransformMessages.TRANSFORM_CONFIGURATION_INVALID,
                        transformId,
                        validationException.getMessage()
                    )
                );
            }
        }, error -> {
            String msg = TransformMessages.getMessage(TransformMessages.FAILED_TO_LOAD_TRANSFORM_CONFIGURATION, transformId);
            logger.error(msg, error);
            markAsFailed(buildTask, msg);
        });

        // <2> Get the transform config
        ActionListener<Void> templateCheckListener = ActionListener.wrap(
            aVoid -> transformServices.getConfigManager().getTransformConfiguration(transformId, getTransformConfigListener),
            error -> {
                Throwable cause = ExceptionsHelper.unwrapCause(error);
                String msg = "Failed to create internal index mappings";
                logger.error(msg, cause);
                markAsFailed(buildTask, msg + "[" + cause + "]");
            }
        );

        // <1> Check the index templates are installed
        TransformInternalIndex.ensureLatestIndexAndTemplateInstalled(
            clusterService,
            buildTask.getParentTaskClient(),
            templateCheckListener
        );
    }

    private static IndexerState currentIndexerState(TransformState previousState) {
        if (previousState == null) {
            return IndexerState.STOPPED;
        }
        switch (previousState.getIndexerState()) {
            // If it is STARTED or INDEXING we want to make sure we revert to started
            // Otherwise, the internal indexer will never get scheduled and execute
            case STARTED:
            case INDEXING:
                return IndexerState.STARTED;
            // If we are STOPPED, STOPPING, or ABORTING and just started executing on this node,
            // then it is safe to say we should be STOPPED
            case STOPPED:
            case STOPPING:
            case ABORTING:
            default:
                return IndexerState.STOPPED;
        }
    }

    private void markAsFailed(TransformTask task, String reason) {
        CountDownLatch latch = new CountDownLatch(1);

        task.fail(
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

    private void startTask(
        TransformTask buildTask,
        ClientTransformIndexerBuilder indexerBuilder,
        Long previousCheckpoint,
        ActionListener<StartTransformAction.Response> listener
    ) {
        buildTask.initializeIndexer(indexerBuilder);
        // TransformTask#start will fail if the task state is FAILED
        buildTask.setNumFailureRetries(numFailureRetries).start(previousCheckpoint, listener);
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
            client,
            persistentTask.getParams(),
            (TransformState) persistentTask.getState(),
            transformServices.getSchedulerEngine(),
            auditor,
            threadPool,
            headers
        );
    }
}