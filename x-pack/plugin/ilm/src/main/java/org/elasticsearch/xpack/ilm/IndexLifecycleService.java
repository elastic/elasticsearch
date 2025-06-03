/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.Lifecycle.State;
import org.elasticsearch.common.scheduler.SchedulerEngine;
import org.elasticsearch.common.scheduler.TimeValueSchedule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ShutdownAwarePlugin;
import org.elasticsearch.shutdown.PluginShutdownService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ilm.CheckShrinkReadyStep;
import org.elasticsearch.xpack.core.ilm.DownsampleStep;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.OperationModeUpdateTask;
import org.elasticsearch.xpack.core.ilm.SetSingleNodeAllocateStep;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.ShrinkStep;
import org.elasticsearch.xpack.core.ilm.ShrunkShardsAllocatedStep;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.ilm.history.ILMHistoryStore;

import java.io.Closeable;
import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.ilm.IndexLifecycleOriginationDateParser.parseIndexNameAndExtractDate;
import static org.elasticsearch.xpack.core.ilm.IndexLifecycleOriginationDateParser.shouldParseIndexName;
import static org.elasticsearch.xpack.core.ilm.LifecycleOperationMetadata.currentILMMode;

/**
 * A service which runs the {@link LifecyclePolicy}s associated with indexes.
 */
public class IndexLifecycleService
    implements
        ClusterStateListener,
        ClusterStateApplier,
        SchedulerEngine.Listener,
        Closeable,
        IndexEventListener,
        ShutdownAwarePlugin {
    private static final Logger logger = LogManager.getLogger(IndexLifecycleService.class);
    private static final Set<String> IGNORE_STEPS_MAINTENANCE_REQUESTED = Set.of(ShrinkStep.NAME, DownsampleStep.NAME);
    private volatile boolean isMaster = false;
    private volatile TimeValue pollInterval;

    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private final Clock clock;
    private final PolicyStepsRegistry policyRegistry;
    private final IndexLifecycleRunner lifecycleRunner;
    private final Settings settings;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final LongSupplier nowSupplier;
    private final ExecutorService managementExecutor;
    /** A reference to the last seen cluster state. If it's not null, we're currently processing a cluster state. */
    private final AtomicReference<ClusterState> lastSeenState = new AtomicReference<>();

    private SchedulerEngine.Job scheduledJob;

    @SuppressWarnings("this-escape")
    public IndexLifecycleService(
        Settings settings,
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        Clock clock,
        LongSupplier nowSupplier,
        NamedXContentRegistry xContentRegistry,
        ILMHistoryStore ilmHistoryStore,
        XPackLicenseState licenseState
    ) {
        super();
        this.settings = settings;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.clock = clock;
        this.nowSupplier = nowSupplier;
        this.scheduledJob = null;
        this.policyRegistry = new PolicyStepsRegistry(xContentRegistry, client, licenseState);
        this.lifecycleRunner = new IndexLifecycleRunner(policyRegistry, ilmHistoryStore, clusterService, threadPool, nowSupplier);
        this.pollInterval = LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING.get(settings);
        this.managementExecutor = threadPool.executor(ThreadPool.Names.MANAGEMENT);
        clusterService.addStateApplier(this);
        clusterService.addListener(this);
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING, this::updatePollInterval);
    }

    public void maybeRunAsyncAction(ClusterState clusterState, IndexMetadata indexMetadata, StepKey nextStepKey) {
        lifecycleRunner.maybeRunAsyncAction(clusterState, indexMetadata, indexMetadata.getLifecyclePolicyName(), nextStepKey);
    }

    /**
     * Resolve the given phase, action, and name into a real {@link StepKey}. The phase is always
     * required, but the action and name are optional. If a name is specified, an action is also required.
     */
    public StepKey resolveStepKey(ClusterState state, Index index, String phase, @Nullable String action, @Nullable String name) {
        if (name == null) {
            if (action == null) {
                return this.policyRegistry.getFirstStepForPhase(state, index, phase);
            } else {
                return this.policyRegistry.getFirstStepForPhaseAndAction(state, index, phase, action);
            }
        } else {
            assert action != null
                : "action should never be null because we don't allow constructing a partial step key with only a phase and name";
            return new StepKey(phase, action, name);
        }
    }

    /**
     * Move the project to an arbitrary step for the provided index.
     *
     * In order to avoid a check-then-set race condition, the current step key
     * is required in order to validate that the index is currently on the
     * provided step. If it is not, an {@link IllegalArgumentException} is
     * thrown.
     * @throws IllegalArgumentException if the step movement cannot be validated
     */
    public ProjectMetadata moveProjectToStep(ProjectMetadata project, Index index, StepKey currentStepKey, StepKey newStepKey) {
        // We manually validate here, because any API must correctly specify the current step key
        // when moving to an arbitrary step key (to avoid race conditions between the
        // check-and-set). moveProjectToStep also does its own validation, but doesn't take
        // the user-input for the current step (which is why we validate here for a passed in step)
        IndexLifecycleTransition.validateTransition(project.index(index), currentStepKey, newStepKey, policyRegistry);
        return IndexLifecycleTransition.moveProjectToStep(index, project, newStepKey, nowSupplier, policyRegistry, true);
    }

    public ClusterState moveClusterStateToPreviouslyFailedStep(ClusterState currentState, String[] indices) {
        ClusterState newState = currentState;
        for (String index : indices) {
            newState = IndexLifecycleTransition.moveClusterStateToPreviouslyFailedStep(newState, index, nowSupplier, policyRegistry, false);
        }
        return newState;
    }

    // package private for testing
    void onMaster(ClusterState clusterState) {
        maybeScheduleJob();

        // TODO multi-project: this probably needs a per-project iteration
        @FixForMultiProject
        final ProjectMetadata projectMetadata = clusterState.metadata().getProject(Metadata.DEFAULT_PROJECT_ID);
        final IndexLifecycleMetadata currentMetadata = projectMetadata.custom(IndexLifecycleMetadata.TYPE);
        if (currentMetadata != null) {
            OperationMode currentMode = currentILMMode(projectMetadata);
            if (OperationMode.STOPPED.equals(currentMode)) {
                return;
            }

            boolean safeToStop = true; // true until proven false by a run policy

            // If we just became master, we need to kick off any async actions that
            // may have not been run due to master rollover
            for (IndexMetadata idxMeta : projectMetadata.indices().values()) {
                if (projectMetadata.isIndexManagedByILM(idxMeta)) {
                    String policyName = idxMeta.getLifecyclePolicyName();
                    final LifecycleExecutionState lifecycleState = idxMeta.getLifecycleExecutionState();
                    StepKey stepKey = Step.getCurrentStepKey(lifecycleState);

                    try {
                        if (OperationMode.STOPPING == currentMode) {
                            if (stepKey != null && IGNORE_STEPS_MAINTENANCE_REQUESTED.contains(stepKey.name())) {
                                logger.info(
                                    "waiting to stop ILM because index [{}] with policy [{}] is currently in step [{}]",
                                    idxMeta.getIndex().getName(),
                                    policyName,
                                    stepKey.name()
                                );
                                lifecycleRunner.maybeRunAsyncAction(clusterState, idxMeta, policyName, stepKey);
                                // ILM is trying to stop, but this index is in a Shrink step (or other dangerous step) so we can't stop
                                safeToStop = false;
                            } else {
                                logger.info(
                                    "skipping policy execution of step [{}] for index [{}] with policy [{}]" + " because ILM is stopping",
                                    stepKey == null ? "n/a" : stepKey.name(),
                                    idxMeta.getIndex().getName(),
                                    policyName
                                );
                            }
                        } else {
                            lifecycleRunner.maybeRunAsyncAction(clusterState, idxMeta, policyName, stepKey);
                        }
                    } catch (Exception e) {
                        if (logger.isTraceEnabled()) {
                            logger.warn(
                                () -> format(
                                    "async action execution failed during master election trigger"
                                        + " for index [%s] with policy [%s] in step [%s], lifecycle state: [%s]",
                                    idxMeta.getIndex().getName(),
                                    policyName,
                                    stepKey,
                                    lifecycleState.asMap()
                                ),
                                e
                            );
                        } else {
                            logger.warn(
                                () -> format(
                                    "async action execution failed during master election trigger"
                                        + " for index [%s] with policy [%s] in step [%s]",
                                    idxMeta.getIndex().getName(),
                                    policyName,
                                    stepKey
                                ),
                                e
                            );

                        }
                        // Don't rethrow the exception, we don't want a failure for one index to be
                        // called to cause actions not to be triggered for further indices
                    }
                }
            }

            if (safeToStop && OperationMode.STOPPING == currentMode) {
                stopILM();
            }
        }
    }

    private void stopILM() {
        submitUnbatchedTask("ilm_operation_mode_update[stopped]", OperationModeUpdateTask.ilmMode(OperationMode.STOPPED));
    }

    @Override
    public void beforeIndexAddedToCluster(Index index, Settings indexSettings) {
        if (shouldParseIndexName(indexSettings)) {
            parseIndexNameAndExtractDate(index.getName());
        }
    }

    private void updatePollInterval(TimeValue newInterval) {
        this.pollInterval = newInterval;
        maybeScheduleJob();
    }

    // pkg-private for testing
    SchedulerEngine getScheduler() {
        return scheduler.get();
    }

    // pkg-private for testing
    SchedulerEngine.Job getScheduledJob() {
        return scheduledJob;
    }

    private synchronized void maybeScheduleJob() {
        if (this.isMaster) {
            if (scheduler.get() == null) {
                // don't create scheduler if the node is shutting down
                if (isClusterServiceStoppedOrClosed() == false) {
                    scheduler.set(new SchedulerEngine(settings, clock));
                    scheduler.get().register(this);
                }
            }

            // scheduler could be null if the node might be shutting down
            if (scheduler.get() != null) {
                scheduledJob = new SchedulerEngine.Job(XPackField.INDEX_LIFECYCLE, new TimeValueSchedule(pollInterval));
                scheduler.get().add(scheduledJob);
            }
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // wait for the cluster state to be recovered so the ILM policies are present
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        // Instead of using a LocalNodeMasterListener to track master changes, this service will
        // track them here to avoid conditions where master listener events run after other
        // listeners that depend on what happened in the master listener
        final boolean prevIsMaster = this.isMaster;
        if (prevIsMaster != event.localNodeMaster()) {
            this.isMaster = event.localNodeMaster();
            if (this.isMaster) {
                // we weren't the master, and now we are
                onMaster(event.state());
            } else {
                // we were the master, and now we aren't
                cancelJob();
                policyRegistry.clear();
            }
        }

        // if we're the master, then process deleted indices and trigger policies
        if (this.isMaster) {
            // cleanup cache in policyRegistry on another thread since its not critical to have it run on the applier thread and computing
            // the deleted indices becomes expensive for larger cluster states
            // ClusterChangedEvent.indicesDeleted uses an equality check to skip computation if necessary.
            final List<Index> indicesDeleted = event.indicesDeleted();
            if (indicesDeleted.isEmpty() == false) {
                managementExecutor.execute(() -> {
                    for (Index index : indicesDeleted) {
                        policyRegistry.delete(index);
                    }
                });
            }

            // Only start processing the new cluster state if we're not already processing one.
            // Note that we might override the last seen state with a new one, even if the previous one hasn't been processed yet.
            // This means that when ILM's cluster state processing takes longer than the overall cluster state application or when
            // the forked thread is waiting in the thread pool queue (e.g. when the master node is swamped), we might skip some
            // cluster state updates. Since ILM does not depend on "deltas" in cluster states, we can skip some cluster states just fine.
            if (lastSeenState.getAndSet(event.state()) == null) {
                processClusterState();
            } else {
                logger.trace("ILM state processor still running, not starting new thread");
            }
        }
    }

    /**
     * Instead of processing cluster state updates on the cluster state applier thread, we fork to a different thread where
     * ILM's runtime of processing the cluster state update does not affect the speed at which the cluster can apply new cluster states.
     * That does not mean we don't need to optimize ILM's cluster state processing, as the overall amount of processing is generally
     * unaffected by this fork approach (unless we skip some cluster states), but it does mean we're saving a significant amount
     * of processing on the critical cluster state applier thread.
     */
    private void processClusterState() {
        managementExecutor.execute(new AbstractRunnable() {

            private final SetOnce<ClusterState> currentState = new SetOnce<>();

            @Override
            protected void doRun() throws Exception {
                final ClusterState currentState = lastSeenState.get();
                // This should never be null, but we're checking anyway to be sure.
                if (currentState == null) {
                    assert false : "Expected current state to non-null when processing cluster state in ILM";
                    return;
                }
                this.currentState.set(currentState);
                triggerPolicies(currentState, true);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("ILM failed to process cluster state", e);
            }

            @Override
            public void onAfter() {
                // If the last seen state is unchanged, we set it to null to indicate that processing has finished and we return.
                if (lastSeenState.compareAndSet(currentState.get(), null)) {
                    return;
                }
                // If the last seen cluster state changed while this thread was running, it means a new cluster state came in and we need to
                // process it. We do that by kicking off a new thread, which will pick up the new cluster state when the thread gets
                // executed.
                processClusterState();
            }

            @Override
            public boolean isForceExecution() {
                // Without force execution, we risk ILM state processing being postponed arbitrarily long, which in turn could cause
                // thundering herd issues if there's significant time between ILM runs.
                return true;
            }
        });
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        // only act if we are master, otherwise keep idle until elected
        if (event.localNodeMaster() == false) {
            return;
        }

        @FixForMultiProject
        final IndexLifecycleMetadata ilmMetadata = event.state()
            .metadata()
            .getProject(Metadata.DEFAULT_PROJECT_ID)
            .custom(IndexLifecycleMetadata.TYPE);
        if (ilmMetadata == null) {
            return;
        }
        final IndexLifecycleMetadata previousIlmMetadata = event.previousState()
            .metadata()
            .getProject(Metadata.DEFAULT_PROJECT_ID)
            .custom(IndexLifecycleMetadata.TYPE);
        if (event.previousState().nodes().isLocalNodeElectedMaster() == false || ilmMetadata != previousIlmMetadata) {
            policyRegistry.update(ilmMetadata);
        }
    }

    private void cancelJob() {
        if (scheduler.get() != null) {
            scheduler.get().remove(XPackField.INDEX_LIFECYCLE);
            scheduledJob = null;
        }
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        if (event.jobName().equals(XPackField.INDEX_LIFECYCLE)) {
            logger.trace("job triggered: {}, {}, {}", event.jobName(), event.scheduledTime(), event.triggeredTime());
            triggerPolicies(clusterService.state(), false);
        }
    }

    public boolean policyExists(String policyId) {
        return policyRegistry.policyExists(policyId);
    }

    /**
     * executes the policy execution on the appropriate indices by running cluster-state tasks per index.
     *
     * If stopping ILM was requested, and it is safe to stop, this will also be done here
     * when possible after no policies are executed.
     *
     * @param clusterState the current cluster state
     * @param fromClusterStateChange whether things are triggered from the cluster-state-listener or the scheduler
     */
    @FixForMultiProject
    void triggerPolicies(ClusterState clusterState, boolean fromClusterStateChange) {
        @FixForMultiProject
        final var projectMetadata = clusterState.metadata().getProject(Metadata.DEFAULT_PROJECT_ID);
        IndexLifecycleMetadata currentMetadata = projectMetadata.custom(IndexLifecycleMetadata.TYPE);

        OperationMode currentMode = currentILMMode(projectMetadata);
        if (currentMetadata == null) {
            if (currentMode == OperationMode.STOPPING) {
                // There are no policies and ILM is in stopping mode, so stop ILM and get out of here
                stopILM();
            }
            return;
        }

        if (OperationMode.STOPPED.equals(currentMode)) {
            return;
        }

        boolean safeToStop = true; // true until proven false by a run policy

        // loop through all indices in cluster state and filter for ones that are
        // managed by the Index Lifecycle Service they have a index.lifecycle.name setting
        // associated to a policy
        for (IndexMetadata idxMeta : projectMetadata.indices().values()) {
            if (projectMetadata.isIndexManagedByILM(idxMeta)) {
                String policyName = idxMeta.getLifecyclePolicyName();
                final LifecycleExecutionState lifecycleState = idxMeta.getLifecycleExecutionState();
                StepKey stepKey = Step.getCurrentStepKey(lifecycleState);

                try {
                    if (OperationMode.STOPPING == currentMode) {
                        if (stepKey != null && IGNORE_STEPS_MAINTENANCE_REQUESTED.contains(stepKey.name())) {
                            logger.info(
                                "waiting to stop ILM because index [{}] with policy [{}] is currently in step [{}]",
                                idxMeta.getIndex().getName(),
                                policyName,
                                stepKey.name()
                            );
                            if (fromClusterStateChange) {
                                lifecycleRunner.runPolicyAfterStateChange(policyName, idxMeta);
                            } else {
                                lifecycleRunner.runPeriodicStep(policyName, clusterState.metadata(), idxMeta);
                            }
                            // ILM is trying to stop, but this index is in a Shrink step (or other dangerous step) so we can't stop
                            safeToStop = false;
                        } else {
                            logger.info(
                                "skipping policy execution of step [{}] for index [{}] with policy [{}] because ILM is stopping",
                                stepKey == null ? "n/a" : stepKey.name(),
                                idxMeta.getIndex().getName(),
                                policyName
                            );
                        }
                    } else {
                        if (fromClusterStateChange) {
                            lifecycleRunner.runPolicyAfterStateChange(policyName, idxMeta);
                        } else {
                            lifecycleRunner.runPeriodicStep(policyName, clusterState.metadata(), idxMeta);
                        }
                    }
                } catch (Exception e) {
                    if (logger.isTraceEnabled()) {
                        logger.warn(
                            () -> format(
                                "async action execution failed during policy trigger"
                                    + " for index [%s] with policy [%s] in step [%s], lifecycle state: [%s]",
                                idxMeta.getIndex().getName(),
                                policyName,
                                stepKey,
                                lifecycleState.asMap()
                            ),
                            e
                        );
                    } else {
                        logger.warn(
                            () -> format(
                                "async action execution failed during policy trigger" + " for index [%s] with policy [%s] in step [%s]",
                                idxMeta.getIndex().getName(),
                                policyName,
                                stepKey
                            ),
                            e
                        );

                    }
                    // Don't rethrow the exception, we don't want a failure for one index to be
                    // called to cause actions not to be triggered for further indices
                }
            }
        }

        if (safeToStop && OperationMode.STOPPING == currentMode) {
            stopILM();
        }
    }

    @Override
    public synchronized void close() {
        // this assertion is here to ensure that the check we use in maybeScheduleJob is accurate for detecting a shutdown in
        // progress, which is that the cluster service is stopped and closed at some point prior to closing plugins
        assert isClusterServiceStoppedOrClosed()
            : "close is called by closing the plugin, which is expected to happen after " + "the cluster service is stopped";
        SchedulerEngine engine = scheduler.get();
        if (engine != null) {
            engine.stop();
        }
    }

    /**
     * Method that checks if the lifecycle state of the cluster service is stopped or closed. This
     * enhances the readability of the code.
     */
    private boolean isClusterServiceStoppedOrClosed() {
        final State state = clusterService.lifecycleState();
        return state == State.STOPPED || state == State.CLOSED;
    }

    // visible for testing
    PolicyStepsRegistry getPolicyRegistry() {
        return policyRegistry;
    }

    static Set<String> indicesOnShuttingDownNodesInDangerousStep(ClusterState state, String nodeId) {
        final Set<String> shutdownNodes = PluginShutdownService.shutdownTypeNodes(
            state,
            SingleNodeShutdownMetadata.Type.REMOVE,
            SingleNodeShutdownMetadata.Type.SIGTERM,
            SingleNodeShutdownMetadata.Type.REPLACE
        );
        if (shutdownNodes.isEmpty()) {
            return Set.of();
        }

        // Returning a set of strings will cause weird behavior with multiple projects
        @FixForMultiProject
        Set<String> indicesPreventingShutdown = state.metadata()
            .projects()
            .values()
            .stream()
            .flatMap(project -> project.indices().entrySet().stream())
            // Filter out to only consider managed indices
            .filter(indexToMetadata -> Strings.hasText(indexToMetadata.getValue().getLifecyclePolicyName()))
            // Only look at indices in the shrink action
            .filter(indexToMetadata -> ShrinkAction.NAME.equals(indexToMetadata.getValue().getLifecycleExecutionState().action()))
            // Only look at indices on a step that may potentially be dangerous if we removed the node
            .filter(indexToMetadata -> {
                String step = indexToMetadata.getValue().getLifecycleExecutionState().step();
                return SetSingleNodeAllocateStep.NAME.equals(step)
                    || CheckShrinkReadyStep.NAME.equals(step)
                    || ShrinkStep.NAME.equals(step)
                    || ShrunkShardsAllocatedStep.NAME.equals(step);
            })
            // Only look at indices where the node picked for the shrink is the node marked as shutting down
            .filter(indexToMetadata -> {
                String nodePicked = indexToMetadata.getValue()
                    .getSettings()
                    .get(IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_id");
                return nodeId.equals(nodePicked);
            })
            .map(Map.Entry::getKey)
            .collect(Collectors.toSet());
        logger.trace(
            "with nodes marked as shutdown for removal {}, indices {} are preventing shutdown",
            shutdownNodes,
            indicesPreventingShutdown
        );
        return indicesPreventingShutdown;
    }

    @Override
    public boolean safeToShutdown(String nodeId, SingleNodeShutdownMetadata.Type shutdownType) {
        switch (shutdownType) {
            case RESTART:
                // It is safe to restart during ILM operation
                return true;
            case REPLACE:
            case REMOVE:
            case SIGTERM:
                Set<String> indices = indicesOnShuttingDownNodesInDangerousStep(clusterService.state(), nodeId);
                return indices.isEmpty();
            default:
                throw new IllegalArgumentException("unknown shutdown type: " + shutdownType);
        }
    }

    @Override
    public void signalShutdown(Collection<String> shutdownNodeIds) {
        // TODO: in the future we could take proactive measures for when a shutdown is actually triggered
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
    }
}
