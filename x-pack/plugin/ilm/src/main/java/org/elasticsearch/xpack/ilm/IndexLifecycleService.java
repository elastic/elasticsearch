/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ilm;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.Lifecycle.State;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.ilm.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.ilm.LifecycleExecutionState;
import org.elasticsearch.xpack.core.ilm.LifecyclePolicy;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.OperationMode;
import org.elasticsearch.xpack.core.ilm.ShrinkStep;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.ilm.history.ILMHistoryStore;

import java.io.Closeable;
import java.time.Clock;
import java.util.Collections;
import java.util.Set;
import java.util.function.LongSupplier;

import static org.elasticsearch.xpack.core.ilm.IndexLifecycleOriginationDateParser.parseIndexNameAndExtractDate;
import static org.elasticsearch.xpack.core.ilm.IndexLifecycleOriginationDateParser.shouldParseIndexName;

/**
 * A service which runs the {@link LifecyclePolicy}s associated with indexes.
 */
public class IndexLifecycleService
    implements ClusterStateListener, ClusterStateApplier, SchedulerEngine.Listener, Closeable, IndexEventListener {
    private static final Logger logger = LogManager.getLogger(IndexLifecycleService.class);
    private static final Set<String> IGNORE_STEPS_MAINTENANCE_REQUESTED = Collections.singleton(ShrinkStep.NAME);
    private volatile boolean isMaster = false;
    private volatile TimeValue pollInterval;

    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private final Clock clock;
    private final PolicyStepsRegistry policyRegistry;
    private final IndexLifecycleRunner lifecycleRunner;
    private final ILMHistoryStore ilmHistoryStore;
    private final Settings settings;
    private ClusterService clusterService;
    private LongSupplier nowSupplier;
    private SchedulerEngine.Job scheduledJob;

    public IndexLifecycleService(Settings settings, Client client, ClusterService clusterService, ThreadPool threadPool, Clock clock,
                                 LongSupplier nowSupplier, NamedXContentRegistry xContentRegistry,
                                 ILMHistoryStore ilmHistoryStore) {
        super();
        this.settings = settings;
        this.clusterService = clusterService;
        this.clock = clock;
        this.nowSupplier = nowSupplier;
        this.scheduledJob = null;
        this.ilmHistoryStore = ilmHistoryStore;
        this.policyRegistry = new PolicyStepsRegistry(xContentRegistry, client);
        this.lifecycleRunner = new IndexLifecycleRunner(policyRegistry, ilmHistoryStore, clusterService, threadPool, nowSupplier);
        this.pollInterval = LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING.get(settings);
        clusterService.addStateApplier(this);
        clusterService.addListener(this);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING,
            this::updatePollInterval);
    }

    public void maybeRunAsyncAction(ClusterState clusterState, IndexMetadata indexMetadata, StepKey nextStepKey) {
        String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexMetadata.getSettings());
        lifecycleRunner.maybeRunAsyncAction(clusterState, indexMetadata, policyName, nextStepKey);
    }

    /**
     * Move the cluster state to an arbitrary step for the provided index.
     *
     * In order to avoid a check-then-set race condition, the current step key
     * is required in order to validate that the index is currently on the
     * provided step. If it is not, an {@link IllegalArgumentException} is
     * thrown.
     * @throws IllegalArgumentException if the step movement cannot be validated
     */
    public ClusterState moveClusterStateToStep(ClusterState currentState, Index index, StepKey currentStepKey, StepKey newStepKey) {
        // We manually validate here, because any API must correctly specify the current step key
        // when moving to an arbitrary step key (to avoid race conditions between the
        // check-and-set). moveClusterStateToStep also does its own validation, but doesn't take
        // the user-input for the current step (which is why we validate here for a passed in step)
        IndexLifecycleTransition.validateTransition(currentState.getMetadata().index(index),
            currentStepKey, newStepKey, policyRegistry);
        return IndexLifecycleTransition.moveClusterStateToStep(index, currentState, newStepKey,
            nowSupplier, policyRegistry, true);
    }

    public ClusterState moveClusterStateToPreviouslyFailedStep(ClusterState currentState, String[] indices) {
        ClusterState newState = currentState;
        for (String index : indices) {
            newState = IndexLifecycleTransition.moveClusterStateToPreviouslyFailedStep(newState, index,
                nowSupplier, policyRegistry, false);
        }
        return newState;
    }

    // package private for testing
    void onMaster(ClusterState clusterState) {
        maybeScheduleJob();

        final IndexLifecycleMetadata currentMetadata = clusterState.metadata().custom(IndexLifecycleMetadata.TYPE);
        if (currentMetadata != null) {
            OperationMode currentMode = currentMetadata.getOperationMode();
            if (OperationMode.STOPPED.equals(currentMode)) {
                return;
            }

            boolean safeToStop = true; // true until proven false by a run policy

            // If we just became master, we need to kick off any async actions that
            // may have not been run due to master rollover
            for (ObjectCursor<IndexMetadata> cursor : clusterState.metadata().indices().values()) {
                IndexMetadata idxMeta = cursor.value;
                String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings());
                if (Strings.isNullOrEmpty(policyName) == false) {
                    final LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(idxMeta);
                    StepKey stepKey = LifecycleExecutionState.getCurrentStepKey(lifecycleState);

                    try {
                        if (OperationMode.STOPPING == currentMode) {
                            if (stepKey != null && IGNORE_STEPS_MAINTENANCE_REQUESTED.contains(stepKey.getName())) {
                                logger.info("waiting to stop ILM because index [{}] with policy [{}] is currently in step [{}]",
                                    idxMeta.getIndex().getName(), policyName, stepKey.getName());
                                lifecycleRunner.maybeRunAsyncAction(clusterState, idxMeta, policyName, stepKey);
                                // ILM is trying to stop, but this index is in a Shrink step (or other dangerous step) so we can't stop
                                safeToStop = false;
                            } else {
                                logger.info("skipping policy execution of step [{}] for index [{}] with policy [{}]" +
                                        " because ILM is stopping",
                                    stepKey == null ? "n/a" : stepKey.getName(), idxMeta.getIndex().getName(), policyName);
                            }
                        } else {
                            lifecycleRunner.maybeRunAsyncAction(clusterState, idxMeta, policyName, stepKey);
                        }
                    } catch (Exception e) {
                        if (logger.isTraceEnabled()) {
                            logger.warn(new ParameterizedMessage("async action execution failed during master election trigger" +
                                " for index [{}] with policy [{}] in step [{}], lifecycle state: [{}]",
                                idxMeta.getIndex().getName(), policyName, stepKey, lifecycleState.asMap()), e);
                        } else {
                            logger.warn(new ParameterizedMessage("async action execution failed during master election trigger" +
                                " for index [{}] with policy [{}] in step [{}]",
                                idxMeta.getIndex().getName(), policyName, stepKey), e);

                        }
                        // Don't rethrow the exception, we don't want a failure for one index to be
                        // called to cause actions not to be triggered for further indices
                    }
                }
            }

            if (safeToStop && OperationMode.STOPPING == currentMode) {
                submitOperationModeUpdate(OperationMode.STOPPED);
            }
        }
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
        // Instead of using a LocalNodeMasterListener to track master changes, this service will
        // track them here to avoid conditions where master listener events run after other
        // listeners that depend on what happened in the master listener
        final boolean prevIsMaster = this.isMaster;
        if (prevIsMaster != event.localNodeMaster()) {
            this.isMaster = event.localNodeMaster();
            if (this.isMaster) {
                onMaster(event.state());
            } else {
                cancelJob();
            }
        }

        final IndexLifecycleMetadata lifecycleMetadata = event.state().metadata().custom(IndexLifecycleMetadata.TYPE);
        if (this.isMaster && lifecycleMetadata != null) {
            triggerPolicies(event.state(), true);
        }
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        if (event.localNodeMaster()) { // only act if we are master, otherwise
            // keep idle until elected
            if (event.state().metadata().custom(IndexLifecycleMetadata.TYPE) != null) {
                policyRegistry.update(event.state());
            }
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
        if (event.getJobName().equals(XPackField.INDEX_LIFECYCLE)) {
            logger.trace("job triggered: " + event.getJobName() + ", " + event.getScheduledTime() + ", " + event.getTriggeredTime());
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
    void triggerPolicies(ClusterState clusterState, boolean fromClusterStateChange) {
        IndexLifecycleMetadata currentMetadata = clusterState.metadata().custom(IndexLifecycleMetadata.TYPE);

        if (currentMetadata == null) {
            return;
        }

        OperationMode currentMode = currentMetadata.getOperationMode();

        if (OperationMode.STOPPED.equals(currentMode)) {
            return;
        }

        boolean safeToStop = true; // true until proven false by a run policy

        // loop through all indices in cluster state and filter for ones that are
        // managed by the Index Lifecycle Service they have a index.lifecycle.name setting
        // associated to a policy
        for (ObjectCursor<IndexMetadata> cursor : clusterState.metadata().indices().values()) {
            IndexMetadata idxMeta = cursor.value;
            String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings());
            if (Strings.isNullOrEmpty(policyName) == false) {
                final LifecycleExecutionState lifecycleState = LifecycleExecutionState.fromIndexMetadata(idxMeta);
                StepKey stepKey = LifecycleExecutionState.getCurrentStepKey(lifecycleState);

                try {
                    if (OperationMode.STOPPING == currentMode) {
                        if (stepKey != null && IGNORE_STEPS_MAINTENANCE_REQUESTED.contains(stepKey.getName())) {
                            logger.info("waiting to stop ILM because index [{}] with policy [{}] is currently in step [{}]",
                                idxMeta.getIndex().getName(), policyName, stepKey.getName());
                            if (fromClusterStateChange) {
                                lifecycleRunner.runPolicyAfterStateChange(policyName, idxMeta);
                            } else {
                                lifecycleRunner.runPeriodicStep(policyName, clusterState.metadata(), idxMeta);
                            }
                            // ILM is trying to stop, but this index is in a Shrink step (or other dangerous step) so we can't stop
                            safeToStop = false;
                        } else {
                            logger.info("skipping policy execution of step [{}] for index [{}] with policy [{}] because ILM is stopping",
                                stepKey == null ? "n/a" : stepKey.getName(), idxMeta.getIndex().getName(), policyName);
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
                        logger.warn(new ParameterizedMessage("async action execution failed during policy trigger" +
                            " for index [{}] with policy [{}] in step [{}], lifecycle state: [{}]",
                            idxMeta.getIndex().getName(), policyName, stepKey, lifecycleState.asMap()), e);
                    } else {
                        logger.warn(new ParameterizedMessage("async action execution failed during policy trigger" +
                            " for index [{}] with policy [{}] in step [{}]",
                            idxMeta.getIndex().getName(), policyName, stepKey), e);

                    }
                    // Don't rethrow the exception, we don't want a failure for one index to be
                    // called to cause actions not to be triggered for further indices
                }
            }
        }

        if (safeToStop && OperationMode.STOPPING == currentMode) {
            submitOperationModeUpdate(OperationMode.STOPPED);
        }
    }

    @Override
    public synchronized void close() {
        // this assertion is here to ensure that the check we use in maybeScheduleJob is accurate for detecting a shutdown in
        // progress, which is that the cluster service is stopped and closed at some point prior to closing plugins
        assert isClusterServiceStoppedOrClosed() : "close is called by closing the plugin, which is expected to happen after " +
            "the cluster service is stopped";
        SchedulerEngine engine = scheduler.get();
        if (engine != null) {
            engine.stop();
        }
    }

    public void submitOperationModeUpdate(OperationMode mode) {
        OperationModeUpdateTask ilmOperationModeUpdateTask;
        if (mode == OperationMode.STOPPING || mode == OperationMode.STOPPED) {
            ilmOperationModeUpdateTask = OperationModeUpdateTask.ilmMode(Priority.IMMEDIATE, mode);
        } else {
            ilmOperationModeUpdateTask = OperationModeUpdateTask.ilmMode(Priority.NORMAL, mode);
        }
        clusterService.submitStateUpdateTask("ilm_operation_mode_update {OperationMode " + mode.name() + "}", ilmOperationModeUpdateTask);
    }

    /**
     * Method that checks if the lifecycle state of the cluster service is stopped or closed. This
     * enhances the readability of the code.
     */
    private boolean isClusterServiceStoppedOrClosed() {
        final State state = clusterService.lifecycleState();
        return state == State.STOPPED || state == State.CLOSED;
    }
}
