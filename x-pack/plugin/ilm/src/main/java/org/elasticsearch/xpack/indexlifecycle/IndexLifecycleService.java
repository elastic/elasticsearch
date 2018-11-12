/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleExecutionState;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.OperationMode;
import org.elasticsearch.xpack.core.indexlifecycle.ShrinkAction;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;

import java.io.Closeable;
import java.time.Clock;
import java.util.Collections;
import java.util.Set;
import java.util.function.LongSupplier;

/**
 * A service which runs the {@link LifecyclePolicy}s associated with indexes.
 */
public class IndexLifecycleService
        implements ClusterStateListener, ClusterStateApplier, SchedulerEngine.Listener, Closeable, LocalNodeMasterListener {
    private static final Logger logger = LogManager.getLogger(IndexLifecycleService.class);
    private static final Set<String> IGNORE_ACTIONS_MAINTENANCE_REQUESTED = Collections.singleton(ShrinkAction.NAME);
    private volatile boolean isMaster = false;
    private volatile TimeValue pollInterval;

    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private final Clock clock;
    private final PolicyStepsRegistry policyRegistry;
    private final IndexLifecycleRunner lifecycleRunner;
    private final Settings settings;
    private Client client;
    private ClusterService clusterService;
    private LongSupplier nowSupplier;
    private SchedulerEngine.Job scheduledJob;

    public IndexLifecycleService(Settings settings, Client client, ClusterService clusterService, Clock clock, LongSupplier nowSupplier,
                                 NamedXContentRegistry xContentRegistry) {
        super();
        this.settings = settings;
        this.client = client;
        this.clusterService = clusterService;
        this.clock = clock;
        this.nowSupplier = nowSupplier;
        this.scheduledJob = null;
        this.policyRegistry = new PolicyStepsRegistry(xContentRegistry, client);
        this.lifecycleRunner = new IndexLifecycleRunner(policyRegistry, clusterService, nowSupplier);
        this.pollInterval = LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING.get(settings);
        clusterService.addStateApplier(this);
        clusterService.addListener(this);
        clusterService.addLocalNodeMasterListener(this);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING,
            this::updatePollInterval);
    }

    public void maybeRunAsyncAction(ClusterState clusterState, IndexMetaData indexMetaData, StepKey nextStepKey) {
        String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexMetaData.getSettings());
        lifecycleRunner.maybeRunAsyncAction(clusterState, indexMetaData, policyName, nextStepKey);
    }

    public ClusterState moveClusterStateToStep(ClusterState currentState, String indexName, StepKey currentStepKey, StepKey nextStepKey) {
        return IndexLifecycleRunner.moveClusterStateToStep(indexName, currentState, currentStepKey, nextStepKey,
            nowSupplier, policyRegistry, false);
    }

    public ClusterState moveClusterStateToFailedStep(ClusterState currentState, String[] indices) {
        return lifecycleRunner.moveClusterStateToFailedStep(currentState, indices);
    }

    @Override
    public void onMaster() {
        this.isMaster = true;
        maybeScheduleJob();

        ClusterState clusterState = clusterService.state();
        IndexLifecycleMetadata currentMetadata = clusterState.metaData().custom(IndexLifecycleMetadata.TYPE);
        if (currentMetadata != null) {
            OperationMode currentMode = currentMetadata.getOperationMode();
            if (OperationMode.STOPPED.equals(currentMode)) {
                return;
            }

            boolean safeToStop = true; // true until proven false by a run policy

            // If we just became master, we need to kick off any async actions that
            // may have not been run due to master rollover
            for (ObjectCursor<IndexMetaData> cursor : clusterState.metaData().indices().values()) {
                IndexMetaData idxMeta = cursor.value;
                String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings());
                if (Strings.isNullOrEmpty(policyName) == false) {
                    StepKey stepKey = IndexLifecycleRunner.getCurrentStepKey(LifecycleExecutionState.fromIndexMetadata(idxMeta));
                    if (OperationMode.STOPPING == currentMode &&
                        stepKey != null &&
                        IGNORE_ACTIONS_MAINTENANCE_REQUESTED.contains(stepKey.getAction()) == false) {
                        logger.info("skipping policy [{}] for index [{}]. stopping Index Lifecycle execution",
                            policyName, idxMeta.getIndex().getName());
                        continue;
                    }
                    lifecycleRunner.maybeRunAsyncAction(clusterState, idxMeta, policyName, stepKey);
                    safeToStop = false; // proven false!
                }
            }
            if (safeToStop && OperationMode.STOPPING == currentMode) {
                submitOperationModeUpdate(OperationMode.STOPPED);
            }
        }
    }

    @Override
    public void offMaster() {
        this.isMaster = false;
        cancelJob();
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.MANAGEMENT;
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

    private void maybeScheduleJob() {
        if (this.isMaster) {
            if (scheduler.get() == null) {
                scheduler.set(new SchedulerEngine(settings, clock));
                scheduler.get().register(this);
            }
            scheduledJob = new SchedulerEngine.Job(XPackField.INDEX_LIFECYCLE, new TimeValueSchedule(pollInterval));
            scheduler.get().add(scheduledJob);
        }
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        IndexLifecycleMetadata lifecycleMetadata = event.state().metaData().custom(IndexLifecycleMetadata.TYPE);
        if (this.isMaster && lifecycleMetadata != null) {
            triggerPolicies(event.state(), true);
        }
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        if (event.localNodeMaster()) { // only act if we are master, otherwise
                                       // keep idle until elected
            if (event.state().metaData().custom(IndexLifecycleMetadata.TYPE) != null) {
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
        IndexLifecycleMetadata currentMetadata = clusterState.metaData().custom(IndexLifecycleMetadata.TYPE);

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
        for (ObjectCursor<IndexMetaData> cursor : clusterState.metaData().indices().values()) {
            IndexMetaData idxMeta = cursor.value;
            String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings());
            if (Strings.isNullOrEmpty(policyName) == false) {
                StepKey stepKey = IndexLifecycleRunner.getCurrentStepKey(LifecycleExecutionState.fromIndexMetadata(idxMeta));
                if (OperationMode.STOPPING == currentMode && stepKey != null
                        && IGNORE_ACTIONS_MAINTENANCE_REQUESTED.contains(stepKey.getAction()) == false) {
                    logger.info("skipping policy [" + policyName + "] for index [" + idxMeta.getIndex().getName()
                        + "]. stopping Index Lifecycle execution");
                    continue;
                }
                if (fromClusterStateChange) {
                    lifecycleRunner.runPolicyAfterStateChange(policyName, idxMeta);
                } else {
                    lifecycleRunner.runPeriodicStep(policyName, idxMeta);
                }
                safeToStop = false; // proven false!
            }
        }
        if (safeToStop && OperationMode.STOPPING == currentMode) {
            submitOperationModeUpdate(OperationMode.STOPPED);
        }
    }

    @Override
    public void close() {
        SchedulerEngine engine = scheduler.get();
        if (engine != null) {
            engine.stop();
        }
    }

    public void submitOperationModeUpdate(OperationMode mode) {
        clusterService.submitStateUpdateTask("ilm_operation_mode_update",
            new OperationModeUpdateTask(mode));
    }
}
