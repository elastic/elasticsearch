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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.protocol.xpack.indexlifecycle.OperationMode;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
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
public class IndexLifecycleService extends AbstractComponent
        implements ClusterStateListener, ClusterStateApplier, SchedulerEngine.Listener, Closeable {
    private static final Logger logger = LogManager.getLogger(IndexLifecycleService.class);
    private static final Set<String> IGNORE_ACTIONS_MAINTENANCE_REQUESTED = Collections.singleton(ShrinkAction.NAME);

    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private final Clock clock;
    private final PolicyStepsRegistry policyRegistry;
    private final IndexLifecycleRunner lifecycleRunner;
    private Client client;
    private ClusterService clusterService;
    private LongSupplier nowSupplier;
    private SchedulerEngine.Job scheduledJob;

    public IndexLifecycleService(Settings settings, Client client, ClusterService clusterService, Clock clock, LongSupplier nowSupplier) {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
        this.clock = clock;
        this.nowSupplier = nowSupplier;
        this.scheduledJob = null;
        this.policyRegistry = new PolicyStepsRegistry();
        this.lifecycleRunner = new IndexLifecycleRunner(policyRegistry, clusterService, nowSupplier);
        clusterService.addStateApplier(this);
        clusterService.addListener(this);
    }

    public ClusterState moveClusterStateToStep(ClusterState currentState, String indexName, StepKey currentStepKey, StepKey nextStepKey) {
        return IndexLifecycleRunner.moveClusterStateToStep(indexName, currentState, currentStepKey, nextStepKey,
            nowSupplier, policyRegistry);
    }

    public ClusterState moveClusterStateToFailedStep(ClusterState currentState, String[] indices) {
        return lifecycleRunner.moveClusterStateToFailedStep(currentState, indices);
    }

    SchedulerEngine getScheduler() {
        return scheduler.get();
    }

    SchedulerEngine.Job getScheduledJob() {
        return scheduledJob;
    }

    public LongSupplier getNowSupplier() {
        return nowSupplier;
    }

    public PolicyStepsRegistry getPolicyRegistry() {
        return policyRegistry;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        IndexLifecycleMetadata lifecycleMetadata = event.state().metaData().custom(IndexLifecycleMetadata.TYPE);
        if (event.localNodeMaster() && lifecycleMetadata != null) {
            TimeValue pollInterval = LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING
                .get(event.state().getMetaData().settings());
            TimeValue previousPollInterval = LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING
                .get(event.previousState().getMetaData().settings());

            boolean pollIntervalSettingChanged = !pollInterval.equals(previousPollInterval);

            if (scheduler.get() == null) { // metadata installed and scheduler should be kicked off. start your engines.
                scheduler.set(new SchedulerEngine(settings, clock));
                scheduler.get().register(this);
                scheduleJob(pollInterval);
            } else if (scheduledJob == null) {
                scheduleJob(pollInterval);
            } else if (pollIntervalSettingChanged) { // all engines are running, just need to update with latest interval
                scheduleJob(pollInterval);
            }

            triggerPolicies(event.state(), true);
        } else {
            cancelJob();
        }
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        if (event.localNodeMaster()) { // only act if we are master, otherwise
                                       // keep idle until elected
            // Since indices keep their current phase's details even if the policy changes, it's possible for a deleted index to have a
            // policy, and then be re-created with the same name, so here we remove indices that have been delete so they don't waste memory
            if (event.indicesDeleted().isEmpty() == false) {
                policyRegistry.removeIndices(event.indicesDeleted());
            }
            if (event.state().metaData().custom(IndexLifecycleMetadata.TYPE) != null) {
                // update policy steps registry
                policyRegistry.update(event.state(), client, nowSupplier);
            }
        }
    }

    private void cancelJob() {
        if (scheduler.get() != null) {
            scheduler.get().remove(IndexLifecycle.NAME);
            scheduledJob = null;
        }
    }

    private void scheduleJob(TimeValue pollInterval) {
        scheduledJob = new SchedulerEngine.Job(IndexLifecycle.NAME, new TimeValueSchedule(pollInterval));
        scheduler.get().add(scheduledJob);
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        if (event.getJobName().equals(IndexLifecycle.NAME)) {
            logger.debug("Job triggered: " + event.getJobName() + ", " + event.getScheduledTime() + ", " + event.getTriggeredTime());
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
                StepKey stepKey = IndexLifecycleRunner.getCurrentStepKey(idxMeta.getSettings());
                if (OperationMode.STOPPING == currentMode && stepKey != null
                        && IGNORE_ACTIONS_MAINTENANCE_REQUESTED.contains(stepKey.getAction()) == false) {
                    logger.info("skipping policy [" + policyName + "] for index [" + idxMeta.getIndex().getName()
                        + "]. stopping Index Lifecycle execution");
                    continue;
                }
                lifecycleRunner.runPolicy(policyName, idxMeta, clusterState, fromClusterStateChange);
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
