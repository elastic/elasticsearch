/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.FormattedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.DiffableUtils;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.core.indexlifecycle.StepResult;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;

import java.io.Closeable;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.CompletableFuture;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * A service which runs the {@link LifecyclePolicy}s associated with indexes.
 */
public class IndexLifecycleService extends AbstractComponent
        implements ClusterStateListener, SchedulerEngine.Listener, Closeable {
    private static final Logger logger = ESLoggerFactory.getLogger(IndexLifecycleService.class);

    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private final Clock clock;
    private final PolicyStepsRegistry policyRegistry;
    private Client client;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private LongSupplier nowSupplier;
    private SchedulerEngine.Job scheduledJob;

    public IndexLifecycleService(Settings settings, Client client, ClusterService clusterService, Clock clock,
            ThreadPool threadPool, LongSupplier nowSupplier) {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
        this.clock = clock;
        this.threadPool = threadPool;
        this.nowSupplier = nowSupplier;
        this.scheduledJob = null;
        this.policyRegistry = new PolicyStepsRegistry();
        clusterService.addListener(this);
    }

    SchedulerEngine getScheduler() {
        return scheduler.get();
    }

    SchedulerEngine.Job getScheduledJob() {
        return scheduledJob;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) { // only act if we are master, otherwise keep idle until elected
            IndexLifecycleMetadata lifecycleMetadata = event.state().metaData().custom(IndexLifecycleMetadata.TYPE);
            TimeValue pollInterval = LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING
                .get(event.state().getMetaData().settings());
            TimeValue previousPollInterval = LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING
                .get(event.previousState().getMetaData().settings());

            boolean pollIntervalSettingChanged = !pollInterval.equals(previousPollInterval);

            if (lifecycleMetadata != null) {
                // update policy steps registry
                policyRegistry.update(event.state());
            }

            if (lifecycleMetadata == null) { // no lifecycle metadata, install initial empty metadata state
                lifecycleMetadata = new IndexLifecycleMetadata(Collections.emptySortedMap());
                installMetadata(lifecycleMetadata);
            } else if (scheduler.get() == null) { // metadata installed and scheduler should be kicked off. start your engines.
                scheduler.set(new SchedulerEngine(clock));
                scheduler.get().register(this);
                scheduleJob(pollInterval);
            } else if (scheduledJob == null) {
                scheduleJob(pollInterval);
            } else if (pollIntervalSettingChanged) { // all engines are running, just need to update with latest interval
                scheduleJob(pollInterval);
            }
        } else {
            cancelJob();
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

    public void triggerPolicies() {
        // loop through all indices in cluster state and filter for ones that are
        // managed by the Index Lifecycle Service they have a index.lifecycle.name setting
        // associated to a policy
        ClusterState clusterState = clusterService.state();
        clusterState.metaData().indices().valuesIt().forEachRemaining((idxMeta) -> {
            String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings());
            if (Strings.isNullOrEmpty(policyName) == false) {
                clusterService.submitStateUpdateTask("index-lifecycle-" + policyName, new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        // ensure date is set
                        currentState = putLifecycleDate(currentState, idxMeta);
                        long lifecycleDate = currentState.metaData().settings()
                            .getAsLong(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, -1L);
                        // get current phase, action, step
                        String phase = currentState.metaData().settings().get(LifecycleSettings.LIFECYCLE_PHASE);
                        String action = currentState.metaData().settings().get(LifecycleSettings.LIFECYCLE_ACTION);
                        String stepName = currentState.metaData().settings().get(LifecycleSettings.LIFECYCLE_STEP);
                        // returns current step to execute. If settings are null, then the first step to be executed in
                        // this policy is returned.
                        Step currentStep = policyRegistry.getStep(policyName, phase, action, stepName);
                        return executeStepUntilAsync(currentStep, clusterState, client, nowSupplier, idxMeta.getIndex());
                    }

                    @Override
                    public void onFailure(String source, Exception e) {

                    }
                });
            }
        });
    }

    /**
     * executes the given step, and then all proceeding steps, until it is necessary to exit the
     * cluster-state thread and let any wait-condition or asynchronous action progress externally
     *
     * TODO(colin): should steps execute themselves and execute `nextStep` internally?
     *
     * @param startStep The current step that has either not been executed, or not completed before
     * @return the new ClusterState
     */
    private ClusterState executeStepUntilAsync(Step startStep, ClusterState currentState, Client client, LongSupplier nowSupplier, Index index) {
        StepResult result = startStep.execute(clusterService, currentState, index, client, nowSupplier);
        while (result.isComplete() && result.indexSurvived() && startStep.hasNextStep()) {
            currentState = result.getClusterState();
            startStep = startStep.getNextStep();
            result = startStep.execute(clusterService, currentState, index, client, nowSupplier);
        }
        if (result.isComplete()) {
            currentState = result.getClusterState();
        }
        return currentState;
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        if (event.getJobName().equals(IndexLifecycle.NAME)) {
            logger.info("Job triggered: " + event.getJobName() + ", " + event.getScheduledTime() + ", " + event.getTriggeredTime());
            triggerPolicies();
        }
    }

    private void installMetadata(IndexLifecycleMetadata lifecycleMetadata) {
        threadPool.executor(ThreadPool.Names.GENERIC)
            .execute(() -> clusterService.submitStateUpdateTask("install-index-lifecycle-metadata", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    ClusterState.Builder builder = new ClusterState.Builder(currentState);
                    MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                    metadataBuilder.putCustom(IndexLifecycleMetadata.TYPE, lifecycleMetadata);
                    builder.metaData(metadataBuilder.build());
                    return builder.build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    logger.error("unable to install index lifecycle metadata", e);
                }
            }));
    }

    private ClusterState putLifecycleDate(ClusterState clusterState, IndexMetaData idxMeta) {
        if (idxMeta.getSettings().hasValue(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE)) {
            return clusterState;
        } else {
            ClusterState.Builder builder = new ClusterState.Builder(clusterState);
            MetaData.Builder metadataBuilder = MetaData.builder(clusterState.metaData());
            Settings settings = Settings.builder()
                .put(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE_SETTING.getKey(), idxMeta.getCreationDate()).build();
            metadataBuilder.updateSettings(settings, idxMeta.getIndex().getName());
            return builder.metaData(metadataBuilder.build()).build();
        }
    }

    @Override
    public void close() {
        SchedulerEngine engine = scheduler.get();
        if (engine != null) {
            engine.stop();
        }
    }
}
