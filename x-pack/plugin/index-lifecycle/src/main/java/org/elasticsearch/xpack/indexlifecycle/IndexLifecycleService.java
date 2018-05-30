/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexlifecycle.IndexLifecycleMetadata;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;

import java.io.Closeable;
import java.time.Clock;
import java.util.Collections;
import java.util.function.LongSupplier;

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
    private IndexLifecycleRunner lifecycleRunner;

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
        this.lifecycleRunner = new IndexLifecycleRunner(policyRegistry, clusterService, nowSupplier);
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
        if (event.localNodeMaster()) { // only act if we are master, otherwise keep idle until elected
            IndexLifecycleMetadata lifecycleMetadata = event.state().metaData().custom(IndexLifecycleMetadata.TYPE);
            TimeValue pollInterval = LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING
                .get(event.state().getMetaData().settings());
            TimeValue previousPollInterval = LifecycleSettings.LIFECYCLE_POLL_INTERVAL_SETTING
                .get(event.previousState().getMetaData().settings());

            boolean pollIntervalSettingChanged = !pollInterval.equals(previousPollInterval);

            if (lifecycleMetadata != null && event.changedCustomMetaDataSet().contains(IndexLifecycleMetadata.TYPE)) {
                // update policy steps registry
                policyRegistry.update(event.state(), client, nowSupplier);
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

            triggerPolicies(event.state(), true);
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

    @Override
    public void triggered(SchedulerEngine.Event event) {
        if (event.getJobName().equals(IndexLifecycle.NAME)) {
            logger.debug("Job triggered: " + event.getJobName() + ", " + event.getScheduledTime() + ", " + event.getTriggeredTime());
            triggerPolicies(clusterService.state(), false);
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

    public void triggerPolicies(ClusterState clusterState, boolean fromClusterStateChange) {
        // loop through all indices in cluster state and filter for ones that are
        // managed by the Index Lifecycle Service they have a index.lifecycle.name setting
        // associated to a policy
        clusterState.metaData().indices().valuesIt().forEachRemaining((idxMeta) -> {
            String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings());
            if (Strings.isNullOrEmpty(policyName) == false) {
                lifecycleRunner.runPolicy(policyName, idxMeta, clusterState, fromClusterStateChange);
            }
        });
    }

    @Override
    public void close() {
        SchedulerEngine engine = scheduler.get();
        if (engine != null) {
            engine.stop();
        }
    }
}
