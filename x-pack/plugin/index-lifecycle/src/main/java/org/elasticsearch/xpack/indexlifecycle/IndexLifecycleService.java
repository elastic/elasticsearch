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
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexlifecycle.LifecyclePolicy;
import org.elasticsearch.xpack.core.indexlifecycle.LifecycleSettings;
import org.elasticsearch.xpack.core.indexlifecycle.Step;
import org.elasticsearch.xpack.core.indexlifecycle.StepResult;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;

import java.io.Closeable;
import java.time.Clock;
import java.util.Collections;
import java.util.List;
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
        clusterService.addListener(this);
    }

    SchedulerEngine getScheduler() {
        return scheduler.get();
    }

    SchedulerEngine.Job getScheduledJob() {
        return scheduledJob;
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

    public synchronized void triggerPolicies() {
        IndexLifecycleMetadata indexLifecycleMetadata = clusterService.state().metaData().custom(IndexLifecycleMetadata.TYPE);
        SortedMap<String, LifecyclePolicy> policies = indexLifecycleMetadata.getPolicies();
        // loop through all indices in cluster state and filter for ones that are
        // managed by the Index Lifecycle Service they have a index.lifecycle.name setting
        // associated to a policy
        ClusterState clusterState = clusterService.state();
        clusterState.metaData().indices().valuesIt().forEachRemaining((idxMeta) -> {
            String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings());
            if (Strings.isNullOrEmpty(policyName) == false) {
                LifecyclePolicy policy = policies.get(policyName);
                // fetch step
                // check whether complete
                // if complete, launch next task
                clusterService.submitStateUpdateTask("index-lifecycle-" + policyName, new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) throws Exception {
                        // ensure date is set
                        currentState = putLifecycleDate(currentState, idxMeta);
                        long lifecycleDate = currentState.metaData().settings()
                            .getAsLong(LifecycleSettings.LIFECYCLE_INDEX_CREATION_DATE, -1L);
                        // TODO(talevy): make real
                        List<Step> steps = policy.getPhases().values().stream()
                            .flatMap(p -> p.toSteps(idxMeta.getIndex(), lifecycleDate, client, threadPool, nowSupplier).stream())
                            .collect(Collectors.toList());
                        StepResult result = policy.execute(steps, currentState, idxMeta, client, nowSupplier);
                        return result.getClusterState();
                    }

                    @Override
                    public void onFailure(String source, Exception e) {

                    }
                });
            }
        });
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
