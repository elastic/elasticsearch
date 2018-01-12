/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import com.google.common.base.Strings;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule.Interval;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule.Interval.Unit;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.util.Collections;
import java.util.SortedMap;
import java.util.function.LongSupplier;

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

            TimeValue pollInterval = IndexLifecycle.LIFECYCLE_POLL_INTERVAL_SETTING
                .get(event.state().getMetaData().settings());
            TimeValue previousPollInterval = IndexLifecycle.LIFECYCLE_POLL_INTERVAL_SETTING
                .get(event.previousState().getMetaData().settings());

            boolean pollIntervalSettingChanged = !pollInterval.equals(previousPollInterval);

            if (lifecycleMetadata == null) { // no lifecycle metadata, install initial empty metadata state
                lifecycleMetadata = new IndexLifecycleMetadata(Collections.emptySortedMap());
                installMetadata(lifecycleMetadata);
            } else if (scheduler.get() == null) { // metadata installed and scheduler should be kicked off. start your engines.
                scheduler.set(new SchedulerEngine(clock));
                scheduler.get().register(this);
                scheduledJob = new SchedulerEngine.Job(IndexLifecycle.NAME,
                    new IntervalSchedule(new Interval(pollInterval.seconds(), Unit.SECONDS)));
                scheduler.get().add(scheduledJob);
            } else if (pollIntervalSettingChanged) { // all engines are running, just need to update with latest interval
                scheduledJob = new SchedulerEngine.Job(IndexLifecycle.NAME,
                    new IntervalSchedule(new Interval(pollInterval.seconds(), Unit.SECONDS)));
                scheduler.get().add(scheduledJob);
            }
        }
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        if (event.getJobName().equals(IndexLifecycle.NAME)) {
            logger.info("Job triggered: " + event.getJobName() + ", " + event.getScheduledTime() + ", " + event.getTriggeredTime());
            IndexLifecycleMetadata indexLifecycleMetadata = clusterService.state().metaData().custom(IndexLifecycleMetadata.TYPE);
            SortedMap<String, LifecyclePolicy> policies = indexLifecycleMetadata.getPolicies();
            clusterService.state().metaData().indices().valuesIt().forEachRemaining((idxMeta) -> {
                String policyName = IndexLifecycle.LIFECYCLE_NAME_SETTING.get(idxMeta.getSettings());
                if (Strings.isNullOrEmpty(policyName) == false) {
                    logger.info("Checking index for next action: " + idxMeta.getIndex().getName() + " (" + policyName + ")");
                    LifecyclePolicy policy = policies.get(policyName);
                    if (policy == null) {
                        logger.error("Unknown lifecycle policy [{}] for index [{}]", policyName, idxMeta.getIndex().getName());
                    } else {
                        try {
                            policy.execute(new InternalIndexLifecycleContext(idxMeta.getIndex(), client, clusterService, nowSupplier));
                        } catch (ElasticsearchException e) {
                            logger.error("Failed to execute lifecycle policy [{}] for index [{}]", policyName, idxMeta.getIndex().getName(),
                                    policyName);
                        }
                    }
                }
            });
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

    @Override
    public void close() throws IOException {
        SchedulerEngine engine = scheduler.get();
        if (engine != null) {
            engine.stop();
        }
    }
}
