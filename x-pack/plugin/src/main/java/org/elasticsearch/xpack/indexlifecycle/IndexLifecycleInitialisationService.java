/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import com.google.common.base.Strings;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.security.InternalClient;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule.Interval;
import org.elasticsearch.xpack.watcher.trigger.schedule.IntervalSchedule.Interval.Unit;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;
import java.util.SortedMap;

import static org.elasticsearch.xpack.indexlifecycle.IndexLifecycle.NAME;

public class IndexLifecycleInitialisationService extends AbstractComponent
        implements ClusterStateListener, SchedulerEngine.Listener, Closeable {
    private static final Logger logger = ESLoggerFactory.getLogger(IndexLifecycleInitialisationService.class);

    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private final Clock clock;
    private InternalClient client;
    private ClusterService clusterService;
    private ThreadPool threadPool;

    public IndexLifecycleInitialisationService(Settings settings, InternalClient client, ClusterService clusterService, Clock clock,
            ThreadPool threadPool) {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
        this.clock = clock;
        this.threadPool = threadPool;
        clusterService.addListener(this);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster()) {
            MetaData metaData = event.state().metaData();
            installMlMetadata(metaData);
        }
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        if (event.getJobName().equals(NAME)) {
            logger.error("Job triggered: " + event.getJobName() + ", " + event.getScheduledTime() + ", " + event.getTriggeredTime());
            IndexLifecycleMetadata indexLifecycleMetadata = clusterService.state().metaData().custom(IndexLifecycleMetadata.TYPE);
            SortedMap<String, LifecyclePolicy> policies = indexLifecycleMetadata.getPolicies();
            clusterService.state().getMetaData().getIndices().valuesIt().forEachRemaining((idxMeta) -> {
                String policyName = IndexLifecycle.LIFECYCLE_TIMESERIES_NAME_SETTING.get(idxMeta.getSettings());
                if (Strings.isNullOrEmpty(policyName) == false) {
                    logger.error("Checking index for next action: " + idxMeta.getIndex().getName() + " (" + policyName + ")");
                    LifecyclePolicy policy = policies.get(policyName);
                    policy.execute(idxMeta, client);
                }
            });
        }
    }

    private void installMlMetadata(MetaData metaData) {
        IndexLifecycleMetadata indexLifecycleMetadata = metaData.custom(IndexLifecycleMetadata.TYPE);
        if (indexLifecycleMetadata == null) {
            threadPool.executor(ThreadPool.Names.GENERIC)
                    .execute(() -> clusterService.submitStateUpdateTask("install-index-lifecycle-metadata", new ClusterStateUpdateTask() {
                        @Override
                        public ClusterState execute(ClusterState currentState) throws Exception {
                            // If the metadata has been added already don't try to update
                            if (currentState.metaData().custom(IndexLifecycleMetadata.TYPE) != null) {
                                return currentState;
                            }
                            ClusterState.Builder builder = new ClusterState.Builder(currentState);
                            MetaData.Builder metadataBuilder = MetaData.builder(currentState.metaData());
                            metadataBuilder.putCustom(IndexLifecycleMetadata.TYPE, IndexLifecycleMetadata.EMPTY_METADATA);
                            builder.metaData(metadataBuilder.build());
                            return builder.build();
                        }

                        @Override
                        public void onFailure(String source, Exception e) {
                            logger.error("unable to install index lifecycle metadata", e);
                        }
                    }));
        } else {
            if (scheduler.get() == null) {
                scheduler.set(new SchedulerEngine(clock));
                scheduler.get().register(this);
                scheduler.get().add(new SchedulerEngine.Job(NAME,
                        new IntervalSchedule(new Interval(indexLifecycleMetadata.getPollInterval(), Unit.SECONDS))));
            }
        }
    }

    @Override
    public void close() throws IOException {
        SchedulerEngine engine = scheduler.get();
        if (engine != null) {
            engine.stop();
        }
    }
}
