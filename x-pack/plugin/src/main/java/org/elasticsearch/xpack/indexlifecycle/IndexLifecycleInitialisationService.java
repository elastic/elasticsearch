/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.security.InternalClient;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;

import static org.elasticsearch.xpack.indexlifecycle.IndexLifecycle.LIFECYCLE_TIMESERIES_NAME_SETTING;
import static org.elasticsearch.xpack.indexlifecycle.IndexLifecycle.NAME;

public class IndexLifecycleInitialisationService extends AbstractComponent implements LocalNodeMasterListener, SchedulerEngine.Listener, Closeable {
    private static final Logger logger = ESLoggerFactory.getLogger(IndexLifecycleInitialisationService.class);

    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private final Clock clock;
    private InternalClient client;
    private ClusterService clusterService;

    public IndexLifecycleInitialisationService(Settings settings, InternalClient client, ClusterService clusterService, Clock clock) {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
        this.clock = clock;
        clusterService.addLocalNodeMasterListener(this);
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    public void onMaster() {
        scheduler.set(new SchedulerEngine(clock));
        scheduler.get().register(this);
        scheduler.get().add(new SchedulerEngine.Job(NAME, ((startTime, now) -> now + 1000)));
    }

    @Override
    public void offMaster() {
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        clusterService.state().getMetaData().getIndices().valuesIt()
            .forEachRemaining((idxMeta) -> {
            if (LIFECYCLE_TIMESERIES_NAME_SETTING.get(idxMeta.getSettings()) != null) {
                // get policy by name
                // idxMeta.getIndex(), idxMeta.getCreationDate(),client
                }
            });
    }

    @Override
    public void close() throws IOException {
        SchedulerEngine engine = scheduler.get();
        if (engine != null) {
            engine.stop();
        }
    }
}
