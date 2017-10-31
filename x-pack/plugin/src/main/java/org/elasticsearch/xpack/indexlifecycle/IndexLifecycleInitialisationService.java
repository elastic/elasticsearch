/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.security.InternalClient;

import java.io.IOException;
import java.time.Clock;

import static org.elasticsearch.xpack.indexlifecycle.IndexLifecycle.LIFECYCLE_TIMESERIES_SETTING;

public class IndexLifecycleInitialisationService extends AbstractLifecycleComponent implements LocalNodeMasterListener, IndexEventListener, SchedulerEngine.Listener {
    private static final Logger logger = ESLoggerFactory.getLogger(XPackPlugin.class);
    private SchedulerEngine scheduler;
    private InternalClient client;
    private ClusterService clusterService;
    private boolean isMaster;

    public IndexLifecycleInitialisationService(Settings settings, InternalClient client, ClusterService clusterService, Clock clock) {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
        this.scheduler = new SchedulerEngine(clock);
        clusterService.addLocalNodeMasterListener(this);
    }

    /**
     * This should kick-off some stuff in the scheduler
     * This is triggered every update to settings in the index
     * @param settings the settings to read the lifecycle details from
     */
    public synchronized void setLifecycleSettings(Index index, long creationDate, Settings settings) {
        if (isMaster == true) {
            IndexLifecycleSettings lifecycleSettings = new IndexLifecycleSettings(index, creationDate, settings, client);
            registerIndexSchedule(lifecycleSettings);
        }
    }

    /**
     * This does the heavy lifting of adding an index's lifecycle policy to the scheduler.
     * @param lifecycleSettings The index lifecycle settings object
     */
    private void registerIndexSchedule(IndexLifecycleSettings lifecycleSettings) {
        // need to check that this isn't re-kicking an existing policy... diffs, etc.
        // this is where the genesis of index lifecycle management occurs... kick off the scheduling... all is valid!

        // TODO: scheduler needs to know which index's settings are being updated...
        lifecycleSettings.schedulePhases(scheduler);

    }

    /**
     * Called before the index gets created. Note that this is also called
     * when the index is created on data nodes
     * @param index The index whose settings are to be validated
     * @param indexSettings The settings for the specified index
     */
    public void beforeIndexCreated(Index index, Settings indexSettings) {
        ESLoggerFactory.getLogger("INDEX-LIFECYCLE-PLUGIN").error("validate setting before index is created");
        LIFECYCLE_TIMESERIES_SETTING.get(indexSettings);
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onMaster() {
        isMaster = true;
        clusterService.state().getMetaData().getIndices().valuesIt()
                .forEachRemaining((idxMeta) -> {
                    if (idxMeta.getSettings().getByPrefix(LIFECYCLE_TIMESERIES_SETTING.getKey()).size() > 0) {
                        IndexLifecycleSettings lifecycleSettings = new IndexLifecycleSettings(idxMeta.getIndex(), idxMeta.getCreationDate(),
                                idxMeta.getSettings().getByPrefix(LIFECYCLE_TIMESERIES_SETTING.getKey()), client);
                        registerIndexSchedule(lifecycleSettings);
                }
            });
    }

    @Override
    public void offMaster() {
        isMaster = false;
        doStop();
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    protected void doStop() {
        scheduler.stop();
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doClose() throws IOException {

    }
}
