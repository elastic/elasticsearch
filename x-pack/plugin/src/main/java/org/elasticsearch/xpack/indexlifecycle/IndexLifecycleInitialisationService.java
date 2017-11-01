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
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.security.InternalClient;

import java.io.Closeable;
import java.io.IOException;
import java.time.Clock;

import static org.elasticsearch.xpack.indexlifecycle.IndexLifecycle.LIFECYCLE_TIMESERIES_SETTING;

public class IndexLifecycleInitialisationService extends AbstractComponent implements LocalNodeMasterListener, IndexEventListener, SchedulerEngine.Listener, Closeable {
    private static final Logger logger = ESLoggerFactory.getLogger(XPackPlugin.class);

    private final SetOnce<SchedulerEngine> scheduler = new SetOnce<>();
    private final Clock clock;
    private InternalClient client;
    private ClusterService clusterService;
    private boolean isMaster;

    public IndexLifecycleInitialisationService(Settings settings, InternalClient client, ClusterService clusterService, Clock clock) {
        super(settings);
        this.client = client;
        this.clusterService = clusterService;
        this.clock = clock;
        clusterService.addLocalNodeMasterListener(this);
    }

    /**
     * This should kick-off some stuff in the scheduler
     * This is triggered every update to settings in the index
     * @param settings the settings to read the lifecycle details from
     */
    public synchronized void setLifecycleSettings(Index index, long creationDate, Settings settings) {
        if (isMaster) {
            IndexLifecycleSettings lifecycleSettings = new IndexLifecycleSettings(index, creationDate, settings, client);
            lifecycleSettings.schedulePhases(scheduler.get());
        }
    }

    /**
     * Called before the index gets created. Note that this is also called
     * when the index is created on data nodes
     * @param index The index whose settings are to be validated
     * @param indexSettings The settings for the specified index
     */
    @Override
    public void beforeIndexCreated(Index index, Settings indexSettings) {
        ESLoggerFactory.getLogger("INDEX-LIFECYCLE-PLUGIN").error("validate setting before index is created");
        LIFECYCLE_TIMESERIES_SETTING.get(indexSettings);
    }

    @Override
    public void afterIndexCreated(IndexService indexService) {
        Settings indexSettings = indexService.getIndexSettings().getSettings();
        Settings lifecycleSettings = (Settings) LIFECYCLE_TIMESERIES_SETTING.get(indexSettings);
        if (isMaster && lifecycleSettings.size() > 0) {
            setLifecycleSettings(indexService.index(), indexService.getMetaData().getCreationDate(),
                indexSettings.getByPrefix(LIFECYCLE_TIMESERIES_SETTING.getKey()));
        }
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void onMaster() {
        isMaster = true;
        scheduler.set(new SchedulerEngine(clock));
        clusterService.state().getMetaData().getIndices().valuesIt()
                .forEachRemaining((idxMeta) -> {
                    if (idxMeta.getSettings().getByPrefix(LIFECYCLE_TIMESERIES_SETTING.getKey()).size() > 0) {
                        setLifecycleSettings(idxMeta.getIndex(), idxMeta.getCreationDate(),
                            idxMeta.getSettings().getByPrefix(LIFECYCLE_TIMESERIES_SETTING.getKey()));
                }
            });
    }

    @Override
    public void offMaster() {
        isMaster = false;
        // when is this called?
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.MANAGEMENT;
    }

    @Override
    public void close() throws IOException {
        SchedulerEngine engine = scheduler.get();
        if (engine != null) {
            engine.stop();
        }
    }
}
