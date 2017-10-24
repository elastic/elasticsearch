/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.indexlifecycle;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.security.InternalClient;

import java.time.Clock;
import java.util.Collections;

import static org.elasticsearch.xpack.indexlifecycle.IndexLifecycle.LIFECYCLE_TIMESERIES_SETTING;

public class IndexLifecycleInitialisationService extends AbstractComponent implements ClusterStateListener, IndexEventListener, SchedulerEngine.Listener {
    private static final Logger logger = ESLoggerFactory.getLogger(XPackPlugin.class);
    private SchedulerEngine scheduler;
    private InternalClient client;

    public IndexLifecycleInitialisationService(Settings settings, InternalClient client, ClusterService clusterService, Clock clock) {
        super(settings);
        this.scheduler = new SchedulerEngine(clock);
        this.client = client;
        this.scheduler.register(this);
        clusterService.addListener(this);
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStart() {
                scheduler.start(Collections.emptyList());
            }

            @Override
            public void beforeStop() {
                super.beforeStop();
                scheduler.stop(); // should I just be an AbstractLifecycleComponent instead?
            }
        });
    }

    /**
     * This should kick-off some stuff in the scheduler
     * @param settings the settings to read the lifecycle details from
     */
    public synchronized void setLifecycleSettings(Index index, Settings settings) {
        // need to check that this isn't re-kicking an existing policy... diffs, etc.
        // this is where the genesis of index lifecycle management occurs... kick off the scheduling... all is valid!
        TimeValue deleteAfter = settings.getAsTime("delete.after", TimeValue.MINUS_ONE);
        SchedulerEngine.Schedule schedule = (startTime, now) -> {
            if (startTime == now) {
                return now + deleteAfter.getMillis();
            } else {
                return -1; // do not schedule another delete after already deleted
            }
        };
        // TODO: scheduler needs to know which index's settings are being updated...
        scheduler.add(new SchedulerEngine.Job(index.getName(), schedule));
        ESLoggerFactory.getLogger("INDEX-LIFECYCLE-PLUGIN")
            .error("kicked off lifecycle job to be triggered in " + deleteAfter.getSeconds() + " seconds");
    }

    /**
     * Called before the index gets created. Note that this is also called
     * when the index is created on data nodes
     */
    public void beforeIndexCreated(Index index, Settings indexSettings) {
        ESLoggerFactory.getLogger("INDEX-LIFECYCLE-PLUGIN").error("validate setting before index is created");
        LIFECYCLE_TIMESERIES_SETTING.get(indexSettings);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        ESLoggerFactory.getLogger("INDEX-LIFECYCLE-PLUGIN").debug("cluster state changed: " + event.source());
    }

    @Override
    public void triggered(SchedulerEngine.Event event) {
        client.admin().indices().prepareDelete(event.getJobName()).execute(new ActionListener<DeleteIndexResponse>() {
            @Override
            public void onResponse(DeleteIndexResponse deleteIndexResponse) {
                logger.error(deleteIndexResponse);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error(e);
            }
        });
    }
}
