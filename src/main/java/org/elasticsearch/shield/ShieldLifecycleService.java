/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.shield.audit.index.IndexAuditTrail;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * This class is used to provide a lifecycle for services that is based on the cluster's state
 * rather than the typical lifecycle that is used to start services as part of the node startup.
 *
 * This type of lifecycle is necessary for services that need to perform actions that require the cluster to be in a
 * certain state; some examples are storing index templates and creating indices. These actions would most likely fail
 * from within a plugin if executed in the {@link org.elasticsearch.common.component.AbstractLifecycleComponent#doStart()}
 * method. However, if the startup of these services waits for the cluster to form and recover indices then it will be
 * successful. This lifecycle service allows for this to happen by listening for {@link ClusterChangedEvent} and checking
 * if the services can start. Additionally, the service also provides hooks for stop and close functionality.
 */
public class ShieldLifecycleService extends AbstractComponent implements ClusterStateListener {

    private final ThreadPool threadPool;
    private final IndexAuditTrail indexAuditTrail;

    @Inject
    public ShieldLifecycleService(Settings settings, ClusterService clusterService, ThreadPool threadPool, IndexAuditTrail indexAuditTrail) {
        super(settings);
        this.threadPool = threadPool;
        this.indexAuditTrail = indexAuditTrail;
        clusterService.add(this);
        clusterService.addLifecycleListener(new LifecycleListener() {

            @Override
            public void beforeStop() {
                stop();
            }

            @Override
            public void beforeClose() {
                close();
            }
        });
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        // TODO if/when we have more services this should not be checking the audit trail
        if (indexAuditTrail.state() == IndexAuditTrail.State.INITIALIZED) {
            final boolean master = event.localNodeMaster();
            if (indexAuditTrail.canStart(event, master)) {
                threadPool.generic().execute(new AbstractRunnable() {

                    @Override
                    public void onFailure(Throwable throwable) {
                        logger.error("failed to start shield lifecycle services", throwable);
                        assert false : "shield lifecycle services startup failed";
                    }

                    @Override
                    public void doRun() {
                        indexAuditTrail.start(master);
                    }
                });
            }
        }
    }

    public void stop() {
        indexAuditTrail.stop();
    }

    public void close() {
        indexAuditTrail.close();
    }
}
