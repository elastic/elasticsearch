/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.shield.audit.AuditTrailModule;
import org.elasticsearch.shield.audit.index.IndexAuditTrail;
import org.elasticsearch.shield.authc.esnative.NativeUsersStore;
import org.elasticsearch.shield.authz.store.NativeRolesStore;
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

    private final Settings settings;
    private final ThreadPool threadPool;
    private final IndexAuditTrail indexAuditTrail;
    private final NativeUsersStore nativeUserStore;
    private final NativeRolesStore nativeRolesStore;

    @Inject
    public ShieldLifecycleService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                                  IndexAuditTrail indexAuditTrail, NativeUsersStore nativeUserStore,
                                  NativeRolesStore nativeRolesStore, Provider<InternalClient> clientProvider) {
        super(settings);
        this.settings = settings;
        this.threadPool = threadPool;
        this.indexAuditTrail = indexAuditTrail;
        this.nativeUserStore = nativeUserStore;
        this.nativeRolesStore = nativeRolesStore;
        // TODO: define a common interface for these and delegate from one place. nativeUserStore store is it's on cluster
        // state listener , but is also activated from this clusterChanged method
        clusterService.add(this);
        clusterService.add(nativeUserStore);
        clusterService.add(nativeRolesStore);
        clusterService.add(new ShieldTemplateService(settings, clusterService, clientProvider, threadPool));
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
        final boolean master = event.localNodeMaster();
        try {
            if (nativeUserStore.canStart(event.state(), master)) {
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Throwable throwable) {
                        logger.error("failed to start native user store service", throwable);
                        assert false : "shield lifecycle services startup failed";
                    }

                    @Override
                    public void doRun() {
                        nativeUserStore.start();
                    }
                });
            }
        } catch (Exception e) {
            logger.error("failed to start native user store", e);
        }

        try {
            if (nativeRolesStore.canStart(event.state(), master)) {
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    public void onFailure(Throwable throwable) {
                        logger.error("failed to start native roles store services", throwable);
                        assert false : "shield lifecycle services startup failed";
                    }

                    @Override
                    public void doRun() {
                        nativeRolesStore.start();
                    }
                });
            }
        } catch (Exception e) {
            logger.error("failed to start native roles store", e);
        }

        try {
            if (AuditTrailModule.indexAuditLoggingEnabled(settings) &&
                    indexAuditTrail.state() == IndexAuditTrail.State.INITIALIZED) {
                if (indexAuditTrail.canStart(event, master)) {
                    threadPool.generic().execute(new AbstractRunnable() {

                        @Override
                        public void onFailure(Throwable throwable) {
                            logger.error("failed to start index audit trail services", throwable);
                            assert false : "shield lifecycle services startup failed";
                        }

                        @Override
                        public void doRun() {
                            indexAuditTrail.start(master);
                        }
                    });
                }
            }
        } catch (Exception e) {
            logger.error("failed to start index audit trail", e);
        }

    }

    public void stop() {
        try {
            nativeUserStore.stop();
        } catch (Exception e) {
            logger.error("failed to stop native user module", e);
        }
        try {
            nativeRolesStore.stop();
        } catch (Exception e) {
            logger.error("failed to stop native roles module", e);
        }
        try {
            indexAuditTrail.stop();
        } catch (Exception e) {
            logger.error("failed to stop audit trail module", e);
        }
    }

    public void close() {
        // There is no .close() method for the roles module
        try {
            indexAuditTrail.close();
        } catch (Exception e) {
            logger.error("failed to close audit trail module", e);
        }
    }
}
