/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.audit.index.IndexAuditTrail;
import org.elasticsearch.xpack.security.authc.esnative.NativeRealmMigrator;
import org.elasticsearch.xpack.security.authc.esnative.NativeUsersStore;
import org.elasticsearch.xpack.security.authz.store.NativeRolesStore;

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
public class SecurityLifecycleService extends AbstractComponent implements ClusterStateListener {

    private final Settings settings;
    private final ThreadPool threadPool;
    private final IndexAuditTrail indexAuditTrail;
    private final NativeUsersStore nativeUserStore;
    private final NativeRolesStore nativeRolesStore;

    public SecurityLifecycleService(Settings settings, ClusterService clusterService, ThreadPool threadPool,
                                    @Nullable IndexAuditTrail indexAuditTrail, NativeUsersStore nativeUserStore,
                                    NativeRolesStore nativeRolesStore, XPackLicenseState licenseState, InternalClient client) {
        super(settings);
        this.settings = settings;
        this.threadPool = threadPool;
        this.indexAuditTrail = indexAuditTrail;
        this.nativeUserStore = nativeUserStore;
        this.nativeRolesStore = nativeRolesStore;
        // TODO: define a common interface for these and delegate from one place. nativeUserStore store is it's on
        // cluster
        // state listener , but is also activated from this clusterChanged method
        clusterService.addListener(this);
        clusterService.addListener(nativeUserStore);
        clusterService.addListener(nativeRolesStore);
        final NativeRealmMigrator nativeRealmMigrator = new NativeRealmMigrator(settings, licenseState, client);
        clusterService.addListener(new SecurityTemplateService(settings, client, nativeRealmMigrator));
        clusterService.addLifecycleListener(new LifecycleListener() {

            @Override
            public void beforeStop() {
                stop();
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
                    public void onFailure(Exception throwable) {
                        logger.error("failed to start native user store service", throwable);
                        assert false : "security lifecycle services startup failed";
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
                    public void onFailure(Exception throwable) {
                        logger.error("failed to start native roles store services", throwable);
                        assert false : "security lifecycle services startup failed";
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
            if (Security.indexAuditLoggingEnabled(settings) &&
                    indexAuditTrail.state() == IndexAuditTrail.State.INITIALIZED) {
                if (indexAuditTrail.canStart(event, master)) {
                    threadPool.generic().execute(new AbstractRunnable() {

                        @Override
                        public void onFailure(Exception throwable) {
                            logger.error("failed to start index audit trail services", throwable);
                            assert false : "security lifecycle services startup failed";
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
        if (indexAuditTrail != null) {
            try {
                indexAuditTrail.stop();
            } catch (Exception e) {
                logger.error("failed to stop audit trail module", e);
            }
        }
    }
}
