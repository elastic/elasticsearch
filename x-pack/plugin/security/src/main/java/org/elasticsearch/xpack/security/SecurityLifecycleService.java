/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.audit.index.IndexAuditTrail;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * This class is used to provide a lifecycle for services that is based on the cluster's state
 * rather than the typical lifecycle that is used to start services as part of the node startup.
 *
 * This type of lifecycle is necessary for services that need to perform actions that require the
 * cluster to be in a certain state; some examples are storing index templates and creating indices.
 * These actions would most likely fail from within a plugin if executed in the
 * {@link org.elasticsearch.common.component.AbstractLifecycleComponent#doStart()} method.
 * However, if the startup of these services waits for the cluster to form and recover indices then
 * it will be successful. This lifecycle service allows for this to happen by listening for
 * {@link ClusterChangedEvent} and checking if the services can start. Additionally, the service
 * also provides hooks for stop and close functionality.
 */
public class SecurityLifecycleService extends AbstractComponent implements ClusterStateListener {

    public static final String INTERNAL_SECURITY_INDEX = SecurityIndexManager.INTERNAL_SECURITY_INDEX;
    public static final String SECURITY_INDEX_NAME = ".security";

    private static final Version MIN_READ_VERSION = Version.V_5_0_0;

    private final Settings settings;
    private final ThreadPool threadPool;
    private final IndexAuditTrail indexAuditTrail;

    private final SecurityIndexManager securityIndex;

    public SecurityLifecycleService(Settings settings, ClusterService clusterService,
                                    ThreadPool threadPool, Client client,
                                    @Nullable IndexAuditTrail indexAuditTrail) {
        super(settings);
        this.settings = settings;
        this.threadPool = threadPool;
        this.indexAuditTrail = indexAuditTrail;
        this.securityIndex = new SecurityIndexManager(settings, client, SECURITY_INDEX_NAME);
        clusterService.addListener(this);
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                close();
            }
        });
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        final ClusterState state = event.state();
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // wait until the gateway has recovered from disk, otherwise we think we don't have the
            // .security index but they may not have been restored from the cluster state on disk
            logger.debug("lifecycle service waiting until state has been recovered");
            return;
        }

        securityIndex.clusterChanged(event);

        try {
            if (Security.indexAuditLoggingEnabled(settings) &&
                    indexAuditTrail.state() == IndexAuditTrail.State.INITIALIZED) {
                if (indexAuditTrail.canStart(event)) {
                    threadPool.generic().execute(new AbstractRunnable() {

                        @Override
                        public void onFailure(Exception throwable) {
                            logger.error("failed to start index audit trail services", throwable);
                            assert false : "security lifecycle services startup failed";
                        }

                        @Override
                        public void doRun() {
                            indexAuditTrail.start();
                        }
                    });
                }
            }
        } catch (Exception e) {
            logger.error("failed to start index audit trail", e);
        }
    }

    public SecurityIndexManager securityIndex() {
        return securityIndex;
    }

    // this is called in a lifecycle listener beforeStop on the cluster service
    private void close() {
        if (indexAuditTrail != null) {
            try {
                indexAuditTrail.stop();
            } catch (Exception e) {
                logger.error("failed to stop audit trail module", e);
            }
        }
    }

    public static boolean securityIndexMappingSufficientToRead(ClusterState clusterState, Logger logger) {
        return checkMappingVersions(clusterState, logger, MIN_READ_VERSION::onOrBefore);
    }

    static boolean securityIndexMappingUpToDate(ClusterState clusterState, Logger logger) {
        return checkMappingVersions(clusterState, logger, Version.CURRENT::equals);
    }

    private static boolean checkMappingVersions(ClusterState clusterState, Logger logger, Predicate<Version> versionPredicate) {
        return SecurityIndexManager.checkIndexMappingVersionMatches(SECURITY_INDEX_NAME, clusterState, logger, versionPredicate);
    }

    public static List<String> indexNames() {
        return Collections.unmodifiableList(Arrays.asList(SECURITY_INDEX_NAME, INTERNAL_SECURITY_INDEX));
    }

    /**
     * Is the move from {@code previousHealth} to {@code currentHealth} a move from an unhealthy ("RED") index state to a healthy
     * ("non-RED") state.
     */
    public static boolean isMoveFromRedToNonRed(ClusterIndexHealth previousHealth, ClusterIndexHealth currentHealth) {
        return (previousHealth == null || previousHealth.getStatus() == ClusterHealthStatus.RED)
                && currentHealth != null && currentHealth.getStatus() != ClusterHealthStatus.RED;
    }

    /**
     * Is the move from {@code previousHealth} to {@code currentHealth} a move from index-exists to index-deleted
     */
    public static boolean isIndexDeleted(ClusterIndexHealth previousHealth, ClusterIndexHealth currentHealth) {
        return previousHealth != null && currentHealth == null;
    }

}
