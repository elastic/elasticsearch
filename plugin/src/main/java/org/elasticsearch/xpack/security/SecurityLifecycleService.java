/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.security.audit.index.IndexAuditTrail;
import org.elasticsearch.xpack.security.support.IndexLifecycleManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
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

    public static final String SECURITY_INDEX_NAME = ".security";
    public static final String SECURITY_TEMPLATE_NAME = "security-index-template";
    public static final String INTERNAL_SECURITY_INDEX = IndexLifecycleManager.INTERNAL_SECURITY_INDEX;

    private static final Version MIN_READ_VERSION = Version.V_5_0_0;

    private final Settings settings;
    private final ThreadPool threadPool;
    private final IndexAuditTrail indexAuditTrail;

    private final IndexLifecycleManager securityIndex;

    public SecurityLifecycleService(Settings settings, ClusterService clusterService,
                                    ThreadPool threadPool, Client client,
                                    @Nullable IndexAuditTrail indexAuditTrail) {
        super(settings);
        this.settings = settings;
        this.threadPool = threadPool;
        this.indexAuditTrail = indexAuditTrail;
        this.securityIndex = new IndexLifecycleManager(settings, client, SECURITY_INDEX_NAME, SECURITY_TEMPLATE_NAME);
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

    IndexLifecycleManager securityIndex() {
        return securityIndex;
    }

    public boolean isSecurityIndexExisting() {
        return securityIndex.indexExists();
    }

    public boolean isSecurityIndexUpToDate() {
        return securityIndex.isIndexUpToDate();
    }

    public boolean isSecurityIndexAvailable() {
        return securityIndex.isAvailable();
    }

    public boolean isSecurityIndexWriteable() {
        return securityIndex.isWritable();
    }

    /**
     * Test whether the effective (active) version of the security mapping meets the
     * <code>requiredVersion</code>.
     *
     * @return <code>true</code> if the effective version passes the predicate, or the security
     * mapping does not exist (<code>null</code> version). Otherwise, <code>false</code>.
     */
    public boolean checkSecurityMappingVersion(Predicate<Version> requiredVersion) {
        return securityIndex.checkMappingVersion(requiredVersion);
    }

    /**
     * Adds a listener which will be notified when the security index health changes. The previous and
     * current health will be provided to the listener so that the listener can determine if any action
     * needs to be taken.
     */
    public void addSecurityIndexHealthChangeListener(BiConsumer<ClusterIndexHealth, ClusterIndexHealth> listener) {
        securityIndex.addIndexHealthChangeListener(listener);
    }

    /**
     * Adds a listener which will be notified when the security index out of date value changes. The previous and
     * current value will be provided to the listener so that the listener can determine if any action
     * needs to be taken.
     */
    void addSecurityIndexOutOfDateListener(BiConsumer<Boolean, Boolean> listener) {
        securityIndex.addIndexOutOfDateListener(listener);
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

    public static boolean securityIndexMappingAndTemplateSufficientToRead(ClusterState clusterState,
                                                                  Logger logger) {
        return checkTemplateAndMappingVersions(clusterState, logger, MIN_READ_VERSION::onOrBefore);
    }

    public static boolean securityIndexMappingAndTemplateUpToDate(ClusterState clusterState,
                                                                  Logger logger) {
        return checkTemplateAndMappingVersions(clusterState, logger, Version.CURRENT::equals);
    }

    private static boolean checkTemplateAndMappingVersions(ClusterState clusterState, Logger logger,
                                                           Predicate<Version> versionPredicate) {
        return IndexLifecycleManager.checkTemplateExistsAndVersionMatches(SECURITY_TEMPLATE_NAME,
                clusterState, logger, versionPredicate) &&
                IndexLifecycleManager.checkIndexMappingVersionMatches(SECURITY_INDEX_NAME,
                        clusterState, logger, versionPredicate);
    }

    public static List<String> indexNames() {
        return Collections.unmodifiableList(Arrays.asList(SECURITY_INDEX_NAME, INTERNAL_SECURITY_INDEX));
    }

    /**
     * Creates the security index, if it does not already exist, then runs the given
     * action on the security index.
     */
    public <T> void createIndexIfNeededThenExecute(final ActionListener<T> listener, final Runnable andThen) {
        if (!isSecurityIndexExisting() || isSecurityIndexUpToDate()) {
            securityIndex.createIndexIfNeededThenExecute(listener, andThen);
        } else {
            listener.onFailure(new IllegalStateException(
                "Security index is not on the current version - the native realm will not be operational until " +
                "the upgrade API is run on the security index"));
        }
    }

    /**
     * Checks if the security index is out of date with the current version. If the index does not exist
     * we treat the index as up to date as we expect it to be created with the current format.
     */
    public boolean isSecurityIndexOutOfDate() {
        return securityIndex.isIndexUpToDate() == false;
    }
}
