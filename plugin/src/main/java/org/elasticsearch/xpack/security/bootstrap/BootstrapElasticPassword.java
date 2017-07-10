/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.bootstrap;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.security.SecurityLifecycleService;
import org.elasticsearch.xpack.security.action.user.ChangePasswordRequestBuilder;
import org.elasticsearch.xpack.security.authc.esnative.ReservedRealm;

import java.util.concurrent.Semaphore;

/**
 * This process adds a ClusterStateListener to the ClusterService that will listen for cluster state updates.
 * Once the cluster and the security index are ready, it will attempt to bootstrap the elastic user's
 * password with a password from the keystore. If the password is not in the keystore or the elastic user
 * already has a password, then the user's password will not be set. Once the process is complete, the
 * listener will remove itself.
 */
public final class BootstrapElasticPassword {

    private final Settings settings;
    private final Logger logger;
    private final ClusterService clusterService;
    private final ReservedRealm reservedRealm;
    private final SecurityLifecycleService lifecycleService;
    private final boolean reservedRealmDisabled;

    public BootstrapElasticPassword(Settings settings, Logger logger, ClusterService clusterService, ReservedRealm reservedRealm,
                                    SecurityLifecycleService lifecycleService) {
        this.reservedRealmDisabled = XPackSettings.RESERVED_REALM_ENABLED_SETTING.get(settings) == false;
        this.settings = settings;
        this.logger = logger;
        this.clusterService = clusterService;
        this.reservedRealm = reservedRealm;
        this.lifecycleService = lifecycleService;
    }

    public void initiatePasswordBootstrap() {
        SecureString bootstrapPassword = ReservedRealm.BOOTSTRAP_ELASTIC_PASSWORD.get(settings);
        if (bootstrapPassword.length() == 0) {
            return;
        } else if (reservedRealmDisabled) {
            logger.warn("elastic password will not be bootstrapped because the reserved realm is disabled");
            bootstrapPassword.close();
            return;
        }

        SecureString passwordHash = new SecureString(ChangePasswordRequestBuilder.validateAndHashPassword(bootstrapPassword));
        bootstrapPassword.close();

        clusterService.addListener(new BootstrapPasswordClusterStateListener(passwordHash));
    }

    private class BootstrapPasswordClusterStateListener implements ClusterStateListener {

        private final Semaphore semaphore = new Semaphore(1);
        private final SecureString passwordHash;
        private final SetOnce<Boolean> isDone = new SetOnce<>();

        private BootstrapPasswordClusterStateListener(SecureString passwordHash) {
            this.passwordHash = passwordHash;
        }

        @Override
        public void clusterChanged(ClusterChangedEvent event) {
            if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)
                    || lifecycleService.isSecurityIndexOutOfDate()
                    || (lifecycleService.isSecurityIndexExisting() && lifecycleService.isSecurityIndexAvailable() == false)
                    || lifecycleService.isSecurityIndexWriteable() == false) {
                // We hold off bootstrapping until the node recovery is complete, the security index is up to date, and
                // security index is writeable. If the security index currently exists, it must also be available.
                return;
            }

            // Only allow one attempt to bootstrap the password at a time
            if (semaphore.tryAcquire()) {
                // Ensure that we do not attempt to bootstrap after the process is complete. This is important as we
                // clear the password hash in the cleanup phase.
                if (isDone.get() != null) {
                    semaphore.release();
                    return;
                }

                reservedRealm.bootstrapElasticUserCredentials(passwordHash, new ActionListener<Boolean>() {
                    @Override
                    public void onResponse(Boolean passwordSet) {
                        cleanup();
                        if (passwordSet == false) {
                            logger.warn("elastic password was not bootstrapped because its password was already set");
                        }
                        semaphore.release();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        cleanup();
                        logger.error("unexpected exception when attempting to bootstrap password", e);
                        semaphore.release();
                    }
                });
            }
        }

        private void cleanup() {
            isDone.set(true);
            IOUtils.closeWhileHandlingException(() -> clusterService.removeListener(this), passwordHash);
        }
    }
}
