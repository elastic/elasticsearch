/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSecrets;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.ReloadablePlugin;

import java.util.concurrent.atomic.AtomicBoolean;

public class ClusterStateSecretsListener implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ClusterStateSecretsListener.class);
    private final ClusterService clusterService;
    private final Environment environment;
    private ReloadablePlugin reloadCallback;

    private final AtomicBoolean requiresCleanup = new AtomicBoolean(true);

    public ClusterStateSecretsListener(ClusterService clusterService, Environment environment) {
        this.clusterService = clusterService;
        this.environment = environment;
        clusterService.addListener(this);
    }

    public void setReloadCallback(ReloadablePlugin reloadCallback) {
        if (this.reloadCallback != null) {
            throw new IllegalStateException("Cannot set reload callback twice");
        }
        this.reloadCallback = reloadCallback;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        assert reloadCallback != null : "Cluster state secrets listener has not been initialized";

        ClusterState state = event.state();
        if (state.clusterRecovered() && state.nodes().isLocalNodeElectedMaster()) {
            ClusterStateSecretsMetadata legacyMetadata = state.custom(ClusterStateSecretsMetadata.TYPE);
            if (legacyMetadata != null && requiresCleanup.compareAndSet(true, false)) {
                removeClusterStateSecretsMetadata();
            }
        }

        ClusterSecrets previousSecrets = event.previousState().custom(ClusterSecrets.TYPE);
        ClusterSecrets currentSecrets = state.custom(ClusterSecrets.TYPE);

        if (currentSecrets == null) {
            return;
        }

        if (previousSecrets == null || currentSecrets.getVersion() > previousSecrets.getVersion()) {
            SecureSettings secrets = currentSecrets.getSettings();
            try {
                reloadCallback.reload(Settings.builder().put(environment.settings(), false).setSecureSettings(secrets).build());
            } catch (Exception e) {
                logger.warn("Failed to reload secure settings from file", e);
            }
        }
    }

    @SuppressForbidden(reason = "removing ClusterStateSecretsMetadata exactly once")
    private void removeClusterStateSecretsMetadata() {
        try {
            clusterService.submitUnbatchedStateUpdateTask("removeClusterStateSecretsMetadata", new ClusterStateUpdateTask() {

                @Override
                public ClusterState execute(ClusterState currentState) {
                    if (currentState.custom(ClusterStateSecretsMetadata.TYPE) == null) {
                        return currentState;
                    }
                    logger.info("Removing obsolete [file_secure_settings_metadata] from cluster custom state");
                    return ClusterState.builder(currentState).removeCustom(ClusterStateSecretsMetadata.TYPE).build();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("Failed to remove obsolete [file_secure_settings_metadata] from cluster custom state", e);
                    requiresCleanup.set(true);
                }
            });
        } catch (RuntimeException e) {
            logger.warn("Failed to remove obsolete [file_secure_settings_metadata] from cluster custom state", e);
            requiresCleanup.set(true);
        }
    }
}
