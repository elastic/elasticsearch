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
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSecrets;
import org.elasticsearch.common.settings.SecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.ReloadablePlugin;

public class ClusterStateSecretsListener implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(ClusterStateSecretsListener.class);
    private final Environment environment;
    private ReloadablePlugin reloadCallback;

    public ClusterStateSecretsListener(ClusterService clusterService, Environment environment) {
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

        ClusterSecrets previousSecrets = event.previousState().custom(ClusterSecrets.TYPE);
        ClusterSecrets currentSecrets = event.state().custom(ClusterSecrets.TYPE);

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
}
