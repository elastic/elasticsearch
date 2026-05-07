/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.settings.secure;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.internal.ReloadAwarePlugin;

import java.util.Collection;
import java.util.List;

public class SecureSettingsPlugin extends Plugin implements ReloadAwarePlugin {

    private ClusterStateSecretsListener clusterStateSecretsListener;

    @Override
    public Collection<?> createComponents(PluginServices services) {
        this.clusterStateSecretsListener = new ClusterStateSecretsListener(services.clusterService(), services.environment());
        return List.of(clusterStateSecretsListener);
    }

    @Override
    public void setReloadCallback(ReloadablePlugin reloadablePlugin) {
        this.clusterStateSecretsListener.setReloadCallback(reloadablePlugin);
    }
}
