/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.Nullable;

import java.util.Collection;
import java.util.List;

/**
 * A {@link LinkedProjectConfigService} implementation that listens for {@link ClusterSettings} changes,
 * creating {@link LinkedProjectConfig}s from the relevant settings and notifying registered listeners of updates.
 */
public class ClusterSettingsLinkedProjectConfigService extends AbstractLinkedProjectConfigService {
    private final Settings settings;
    private final ProjectResolver projectResolver;

    /**
     * Constructs a new {@link ClusterSettingsLinkedProjectConfigService}.
     *
     * @param settings        The initial node settings available on startup, used in {@link #getInitialLinkedProjectConfigs()}.
     * @param clusterSettings The {@link ClusterSettings} to add setting update consumers to, if non-null.
     * @param projectResolver The {@link ProjectResolver} to use to resolve the origin project ID.
     */
    @SuppressWarnings("this-escape")
    public ClusterSettingsLinkedProjectConfigService(
        Settings settings,
        @Nullable ClusterSettings clusterSettings,
        ProjectResolver projectResolver
    ) {
        this.settings = settings;
        this.projectResolver = projectResolver;
        if (clusterSettings != null) {
            List<Setting.AffixSetting<?>> remoteClusterSettings = List.of(
                RemoteClusterSettings.REMOTE_CLUSTER_COMPRESS,
                RemoteClusterSettings.REMOTE_CLUSTER_PING_SCHEDULE,
                RemoteClusterSettings.REMOTE_CONNECTION_MODE,
                RemoteClusterSettings.REMOTE_CLUSTER_SKIP_UNAVAILABLE,
                RemoteClusterSettings.SniffConnectionStrategySettings.REMOTE_CLUSTERS_PROXY,
                RemoteClusterSettings.SniffConnectionStrategySettings.REMOTE_CLUSTER_SEEDS,
                RemoteClusterSettings.SniffConnectionStrategySettings.REMOTE_NODE_CONNECTIONS,
                RemoteClusterSettings.ProxyConnectionStrategySettings.PROXY_ADDRESS,
                RemoteClusterSettings.ProxyConnectionStrategySettings.REMOTE_SOCKET_CONNECTIONS,
                RemoteClusterSettings.ProxyConnectionStrategySettings.SERVER_NAME
            );
            clusterSettings.addAffixGroupUpdateConsumer(remoteClusterSettings, this::settingsChangedCallback);
        }
    }

    @Override
    @FixForMultiProject(description = "Refactor to add the linked project IDs associated with the aliases.")
    public Collection<LinkedProjectConfig> getInitialLinkedProjectConfigs() {
        return RemoteClusterSettings.getRemoteClusters(settings)
            .stream()
            .filter(alias -> RemoteClusterSettings.isConnectionEnabled(alias, settings))
            .map(alias -> RemoteClusterSettings.toConfig(projectResolver.getProjectId(), ProjectId.DEFAULT, alias, settings))
            .toList();
    }

    @FixForMultiProject(description = "Refactor to add the linked project ID associated with the alias.")
    private void settingsChangedCallback(String clusterAlias, Settings newSettings) {
        final var mergedSettings = Settings.builder().put(settings, false).put(newSettings, false).build();
        if (RemoteClusterSettings.isConnectionEnabled(clusterAlias, mergedSettings)) {
            handleUpdate(RemoteClusterSettings.toConfig(projectResolver.getProjectId(), ProjectId.DEFAULT, clusterAlias, mergedSettings));
        } else {
            handleRemoved(projectResolver.getProjectId(), ProjectId.DEFAULT, clusterAlias);
        }
    }
}
