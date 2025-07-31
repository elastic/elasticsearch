/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;

import java.util.List;
import java.util.function.Consumer;

public interface LinkedClusterConnectionConfigListener {

    void listen(Consumer<LinkedClusterConnectionConfig> consumer);

    class ClusterSettingsListener implements LinkedClusterConnectionConfigListener {
        private final ClusterSettings clusterSettings;

        public ClusterSettingsListener(ClusterSettings clusterSettings) {
            this.clusterSettings = clusterSettings;
        }

        @Override
        public void listen(Consumer<LinkedClusterConnectionConfig> consumer) {
            List<Setting.AffixSetting<?>> remoteClusterSettings = List.of(
                RemoteClusterService.REMOTE_CLUSTER_COMPRESS,
                RemoteClusterService.REMOTE_CLUSTER_PING_SCHEDULE,
                RemoteConnectionStrategy.REMOTE_CONNECTION_MODE,
                SniffConnectionStrategy.REMOTE_CLUSTERS_PROXY,
                SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS,
                SniffConnectionStrategy.REMOTE_NODE_CONNECTIONS,
                ProxyConnectionStrategy.PROXY_ADDRESS,
                ProxyConnectionStrategy.REMOTE_SOCKET_CONNECTIONS,
                ProxyConnectionStrategy.SERVER_NAME
            );
            clusterSettings.addAffixGroupUpdateConsumer(
                remoteClusterSettings,
                (alias, settings) -> consumer.accept(new LinkedClusterConnectionConfig(alias, settings))
            );
        }
    }

}
