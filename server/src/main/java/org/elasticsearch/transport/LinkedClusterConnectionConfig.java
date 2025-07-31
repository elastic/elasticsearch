/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.common.settings.Settings;

import java.util.List;

import static org.elasticsearch.transport.RemoteConnectionStrategy.REMOTE_CONNECTION_MODE;

public class LinkedClusterConnectionConfig {
    private final String clusterAlias;
    private final String proxyAddress;
    // we would not actually store settings in the final version
    private final Settings settings;

    public LinkedClusterConnectionConfig(String clusterAlias, Settings settings) {
        this.clusterAlias = clusterAlias;
        this.proxyAddress = ProxyConnectionStrategy.PROXY_ADDRESS.get(settings);
        this.settings = settings;
    }

    // this would be called by the CPS ProjectCustom based implementation
    public LinkedClusterConnectionConfig(String clusterAlias, String proxyAddress) {
        this.clusterAlias = clusterAlias;
        this.proxyAddress = proxyAddress;
        this.settings = null;
    }

    public List<String> getSeeds() {
        return SniffConnectionStrategy.REMOTE_CLUSTER_SEEDS.getConcreteSettingForNamespace(clusterAlias).get(settings);
    }

    public String getProxyAddress() {
        return proxyAddress;
    }

    public RemoteConnectionStrategy.ConnectionStrategy getConnectionMode() {
        return REMOTE_CONNECTION_MODE.getConcreteSettingForNamespace(clusterAlias).get(settings);
    }

    public Settings getSettings() {
        return settings;
    }

    public String getClusterAlias() {
        return clusterAlias;
    }
}
