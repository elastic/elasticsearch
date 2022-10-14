/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.transport.TcpTransport;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.elasticsearch.transport.RemoteClusterService.REMOTE_CLUSTER_AUTHORIZATION;

/**
 * Load/reload API Keys from cluster settings.
 * Injected into SecurityServerTransportInterceptor.
 * Latest API Key value needs to be used in header when sending TransportRequest to remote cluster nodes.
 */
public class CrossClusterSecurity {

    private static final Logger LOGGER = LogManager.getLogger(CrossClusterSecurity.class);

    private final Map<String, String> apiKeys = ConcurrentCollections.newConcurrentMap();

    public CrossClusterSecurity(final Settings settings, final ClusterSettings clusterSettings) {
        if (TcpTransport.isUntrustedRemoteClusterEnabled()) {
            this.setApiKeys(REMOTE_CLUSTER_AUTHORIZATION.getAsMap(settings));
            clusterSettings.addAffixMapUpdateConsumer(REMOTE_CLUSTER_AUTHORIZATION, this::setApiKeys, (k, v) -> {});
        }
    }

    public String getApiKey(final String clusterAlias) {
        if (TcpTransport.isUntrustedRemoteClusterEnabled()) {
            return this.apiKeys.get(clusterAlias);
        } else {
            return null;
        }
    }

    private void setApiKeys(final Map<String, String> identityHashMap) {
        // Workaround: Copy entries from IdentityHashMap to HashMap, so Map.containsKey() works as expected
        final HashMap<String, String> newClusterAliasApiKeyMap = new HashMap<>(identityHashMap);
        if (TcpTransport.isUntrustedRemoteClusterEnabled()) {
            final Collection<String> added = newClusterAliasApiKeyMap.keySet()
                .stream()
                .filter(clusterAlias -> this.apiKeys.containsKey(clusterAlias) == false)
                .toList();
            final Collection<String> removed = this.apiKeys.keySet()
                .stream()
                .filter(clusterAlias -> newClusterAliasApiKeyMap.containsKey(clusterAlias) == false)
                .toList();
            final Collection<String> changed = this.apiKeys.entrySet()
                .stream()
                .filter(
                    clusterAliasApiKey -> newClusterAliasApiKeyMap.containsKey(clusterAliasApiKey.getKey())
                        && Objects.equals(clusterAliasApiKey.getValue(), newClusterAliasApiKeyMap.get(clusterAliasApiKey.getKey())) == false
                )
                .map(Map.Entry::getKey)
                .toList();
            final Collection<String> unchanged = this.apiKeys.entrySet()
                .stream()
                .filter(
                    clusterAliasAndApiKey -> newClusterAliasApiKeyMap.containsKey(clusterAliasAndApiKey.getKey())
                        && Objects.equals(clusterAliasAndApiKey.getValue(), newClusterAliasApiKeyMap.get(clusterAliasAndApiKey.getKey()))
                )
                .map(Map.Entry::getKey)
                .toList();
            LOGGER.info(
                "Old: {}, New: {}, Added: {}, Removed: {}, Changed: {}, Unchanged: {}",
                new TreeMap<>(this.apiKeys),
                new TreeMap<>(newClusterAliasApiKeyMap),
                new TreeSet<>(added),
                new TreeSet<>(removed),
                new TreeSet<>(changed),
                new TreeSet<>(unchanged)
            );

            removed.forEach(this.apiKeys::remove); // removed
            this.apiKeys.putAll(newClusterAliasApiKeyMap); // added, changed, and unchanged
        }
    }
}
