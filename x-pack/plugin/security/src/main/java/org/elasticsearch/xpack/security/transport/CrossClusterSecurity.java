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
import org.elasticsearch.common.util.StringLiteralDeduplicator;
import org.elasticsearch.common.util.StringLiteralOutturn;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.transport.TcpTransport;

import java.util.Collection;
import java.util.IdentityHashMap;
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

    /**
     * Initialize load and reload REMOTE_CLUSTER_AUTHORIZATION values.
     * @param settings Contains zero, one, or many values of REMOTE_CLUSTER_AUTHORIZATION literal values.
     * @param clusterSettings Contains one affix of setting REMOTE_CLUSTER_AUTHORIZATION.
     */
    public CrossClusterSecurity(final Settings settings, final ClusterSettings clusterSettings) {
        if (TcpTransport.isUntrustedRemoteClusterEnabled()) {
            // Settings.getAsMap returns HashMap which is passed to setApiKeys.
            this.setApiKeys(REMOTE_CLUSTER_AUTHORIZATION.getAsMap(settings));
            // ClusterSettings.addAffixMapUpdateConsumer registers setApiKeys to receive IdentityHashMap from Settings.
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

    public static final StringLiteralDeduplicator x = new StringLiteralDeduplicator();

    private void setApiKeys(final Map<String, String> map) {
        final Map<String, String> newClusterAliasApiKeyMap = (map instanceof IdentityHashMap)
            ? StringLiteralOutturn.MapStringKey.outturn(map)
            : map;
        final Collection<String> added = newClusterAliasApiKeyMap.entrySet()
            .stream()
            .filter(clusterAliasApiKey -> this.apiKeys.containsKey(clusterAliasApiKey.getKey()) == false)
            .map(Object::toString)
            .toList();
        final Collection<String> removed = this.apiKeys.entrySet()
            .stream()
            .filter(clusterAliasApiKey -> newClusterAliasApiKeyMap.containsKey(clusterAliasApiKey.getKey()) == false)
            .map(Object::toString)
            .toList();
        final Collection<String> changed = newClusterAliasApiKeyMap.entrySet()
            .stream()
            .filter(
                clusterAliasApiKey -> this.apiKeys.containsKey(clusterAliasApiKey.getKey())
                    && Objects.equals(clusterAliasApiKey.getValue(), this.apiKeys.get(clusterAliasApiKey.getKey())) == false
            )
            .map(Object::toString)
            .toList();
        final Collection<String> unchanged = newClusterAliasApiKeyMap.entrySet()
            .stream()
            .filter(
                clusterAliasAndApiKey -> this.apiKeys.containsKey(clusterAliasAndApiKey.getKey())
                    && Objects.equals(clusterAliasAndApiKey.getValue(), this.apiKeys.get(clusterAliasAndApiKey.getKey()))
            )
            .map(Object::toString)
            .toList();
        LOGGER.info(
            "Changed: {}, Added: {}, Removed: {}, Unchanged: {}, Old: {}, New: {}",
            new TreeSet<>(changed),
            new TreeSet<>(added),
            new TreeSet<>(removed),
            new TreeSet<>(unchanged),
            new TreeMap<>(this.apiKeys),
            new TreeMap<>(newClusterAliasApiKeyMap)
        );

        removed.forEach(this.apiKeys::remove); // removed
        this.apiKeys.putAll(newClusterAliasApiKeyMap); // added, changed, and unchanged
    }

}
