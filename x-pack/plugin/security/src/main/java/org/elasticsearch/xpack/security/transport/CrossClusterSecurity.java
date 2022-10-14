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
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.transport.TcpTransport;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;
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

    private void setApiKeys(final Map<String, String> possibleIdentityHashMap) {
        final Map<String, String> newClusterAliasApiKeyMap = convertToRegularMap(possibleIdentityHashMap);
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
            "Added: {}, Removed: {}, Changed: {}, Unchanged: {}, Old: {}, New: {}",
            new TreeSet<>(added),
            new TreeSet<>(removed),
            new TreeSet<>(changed),
            new TreeSet<>(unchanged),
            new TreeMap<>(this.apiKeys),
            new TreeMap<>(newClusterAliasApiKeyMap)
        );

        removed.forEach(this.apiKeys::remove); // removed
        this.apiKeys.putAll(newClusterAliasApiKeyMap); // added, changed, and unchanged
    }

    // Java Strings are internally encoded as UTF-16, so no conversion needed
    // If we used UTF-8, that would add two unnecessary conversion steps (UTF16 => UTF-8 => UTF-16).
    private static final Charset INTERMEDIATE_CHARSET = StandardCharsets.UTF_16;

    public static Map<String, String> convertToRegularMap(Map<String, String> map) {
        if (map instanceof IdentityHashMap == false) {
            return map;
        }
        // Copy all keys/values to a HashMap, but replace intern String keys with non-intern String keys
        final Map<String, String> nonInternMap;
        nonInternMap = new HashMap<>();
        for (final Map.Entry<String, String> entry : map.entrySet()) {
            final String internKey = entry.getKey();
            final byte[] keyBytes = internKey.getBytes(INTERMEDIATE_CHARSET); // copy bytes to a new, non-intern String object
            final String nonInternKey = new String(keyBytes, 0, keyBytes.length, INTERMEDIATE_CHARSET);
            nonInternMap.put(nonInternKey, entry.getValue());
        }
        return nonInternMap;
    }
}
