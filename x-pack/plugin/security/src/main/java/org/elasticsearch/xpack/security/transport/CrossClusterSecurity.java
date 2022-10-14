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

    private void setApiKeys(final Map<String, String> map) {
        final Map<String, String> newClusterAliasApiKeyMap = (map instanceof IdentityHashMap) ? internKeysToNonIntern(map) : map;
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

    /**
     * Copy all entries to a new Map, but use new non-intern copies of the String keys.
     * For example, this can be used to convert an IdentityHashMap to a Map which complies with the equals(), get(), etc Map APIs.
     * @param map Map with String keys that are assumed to be interned.
     * @return New Map with non-intern String keys.
     */
    public static Map<String, String> internKeysToNonIntern(Map<String, String> map) {
        final Map<String, String> nonInternMap;
        nonInternMap = new HashMap<>();
        for (final Map.Entry<String, String> entry : map.entrySet()) {
            nonInternMap.put(internToNonIntern(entry.getKey()), entry.getValue());
        }
        return nonInternMap;
    }

    /**
     * Copy intern String characters to a new non-intern String object.
     * @param internString String that is assumed to be interned.
     * @return New String object which is not interned.
     */
    public static String internToNonIntern(final String internString) {
        return internToNonIntern(internString, DEFAULT_INTERMEDIATE_CHARSET);
    }

    public static String internToNonIntern(final String internString, final Charset intermediateCharset) {
        final byte[] keyBytes = internString.getBytes(intermediateCharset); // copy bytes to a new, non-intern String object
        return new String(keyBytes, 0, keyBytes.length, intermediateCharset);
    }

    /**
     * Use UTF-16 encoded chars to avoid unnecessary conversions.
     * Java Strings are internally encoded as UTF-16, so u
     * If using something else like UTF-8, that leads to two unnecessary conversions.
     * First UTF16 to UTF8, then UTF8 to UTF-16.
     */
    private static final Charset DEFAULT_INTERMEDIATE_CHARSET = StandardCharsets.UTF_16;

}
