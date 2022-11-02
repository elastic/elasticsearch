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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.elasticsearch.transport.RemoteClusterService.REMOTE_CLUSTER_AUTHORIZATION;

/**
 * Load/reload API Keys from cluster settings.
 * Injected into SecurityServerTransportInterceptor.
 * Latest API Key value needs to be used in header when sending TransportRequest to remote cluster nodes.
 */
public class RemoteClusterAuthorizationResolver {

    private static final Logger LOGGER = LogManager.getLogger(RemoteClusterAuthorizationResolver.class);

    private final Map<String, String> apiKeys = ConcurrentCollections.newConcurrentMap();

    /**
     * Initialize load and reload REMOTE_CLUSTER_AUTHORIZATION values.
     * @param settings Contains zero, one, or many values of REMOTE_CLUSTER_AUTHORIZATION literal values.
     * @param clusterSettings Contains one affix of setting REMOTE_CLUSTER_AUTHORIZATION.
     */
    public RemoteClusterAuthorizationResolver(final Settings settings, final ClusterSettings clusterSettings) {
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

    private void setApiKeys(final Map<String, String> newApiKeys) {
        final Collection<Map.Entry<String, String>> added = new ArrayList<>();
        final Collection<Map.Entry<String, String>> removed = new ArrayList<>();
        final Collection<Map.Entry<String, String>> changed = new ArrayList<>();
        final Collection<Map.Entry<String, String>> unchanged = new ArrayList<>();
        for (final Map.Entry<String, String> newEntry : newApiKeys.entrySet()) {
            if (this.apiKeys.containsKey(newEntry.getKey()) == false) {
                added.add(newEntry);
            } else if (Objects.equals(newEntry.getValue(), this.apiKeys.get(newEntry.getKey()))) {
                unchanged.add(newEntry);
            } else {
                changed.add(newEntry);
            }
        }
        for (final Map.Entry<String, String> oldEntry : this.apiKeys.entrySet()) {
            if (newApiKeys.containsKey(oldEntry.getKey()) == false) {
                removed.add(oldEntry);
            }
        }
        LOGGER.info(
            "\nNew: {}\nOld: {}\nChanged: {}\nAdded: {}\nRemoved: {}\nUnchanged: {}",
            toStringCsv(newApiKeys.entrySet()),
            toStringCsv(this.apiKeys.entrySet()),
            toStringCsv(changed),
            toStringCsv(added),
            toStringCsv(removed),
            toStringCsv(unchanged)
        );
        added.forEach(e -> this.apiKeys.put(e.getKey(), e.getValue())); // process added
        changed.forEach(e -> this.apiKeys.put(e.getKey(), e.getValue())); // process changed
        removed.forEach(e -> this.apiKeys.remove(e.getKey())); // process removed
    }

    private static String toStringCsv(Collection<Map.Entry<String, String>> collection) {
        return collection.stream().map(Object::toString).collect(Collectors.toCollection(TreeSet::new)).toString();
    }
}
