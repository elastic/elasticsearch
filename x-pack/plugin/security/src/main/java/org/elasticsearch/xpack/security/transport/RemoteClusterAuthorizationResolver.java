/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.transport.TcpTransport;

import java.util.Map;

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
            for (final Map.Entry<String, String> entry : REMOTE_CLUSTER_AUTHORIZATION.getAsMap(settings).entrySet()) {
                this.updateAuthorization(entry.getKey(), entry.getValue());
            }
            // ClusterSettings.addAffixMapUpdateConsumer registers setApiKeys to receive IdentityHashMap from Settings.
            clusterSettings.addAffixUpdateConsumer(REMOTE_CLUSTER_AUTHORIZATION, this::updateAuthorization, (k, v) -> {});
        }
    }

    public String resolveAuthorization(final String clusterAlias) {
        if (TcpTransport.isUntrustedRemoteClusterEnabled()) {
            return this.apiKeys.get(clusterAlias);
        }
        return null;
    }

    private void updateAuthorization(final String clusterAlias, final String authorization) {
        if (Strings.isEmpty(authorization)) {
            final boolean notFound = Strings.isEmpty(apiKeys.remove(clusterAlias));
            LOGGER.debug("clusterAlias {} {}", clusterAlias, (notFound ? "not found" : "removed"));
        } else {
            final boolean notFound = Strings.isEmpty(apiKeys.put(clusterAlias, authorization));
            LOGGER.debug("clusterAlias {} {}", clusterAlias, (notFound ? "added" : "updated"));
        }
    }
}
