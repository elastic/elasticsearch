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
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.transport.RemoteClusterService.REMOTE_CLUSTER_AUTHORIZATION;

public class RemoteClusterCredentialsResolver {

    private static final Logger LOGGER = LogManager.getLogger(RemoteClusterCredentialsResolver.class);

    private final Map<String, String> apiKeys = ConcurrentCollections.newConcurrentMap();

    public RemoteClusterCredentialsResolver(final Settings settings, final ClusterSettings clusterSettings) {
        if (TcpTransport.isUntrustedRemoteClusterEnabled()) {
            for (final Map.Entry<String, String> entry : REMOTE_CLUSTER_AUTHORIZATION.getAsMap(settings).entrySet()) {
                if (Strings.isEmpty(entry.getValue()) == false) {
                    update(entry.getKey(), entry.getValue());
                }
            }
            clusterSettings.addAffixUpdateConsumer(REMOTE_CLUSTER_AUTHORIZATION, this::update, (clusterAlias, authorization) -> {});
        }
    }

    public Optional<RemoteClusterCredentials> resolve(final String clusterAlias) {
        if (TcpTransport.isUntrustedRemoteClusterEnabled()) {
            final String apiKey = apiKeys.get(clusterAlias);
            return apiKey == null
                ? Optional.empty()
                : Optional.of(new RemoteClusterCredentials(clusterAlias, ApiKeyService.withApiKeyPrefix(apiKey)));
        }
        return Optional.empty();
    }

    private void update(final String clusterAlias, final String authorization) {
        if (Strings.isEmpty(authorization)) {
            apiKeys.remove(clusterAlias);
            LOGGER.debug("Credentials value for cluster alias [{}] removed", clusterAlias);
        } else {
            final boolean notFound = Strings.isEmpty(apiKeys.put(clusterAlias, authorization));
            LOGGER.debug("Credentials value for cluster alias [{}] {}", clusterAlias, (notFound ? "added" : "updated"));
        }
    }

    record RemoteClusterCredentials(String clusterAlias, String credentials) {}
}
