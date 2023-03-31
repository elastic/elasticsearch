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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.security.authc.ApiKeyService;

import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.transport.RemoteClusterService.REMOTE_CLUSTER_CREDENTIALS;

public class RemoteClusterCredentialsResolver {

    private static final Logger logger = LogManager.getLogger(RemoteClusterCredentialsResolver.class);

    private final Map<String, SecureString> clusterCredentials;

    public RemoteClusterCredentialsResolver(final Settings settings) {
        this.clusterCredentials = REMOTE_CLUSTER_CREDENTIALS.getAsMap(settings);
        logger.debug(
            "Read cluster credentials for remote clusters [{}]",
            Strings.collectionToCommaDelimitedString(clusterCredentials.keySet())
        );
    }

    public Optional<RemoteClusterCredentials> resolve(final String clusterAlias) {
        final SecureString apiKey = clusterCredentials.get(clusterAlias);
        if (apiKey == null) {
            return Optional.empty();
        } else {
            return Optional.of(new RemoteClusterCredentials(clusterAlias, ApiKeyService.withApiKeyPrefix(apiKey.toString())));
        }
    }

    record RemoteClusterCredentials(String clusterAlias, String credentials) {
        @Override
        public String toString() {
            return "RemoteClusterCredentials{clusterAlias='" + clusterAlias + "', credentials='::es_redacted::'}";
        }
    }
}
