/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.transport;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.transport.RemoteClusterService.REMOTE_CLUSTER_CREDENTIALS;

public class RemoteClusterCredentialsManager {

    private static final Logger logger = LogManager.getLogger(RemoteClusterCredentialsManager.class);

    private volatile Map<String, SecureString> clusterCredentials = new HashMap<>();

    public RemoteClusterCredentialsManager(Settings settings) {
        updateClusterCredentials(settings);
    }

    // TODO `synchronized` is heavy-handed here
    public synchronized UpdateResult updateClusterCredentials(Settings settings) {
        final Map<String, SecureString> credentialsToUpdate = REMOTE_CLUSTER_CREDENTIALS.getAsMap(settings);
        final Set<String> added = Sets.difference(credentialsToUpdate.keySet(), clusterCredentials.keySet());
        final Set<String> removed = Sets.difference(clusterCredentials.keySet(), credentialsToUpdate.keySet());
        clusterCredentials = credentialsToUpdate;
        logger.debug("Updated remote cluster credentials: [{}]", clusterCredentials.keySet());
        logger.info("Added remote cluster credentials for: [{}]", added);
        logger.info("Removed remote cluster credentials for: [{}]", removed);
        return new UpdateResult(added, removed);
    }

    public record UpdateResult(Set<String> added, Set<String> removed) {}

    @Nullable
    public SecureString resolveCredentials(String clusterAlias) {
        return clusterCredentials.get(clusterAlias);
    }

    public boolean hasCredentials(String clusterAlias) {
        return clusterCredentials.containsKey(clusterAlias);
    }

    public static final RemoteClusterCredentialsManager EMPTY = new RemoteClusterCredentialsManager(Settings.EMPTY);
}
