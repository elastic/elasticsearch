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
import org.elasticsearch.core.Nullable;

import java.util.Map;

import static org.elasticsearch.transport.RemoteClusterService.REMOTE_CLUSTER_CREDENTIALS;

public class RemoteClusterCredentialsManager {

    private static final Logger logger = LogManager.getLogger(RemoteClusterCredentialsManager.class);

    private volatile Map<String, SecureString> clusterCredentials;

    public RemoteClusterCredentialsManager(Settings settings) {
        updateClusterCredentials(settings);
    }

    public void updateClusterCredentials(Settings settings) {
        clusterCredentials = REMOTE_CLUSTER_CREDENTIALS.getAsMap(settings);
        logger.debug("Updated remote cluster credentials: [{}]", clusterCredentials.keySet());
    }

    @Nullable
    public SecureString resolveCredentials(String clusterAlias) {
        return clusterCredentials.get(clusterAlias);
    }

    public boolean hasCredentials(String clusterAlias) {
        return clusterCredentials.containsKey(clusterAlias);
    }

    public static final RemoteClusterCredentialsManager EMPTY = new RemoteClusterCredentialsManager(Settings.EMPTY);
}
