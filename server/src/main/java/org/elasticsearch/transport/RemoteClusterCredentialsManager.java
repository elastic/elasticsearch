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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.transport.RemoteClusterService.REMOTE_CLUSTER_CREDENTIALS;

public class RemoteClusterCredentialsManager {
    private static final Logger logger = LogManager.getLogger(RemoteClusterCredentialsManager.class);

    private volatile Map<String, SecureString> clusterCredentials;

    public RemoteClusterCredentialsManager(Settings settings) {
        this.clusterCredentials = Collections.emptyMap();
        updateClusterCredentials(settings);
    }

    public synchronized UpdateRemoteClusterCredentialsResult updateClusterCredentials(Settings settings) {
        final Map<String, SecureString> newClusterCredentials = REMOTE_CLUSTER_CREDENTIALS.getAsMap(settings);
        if (clusterCredentials.isEmpty()) {
            setCredentialsAndLog(newClusterCredentials);
            return new UpdateRemoteClusterCredentialsResult(newClusterCredentials.keySet(), Collections.emptySet());
        }
        final Set<String> aliasesWithAddedCredentials = Sets.difference(newClusterCredentials.keySet(), clusterCredentials.keySet());
        final Set<String> aliasesWithRemovedCredentials = Sets.difference(clusterCredentials.keySet(), newClusterCredentials.keySet());
        setCredentialsAndLog(newClusterCredentials);
        assert Sets.haveEmptyIntersection(aliasesWithRemovedCredentials, aliasesWithAddedCredentials);
        return new UpdateRemoteClusterCredentialsResult(aliasesWithAddedCredentials, aliasesWithRemovedCredentials);
    }

    private void setCredentialsAndLog(Map<String, SecureString> newClusterCredentials) {
        clusterCredentials = newClusterCredentials;
        logger.debug(
            () -> Strings.format(
                "Updated remote cluster credentials for clusters: [%s]",
                Strings.collectionToCommaDelimitedString(clusterCredentials.keySet())
            )
        );
    }

    public record UpdateRemoteClusterCredentialsResult(Set<String> aliasesWithAddedCredentials, Set<String> aliasesWithRemovedCredentials) {
        int totalSize() {
            return aliasesWithAddedCredentials.size() + aliasesWithRemovedCredentials.size();
        }
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
