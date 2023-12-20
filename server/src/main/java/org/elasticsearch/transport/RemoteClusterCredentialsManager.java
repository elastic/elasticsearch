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
import java.util.SortedSet;
import java.util.TreeSet;

import static org.elasticsearch.transport.RemoteClusterService.REMOTE_CLUSTER_CREDENTIALS;

public class RemoteClusterCredentialsManager {
    private static final Logger logger = LogManager.getLogger(RemoteClusterCredentialsManager.class);

    private volatile Map<String, SecureString> clusterCredentials = Collections.emptyMap();

    public RemoteClusterCredentialsManager(Settings settings) {
        updateClusterCredentials(settings);
    }

    public final synchronized UpdateRemoteClusterCredentialsResult updateClusterCredentials(Settings settings) {
        final Map<String, SecureString> newClusterCredentials = REMOTE_CLUSTER_CREDENTIALS.getAsMap(settings);
        if (clusterCredentials.isEmpty()) {
            setClusterCredentialsAndLog(newClusterCredentials);
            return new UpdateRemoteClusterCredentialsResult(new TreeSet<>(newClusterCredentials.keySet()), Collections.emptySortedSet());
        }

        final SortedSet<String> addedClusterAliases = Sets.sortedDifference(newClusterCredentials.keySet(), clusterCredentials.keySet());
        final SortedSet<String> removedClusterAliases = Sets.sortedDifference(clusterCredentials.keySet(), newClusterCredentials.keySet());
        setClusterCredentialsAndLog(newClusterCredentials);
        assert Sets.haveEmptyIntersection(removedClusterAliases, addedClusterAliases);
        return new UpdateRemoteClusterCredentialsResult(addedClusterAliases, removedClusterAliases);
    }

    public record UpdateRemoteClusterCredentialsResult(
        // Use sorted sets since we will iterate over these, and call a synchronized method for each.
        // Sorting establishes a deterministic call order to prevent deadlocks
        SortedSet<String> addedClusterAliases,
        SortedSet<String> removedClusterAliases
    ) {}

    @Nullable
    public SecureString resolveCredentials(String clusterAlias) {
        return clusterCredentials.get(clusterAlias);
    }

    public boolean hasCredentials(String clusterAlias) {
        return clusterCredentials.containsKey(clusterAlias);
    }

    private void setClusterCredentialsAndLog(Map<String, SecureString> newClusterCredentials) {
        clusterCredentials = newClusterCredentials;
        logger.debug(
            () -> Strings.format(
                "Updated remote cluster credentials for clusters: [%s]",
                Strings.collectionToCommaDelimitedString(clusterCredentials.keySet())
            )
        );
    }

    public static final RemoteClusterCredentialsManager EMPTY = new RemoteClusterCredentialsManager(Settings.EMPTY);
}
