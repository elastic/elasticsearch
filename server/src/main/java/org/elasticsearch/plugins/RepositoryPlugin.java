/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * An extension point for {@link Plugin} implementations to add custom snapshot repositories.
 */
public interface RepositoryPlugin {

    /**
     * Returns repository types added by this plugin.
     *
     * @param env The environment for the local node, which may be used for the local settings and path.repo
     *
     * The key of the returned {@link Map} is the type name of the repository and
     * the value is a factory to construct the {@link Repository} interface.
     */
    default Map<String, Repository.Factory> getRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        BigArrays bigArrays,
        RecoverySettings recoverySettings
    ) {
        return Collections.emptyMap();
    }

    /**
     * Returns internal repository types added by this plugin. Internal repositories cannot be registered
     * through the external API.
     *
     * @param env The environment for the local node, which may be used for the local settings and path.repo
     *
     * The key of the returned {@link Map} is the type name of the repository and
     * the value is a factory to construct the {@link Repository} interface.
     */
    default Map<String, Repository.Factory> getInternalRepositories(
        Environment env,
        NamedXContentRegistry namedXContentRegistry,
        ClusterService clusterService,
        RecoverySettings recoverySettings
    ) {
        return Collections.emptyMap();
    }

    /**
     * Returns a check that is run on restore. This allows plugins to prevent certain restores from happening.
     *
     * returns null if no check is provided
     */
    default BiConsumer<Snapshot, Version> addPreRestoreVersionCheck() {
        return null;
    }

}
