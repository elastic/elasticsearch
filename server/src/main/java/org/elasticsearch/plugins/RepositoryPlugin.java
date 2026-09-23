/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.snapshots.RestoreLifecycleListener;
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
        RecoverySettings recoverySettings,
        RepositoriesMetrics repositoriesMetrics,
        SnapshotMetrics snapshotMetrics
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
    default BiConsumer<Snapshot, IndexVersion> addPreRestoreVersionCheck() {
        return null;
    }

    /**
     * Returns the listener this plugin installs on restore initialization and completion, or {@link RestoreLifecycleListener#NOOP} to
     * install none. At most one plugin may install a listener.
     * <p>
     * This is pulled from the plugin rather than handed to it because {@link org.elasticsearch.snapshots.RestoreService} is constructed
     * after {@link Plugin#createComponents}, so a plugin cannot be given the service to register with. A plugin whose listener needs
     * components of its own should build the listener in {@code createComponents} and return it here.
     * <p>
     * The returned listener runs inside master-service cluster-state updates and so must not block or perform I/O.
     */
    default RestoreLifecycleListener getRestoreLifecycleListener() {
        return RestoreLifecycleListener.NOOP;
    }

}
