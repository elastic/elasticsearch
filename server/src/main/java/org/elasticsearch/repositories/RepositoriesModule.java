/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories;

import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotRestoreException;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Sets up classes for Snapshot/Restore.
 */
public final class RepositoriesModule {

    private final RepositoriesService repositoriesService;

    public RepositoriesModule(
        Environment env,
        List<RepositoryPlugin> repoPlugins,
        NodeClient client,
        ThreadPool threadPool,
        ClusterService clusterService,
        BigArrays bigArrays,
        NamedXContentRegistry namedXContentRegistry,
        RecoverySettings recoverySettings,
        TelemetryProvider telemetryProvider
    ) {
        final RepositoriesMetrics repositoriesMetrics = new RepositoriesMetrics(telemetryProvider.getMeterRegistry());
        Map<String, Repository.Factory> factories = new HashMap<>();
        factories.put(
            FsRepository.TYPE,
            (projectId, metadata) -> new FsRepository(projectId, metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings)
        );

        for (RepositoryPlugin repoPlugin : repoPlugins) {
            Map<String, Repository.Factory> newRepoTypes = repoPlugin.getRepositories(
                env,
                namedXContentRegistry,
                clusterService,
                bigArrays,
                recoverySettings,
                repositoriesMetrics
            );
            for (Map.Entry<String, Repository.Factory> entry : newRepoTypes.entrySet()) {
                if (factories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Repository type [" + entry.getKey() + "] is already registered");
                }
            }
        }

        Map<String, Repository.Factory> internalFactories = new HashMap<>();
        for (RepositoryPlugin repoPlugin : repoPlugins) {
            Map<String, Repository.Factory> newRepoTypes = repoPlugin.getInternalRepositories(
                env,
                namedXContentRegistry,
                clusterService,
                recoverySettings
            );
            for (Map.Entry<String, Repository.Factory> entry : newRepoTypes.entrySet()) {
                if (internalFactories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Internal repository type [" + entry.getKey() + "] is already registered");
                }
                if (factories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException(
                        "Internal repository type [" + entry.getKey() + "] is already registered as a " + "non-internal repository"
                    );
                }
            }
        }

        List<BiConsumer<Snapshot, IndexVersion>> preRestoreChecks = new ArrayList<>();
        for (RepositoryPlugin repoPlugin : repoPlugins) {
            BiConsumer<Snapshot, IndexVersion> preRestoreCheck = repoPlugin.addPreRestoreVersionCheck();
            if (preRestoreCheck != null) {
                preRestoreChecks.add(preRestoreCheck);
            }
        }
        if (preRestoreChecks.isEmpty()) {
            preRestoreChecks.add((snapshot, version) -> {
                // pre-restore checks will be run against the version in which the snapshot was created as well as
                // the version in which the restored index was created
                if (version.before(IndexVersions.MINIMUM_COMPATIBLE)) {
                    throw new SnapshotRestoreException(
                        snapshot,
                        "the snapshot was created with Elasticsearch version ["
                            + version.toReleaseVersion()
                            + "] which is below the current versions minimum index compatibility version ["
                            + IndexVersions.MINIMUM_COMPATIBLE.toReleaseVersion()
                            + "]"
                    );
                }
            });
        }

        Settings settings = env.settings();
        Map<String, Repository.Factory> repositoryTypes = Collections.unmodifiableMap(factories);
        Map<String, Repository.Factory> internalRepositoryTypes = Collections.unmodifiableMap(internalFactories);
        repositoriesService = new RepositoriesService(
            settings,
            clusterService,
            repositoryTypes,
            internalRepositoryTypes,
            threadPool,
            client,
            preRestoreChecks
        );
    }

    public RepositoriesService getRepositoryService() {
        return repositoriesService;
    }
}
