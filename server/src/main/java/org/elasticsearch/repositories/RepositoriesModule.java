/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sets up classes for Snapshot/Restore.
 */
public final class RepositoriesModule {

    private final RepositoriesService repositoriesService;

    public RepositoriesModule(
        Environment env,
        List<RepositoryPlugin> repoPlugins,
        TransportService transportService,
        ClusterService clusterService,
        BigArrays bigArrays,
        NamedXContentRegistry namedXContentRegistry,
        RecoverySettings recoverySettings
    ) {
        Map<String, Repository.Factory> factories = new HashMap<>();
        factories.put(
            FsRepository.TYPE,
            metadata -> new FsRepository(metadata, env, namedXContentRegistry, clusterService, bigArrays, recoverySettings)
        );

        for (RepositoryPlugin repoPlugin : repoPlugins) {
            Map<String, Repository.Factory> newRepoTypes = repoPlugin.getRepositories(
                env,
                namedXContentRegistry,
                clusterService,
                bigArrays,
                recoverySettings
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

        Settings settings = env.settings();
        Map<String, Repository.Factory> repositoryTypes = Collections.unmodifiableMap(factories);
        Map<String, Repository.Factory> internalRepositoryTypes = Collections.unmodifiableMap(internalFactories);
        repositoriesService = new RepositoriesService(
            settings,
            clusterService,
            transportService,
            repositoryTypes,
            internalRepositoryTypes,
            transportService.getThreadPool()
        );
    }

    public RepositoriesService getRepositoryService() {
        return repositoriesService;
    }
}
