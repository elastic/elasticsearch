/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.threadpool.ThreadPool;
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

    public RepositoriesModule(Environment env, List<RepositoryPlugin> repoPlugins, TransportService transportService,
                              ClusterService clusterService, ThreadPool threadPool, NamedXContentRegistry namedXContentRegistry,
                              RecoverySettings recoverySettings) {
        Map<String, Repository.Factory> factories = new HashMap<>();
        factories.put(FsRepository.TYPE, metadata -> new FsRepository(metadata, env, namedXContentRegistry, clusterService,
            recoverySettings));

        for (RepositoryPlugin repoPlugin : repoPlugins) {
            Map<String, Repository.Factory> newRepoTypes = repoPlugin.getRepositories(env, namedXContentRegistry, clusterService,
                recoverySettings);
            for (Map.Entry<String, Repository.Factory> entry : newRepoTypes.entrySet()) {
                if (factories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Repository type [" + entry.getKey() + "] is already registered");
                }
            }
        }

        Map<String, Repository.Factory> internalFactories = new HashMap<>();
        for (RepositoryPlugin repoPlugin : repoPlugins) {
            Map<String, Repository.Factory> newRepoTypes = repoPlugin.getInternalRepositories(env, namedXContentRegistry, clusterService,
                recoverySettings);
            for (Map.Entry<String, Repository.Factory> entry : newRepoTypes.entrySet()) {
                if (internalFactories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Internal repository type [" + entry.getKey() + "] is already registered");
                }
                if (factories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Internal repository type [" + entry.getKey() + "] is already registered as a " +
                        "non-internal repository");
                }
            }
        }

        Settings settings = env.settings();
        Map<String, Repository.Factory> repositoryTypes = Collections.unmodifiableMap(factories);
        Map<String, Repository.Factory> internalRepositoryTypes = Collections.unmodifiableMap(internalFactories);
        repositoriesService = new RepositoriesService(settings, clusterService, transportService, repositoryTypes,
            internalRepositoryTypes, threadPool);
    }

    public RepositoriesService getRepositoryService() {
        return repositoriesService;
    }
}
