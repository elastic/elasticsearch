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

import org.elasticsearch.action.admin.cluster.snapshots.status.TransportNodesSnapshotsStatus;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sets up classes for Snapshot/Restore.
 */
public class RepositoriesModule extends AbstractModule {

    private final RepositoriesService repositoriesService;

    public RepositoriesModule(Environment env, List<RepositoryPlugin> repoPlugins, TransportService transportService,
                              ClusterService clusterService, ThreadPool threadPool, NamedXContentRegistry namedXContentRegistry) {
        Map<String, Repository.Factory> factories = new HashMap<>();
        factories.put(FsRepository.TYPE, (metadata) -> new FsRepository(metadata, env, namedXContentRegistry));

        for (RepositoryPlugin repoPlugin : repoPlugins) {
            Map<String, Repository.Factory> newRepoTypes = repoPlugin.getRepositories(env, namedXContentRegistry);
            for (Map.Entry<String, Repository.Factory> entry : newRepoTypes.entrySet()) {
                if (factories.put(entry.getKey(), entry.getValue()) != null) {
                    throw new IllegalArgumentException("Repository type [" + entry.getKey() + "] is already registered");
                }
            }
        }

        Map<String, Repository.Factory> internalFactories = new HashMap<>();
        for (RepositoryPlugin repoPlugin : repoPlugins) {
            Map<String, Repository.Factory> newRepoTypes = repoPlugin.getInternalRepositories(env, namedXContentRegistry);
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

        Map<String, Repository.Factory> repositoryTypes = Collections.unmodifiableMap(factories);
        Map<String, Repository.Factory> internalRepositoryTypes = Collections.unmodifiableMap(internalFactories);
        repositoriesService = new RepositoriesService(env.settings(), clusterService, transportService, repositoryTypes,
            internalRepositoryTypes, threadPool);
    }

    @Override
    protected void configure() {
        bind(RepositoriesService.class).toInstance(repositoriesService);
        bind(SnapshotsService.class).asEagerSingleton();
        bind(SnapshotShardsService.class).asEagerSingleton();
        bind(TransportNodesSnapshotsStatus.class).asEagerSingleton();
        bind(RestoreService.class).asEagerSingleton();
    }
}
