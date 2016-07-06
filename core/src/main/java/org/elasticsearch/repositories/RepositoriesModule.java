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
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.repositories.uri.URLRepository;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotShardsService;
import org.elasticsearch.snapshots.SnapshotsService;

/**
 * Sets up classes for Snapshot/Restore.
 *
 * Plugins can add custom repository types by calling {@link #registerRepository(String, Class)}.
 */
public class RepositoriesModule extends AbstractModule {

    private final RepositoryTypesRegistry repositoryTypes = new RepositoryTypesRegistry();

    public RepositoriesModule() {
        registerRepository(FsRepository.TYPE, FsRepository.class);
        registerRepository(URLRepository.TYPE, URLRepository.class);
    }

    /** Registers a custom repository type to the given {@link Repository}. */
    public void registerRepository(String type, Class<? extends Repository> repositoryType) {
        repositoryTypes.registerRepository(type, repositoryType);
    }

    @Override
    protected void configure() {
        bind(RepositoriesService.class).asEagerSingleton();
        bind(SnapshotsService.class).asEagerSingleton();
        bind(SnapshotShardsService.class).asEagerSingleton();
        bind(TransportNodesSnapshotsStatus.class).asEagerSingleton();
        bind(RestoreService.class).asEagerSingleton();
        bind(RepositoryTypesRegistry.class).toInstance(repositoryTypes);
    }
}
