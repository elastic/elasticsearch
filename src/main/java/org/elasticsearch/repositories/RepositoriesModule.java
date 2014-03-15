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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.elasticsearch.action.admin.cluster.snapshots.status.TransportNodesSnapshotsStatus;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.repositories.fs.FsRepositoryModule;
import org.elasticsearch.repositories.uri.URLRepository;
import org.elasticsearch.repositories.uri.URLRepositoryModule;
import org.elasticsearch.snapshots.RestoreService;
import org.elasticsearch.snapshots.SnapshotsService;

import java.util.Map;

/**
 * Module responsible for registering other repositories.
 * <p/>
 * Repositories implemented as plugins should implement {@code onModule(RepositoriesModule module)} method, in which
 * they should register repository using {@link #registerRepository(String, Class)} method.
 */
public class RepositoriesModule extends AbstractModule {

    private Map<String, Class<? extends Module>> repositoryTypes = Maps.newHashMap();

    public RepositoriesModule() {
        registerRepository(FsRepository.TYPE, FsRepositoryModule.class);
        registerRepository(URLRepository.TYPE, URLRepositoryModule.class);
    }

    /**
     * Registers a custom repository type name against a module.
     *
     * @param type   The type
     * @param module The module
     */
    public void registerRepository(String type, Class<? extends Module> module) {
        repositoryTypes.put(type, module);
    }

    @Override
    protected void configure() {
        bind(RepositoriesService.class).asEagerSingleton();
        bind(SnapshotsService.class).asEagerSingleton();
        bind(TransportNodesSnapshotsStatus.class).asEagerSingleton();
        bind(RestoreService.class).asEagerSingleton();
        bind(RepositoryTypesRegistry.class).toInstance(new RepositoryTypesRegistry(ImmutableMap.copyOf(repositoryTypes)));
    }
}
