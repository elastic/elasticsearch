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

import org.elasticsearch.common.inject.Binder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.ExtensionPoint;
import org.elasticsearch.index.snapshots.IndexShardRepository;

/**
 * A mapping from type name to implementations of {@link Repository} and {@link IndexShardRepository}.
 */
public class RepositoryTypesRegistry {
    // invariant: repositories and shardRepositories have the same keyset
    private final ExtensionPoint.SelectedType<Repository> repositoryTypes =
        new ExtensionPoint.SelectedType<>("repository", Repository.class);
    private final ExtensionPoint.SelectedType<IndexShardRepository> shardRepositoryTypes =
        new ExtensionPoint.SelectedType<>("index_repository", IndexShardRepository.class);

    /** Adds a new repository type to the registry, bound to the given implementation classes. */
    public void registerRepository(String name, Class<? extends Repository> repositoryType, Class<? extends IndexShardRepository> shardRepositoryType) {
        repositoryTypes.registerExtension(name, repositoryType);
        shardRepositoryTypes.registerExtension(name, shardRepositoryType);
    }

    /**
     * Looks up the given type and binds the implementation into the given binder.
     * Throws an {@link IllegalArgumentException} if the given type does not exist.
     */
    public void bindType(Binder binder, String type) {
        Settings settings = Settings.builder().put("type", type).build();
        repositoryTypes.bindType(binder, settings, "type", null);
        shardRepositoryTypes.bindType(binder, settings, "type", null);
    }
}
