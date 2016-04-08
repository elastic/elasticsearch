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

import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;

/**
 * Binds repository classes for the specific repository type.
 */
public class RepositoryModule extends AbstractModule {

    private RepositoryName repositoryName;

    private final Settings globalSettings;

    private final Settings settings;

    private final RepositoryTypesRegistry typesRegistry;

    /**
     * Spawns module for repository with specified name, type and settings
     *
     * @param repositoryName repository name and type
     * @param settings       repository settings
     * @param globalSettings global settings
     * @param typesRegistry  registry of repository types
     */
    public RepositoryModule(RepositoryName repositoryName, Settings settings, Settings globalSettings, RepositoryTypesRegistry typesRegistry) {
        this.repositoryName = repositoryName;
        this.globalSettings = globalSettings;
        this.settings = settings;
        this.typesRegistry = typesRegistry;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure() {
        typesRegistry.bindType(binder(), repositoryName.type());
        bind(RepositorySettings.class).toInstance(new RepositorySettings(globalSettings, settings));
    }
}
