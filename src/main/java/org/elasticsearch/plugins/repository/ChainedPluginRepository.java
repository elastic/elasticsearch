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

package org.elasticsearch.plugins.repository;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Collection;

public class ChainedPluginRepository implements PluginRepository {

    private final String name;

    private final ImmutableMap<String, PluginRepository> repositories;

    private ChainedPluginRepository(String name, ImmutableMap<String, PluginRepository> repositories) {
        this.name = name;
        this.repositories = repositories;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Collection<PluginDescriptor> find(String name) {
        ImmutableList.Builder<PluginDescriptor> plugins = ImmutableList.builder();

        if (repositories != null) {
            for (PluginRepository repository : repositories.values()) {
                plugins.addAll(repository.find(name));
            }
        }
        return plugins.build();
    }

    @Override
    public PluginDescriptor find(String organisation, String name, String version) {
        if (repositories != null) {
            for (PluginRepository repository : repositories.values()) {
                PluginDescriptor plugin = repository.find(organisation, name, version);
                if (plugin != null) {
                    return plugin;
                }
            }
        }
        return null;
    }

    @Override
    public Collection<PluginDescriptor> list() {
        return list(new Filter() {
            @Override
            public boolean accept(PluginDescriptor plugin) {
                // Accepts all plugins
                return true;
            }
        });
    }

    @Override
    public Collection<PluginDescriptor> list(Filter filter) {
        ImmutableList.Builder<PluginDescriptor> plugins = ImmutableList.builder();

        if (repositories != null) {
            for (PluginRepository repository : repositories.values()) {
                plugins.addAll(repository.list(filter));
            }
        }
        return plugins.build();
    }

    @Override
    public void install(PluginDescriptor plugin) {
        if (repositories != null) {
            for (PluginRepository repository : repositories.values()) {
                repository.install(plugin);
            }
        }
    }

    @Override
    public void remove(PluginDescriptor plugin) {
        if (repositories != null) {
            for (PluginRepository repository : repositories.values()) {
                repository.remove(plugin);
            }
        }
    }

    public static class Builder {

        private String name;
        private ImmutableMap.Builder<String, PluginRepository> repositoriesBuilder = ImmutableMap.builder();

        private Builder(String name) {
            this.name = name;
        }

        public static Builder chain(String name) {
            return new Builder(name);
        }

        public Builder addRepository(PluginRepository repository) {
            if (repository != null) {
                repositoriesBuilder.put(repository.name(), repository);
            }
            return this;
        }

        public ChainedPluginRepository build() {
            return new ChainedPluginRepository(name, repositoriesBuilder.build());
        }
    }
}
