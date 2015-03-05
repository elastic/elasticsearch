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

import java.util.Collection;

/**
 * Interface for repository of plugins.
 */
public interface PluginRepository {

    /**
     * Returns the name of the plugin repository
     *
     * @return name of the plugin repository
     */
    String name();

    /**
     * Finds all plugins contained in the repository that have the given name.
     *
     * @param name name of the plugin
     * @return the list of plugins that have the given name.
     */
    Collection<PluginDescriptor> find(String name);

    /**
     * Finds the plugin contained in the repository that has the given
     * organisation, name and version.
     *
     * @param organisation organisation of the plugin
     * @param name name of the plugin
     * @param version version of the plugin
     *
     * @return the plugin that has the given organisation/name/version.
     */
    PluginDescriptor find(String organisation, String name, String version);

    /**
     * Return the list of all plugins contained in the repository.
     *
     * This may or may not be supported by the underlying repository. If not
     * supported, this method should return an empty list.
     *
     * @return the list of all plugins
     */
    Collection<PluginDescriptor> list();

    /**
     * Return the list of all plugins contained in the repository, filtered by
     * the given {@link Filter}.
     *
     * This may or may not be supported by the underlying repository. If not
     * supported, this method should return an empty list.
     *
     * @param filter the {@link Filter}
     * @return the list of all plugins, filtered
     */
    Collection<PluginDescriptor> list(Filter filter);

    /**
     * Installs a plugin in the repository.
     *
     * @param plugin the plugin to install
     */
    void install(PluginDescriptor plugin);

    /**
     * Removes the specified plugin.
     *
     * @param plugin the plugin to remove
     */
    void remove(PluginDescriptor plugin);

    /**
     * This interface is implemented by objects that decide if a plugin
     * contained in the repository should be accepted or filtered. A Filter
     * can be passed as the parameter of the {@link PluginRepository#list(Filter)}
     * method.
     */
    public static interface Filter {

        /**
         * Indicates if the given plugin entry should be accepted or filtered.
         *
         * @param plugin the {@link PluginDescriptor} to be tested
         * @return {@code true} if the plugin entry should be accepted
         */
        boolean accept(PluginDescriptor plugin);
    }
}
