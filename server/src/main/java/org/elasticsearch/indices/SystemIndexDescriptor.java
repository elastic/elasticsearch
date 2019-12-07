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

package org.elasticsearch.indices;

/**
 * Describes a system index. Provides the information required to create and maintain the system index.
 */
public class SystemIndexDescriptor {
    private final String name;
    private final String sourcePluginName;

    /**
     *
     * @param indexName The name of the system index.
     * @param sourcePluginName The name of the plugin responsible for this system index.
     */
    public SystemIndexDescriptor(String indexName, String sourcePluginName) {
        this.name = indexName;
        this.sourcePluginName = sourcePluginName;
    }

    /**
     * Get the name of the system index.
     * @return The name of the system index.
     */
    public String getIndexName() {
        return name;
    }

    /**
     * Get the name of the plugin responsible for this system index. It is recommended, but not required, that this is
     * the name of the class implementing {@link org.elasticsearch.plugins.SystemIndexPlugin}.
     * @return The name of the plugin responsible for this system index.
     */
    public String getSourcePluginName() {
        return sourcePluginName;
    }

    // TODO: Index settings and mapping
    // TODO: getThreadpool()
    // TODO: Upgrade handling (reindex script?)
}
