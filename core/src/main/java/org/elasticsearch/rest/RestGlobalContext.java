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

package org.elasticsearch.rest;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;

/**
 * Context passed to all Rest actions on construction. Think carefully before
 * adding things to this because they can be seen by all rest actions.
 */
public class RestGlobalContext {
    private final Settings settings;
    private final RestController controller;
    private final Client client;
    private final IndicesQueriesRegistry indicesQueriesRegistry;
    private final IndexNameExpressionResolver indexNameExpressionResolver;
    private final SettingsFilter settingsFilter;

    public RestGlobalContext(Settings settings, RestController controller, Client client, IndicesQueriesRegistry indicesQueriesRegistry,
            IndexNameExpressionResolver indexNameExpressionResolver, SettingsFilter settingsFilter) {
        this.settings = settings;
        this.controller = controller;
        this.client = client;
        this.indicesQueriesRegistry = indicesQueriesRegistry;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
        this.settingsFilter = settingsFilter;
    }

    public Settings getSettings() {
        return settings;
    }

    public RestController getController() {
        return controller;
    }

    public Client getClient() {
        return client;
    }

    public IndicesQueriesRegistry getIndicesQueriesRegistry() {
        return indicesQueriesRegistry;
    }

    public IndexNameExpressionResolver getIndexNameExpressionResolver() {
        return indexNameExpressionResolver;
    }

    public SettingsFilter getSettingsFilter() {
        return settingsFilter;
    }
}
