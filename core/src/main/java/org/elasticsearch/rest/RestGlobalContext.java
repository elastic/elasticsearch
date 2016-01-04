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

import java.util.Set;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
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

    public RestGlobalContext(Settings settings, RestController controller, Client client, IndicesQueriesRegistry indicesQueriesRegistry) {
        this.settings = settings;
        this.controller = controller;
        this.client = client;
        this.indicesQueriesRegistry = indicesQueriesRegistry;
    }

    public Settings getSettings() {
        return settings;
    }

    /**
     * Returns the REST headers that get copied over from a
     * {@link org.elasticsearch.rest.RestRequest} to its corresponding
     * {@link org.elasticsearch.transport.TransportRequest}(s). By default no
     * headers get copied but it is possible to extend this behaviour via
     * plugins by calling
     * {@link RestController#registerRelevantHeaders(String...)}.
     */
    public Set<String> relevantHeaders() {
        return controller.relevantHeaders();
    }

    public Client getClient() {
        return client;
    }

    public IndicesQueriesRegistry getIndicesQueriesRegistry() {
        return indicesQueriesRegistry;
    }
}
