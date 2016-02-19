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

package org.elasticsearch.index;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.query.IndicesQueriesRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * Simple provider class that holds the Index and Node level services used by
 * a shard.
 * This is just a temporary solution until we cleaned up index creation and removed injectors on that level as well.
 */
public final class NodeServicesProvider {

    private final ThreadPool threadPool;
    private final BigArrays bigArrays;
    private final Client client;
    private final IndicesQueriesRegistry indicesQueriesRegistry;
    private final ScriptService scriptService;
    private final CircuitBreakerService circuitBreakerService;

    @Inject
    public NodeServicesProvider(ThreadPool threadPool, BigArrays bigArrays, Client client, ScriptService scriptService, IndicesQueriesRegistry indicesQueriesRegistry, CircuitBreakerService circuitBreakerService) {
        this.threadPool = threadPool;
        this.bigArrays = bigArrays;
        this.client = client;
        this.indicesQueriesRegistry = indicesQueriesRegistry;
        this.scriptService = scriptService;
        this.circuitBreakerService = circuitBreakerService;
    }

    public ThreadPool getThreadPool() {
        return threadPool;
    }

    public BigArrays getBigArrays() { return bigArrays; }

    public Client getClient() {
        return client;
    }

    public IndicesQueriesRegistry getIndicesQueriesRegistry() {
        return indicesQueriesRegistry;
    }

    public ScriptService getScriptService() {
        return scriptService;
    }

    public CircuitBreakerService getCircuitBreakerService() {
        return circuitBreakerService;
    }
}
