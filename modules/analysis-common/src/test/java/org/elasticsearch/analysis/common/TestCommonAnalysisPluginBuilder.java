/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.analysis.common;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.MockScriptService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestCommonAnalysisPluginBuilder {
    private final ThreadPool threadPool;

    private ScriptService scriptService = null;
    private Client client = null;
    private CircuitBreakerService circuitBreakerService = null;

    public TestCommonAnalysisPluginBuilder(ThreadPool threadPool) {
        this.threadPool = threadPool;
    }

    public TestCommonAnalysisPluginBuilder scriptService(ScriptService scriptService) {
        this.scriptService = scriptService;
        return this;
    }

    public TestCommonAnalysisPluginBuilder client(Client client) {
        this.client = client;
        return this;
    }

    public TestCommonAnalysisPluginBuilder circuitBreakerService(CircuitBreakerService circuitBreakerService) {
        this.circuitBreakerService = circuitBreakerService;
        return this;
    }

    public CommonAnalysisPlugin build() {
        ScriptService scriptService = this.scriptService != null
            ? this.scriptService
            : new MockScriptService(Settings.EMPTY, Map.of(), Map.of());
        Client client = this.client != null ? this.client : new NoOpClient(this.threadPool);
        CircuitBreakerService circuitBreakerService = this.circuitBreakerService != null
            ? this.circuitBreakerService
            : new NoneCircuitBreakerService();

        IndicesService indicesService = mock(IndicesService.class);
        when(indicesService.getCircuitBreakerService()).thenReturn(circuitBreakerService);

        Plugin.PluginServices pluginServices = mock(Plugin.PluginServices.class);
        when(pluginServices.scriptService()).thenReturn(scriptService);
        when(pluginServices.client()).thenReturn(client);
        when(pluginServices.indicesService()).thenReturn(indicesService);

        CommonAnalysisPlugin plugin = new CommonAnalysisPlugin();
        plugin.createComponents(pluginServices);

        return plugin;
    }
}
