/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.apm;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.NetworkPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public class APM extends Plugin implements NetworkPlugin {
    private final SetOnce<APMTracer> tracer = new SetOnce<>();
    private final Settings settings;

    public APM(Settings settings) {
        this.settings = settings;
    }

    @Override
    public Collection<Object> createComponents(
        Client client,
        ClusterService clusterService,
        ThreadPool threadPool,
        ResourceWatcherService resourceWatcherService,
        ScriptService scriptService,
        NamedXContentRegistry xContentRegistry,
        Environment environment,
        NodeEnvironment nodeEnvironment,
        NamedWriteableRegistry namedWriteableRegistry,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        final APMAgentSettings apmAgentSettings = new APMAgentSettings();
        final APMTracer apmTracer = new APMTracer(settings, clusterService);

        apmAgentSettings.syncAgentSystemProperties(settings);
        apmAgentSettings.addClusterSettingsListeners(clusterService, apmTracer);

        tracer.set(apmTracer);
        return List.of(apmTracer);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            APMAgentSettings.APM_ENABLED_SETTING,
            APMAgentSettings.APM_TRACING_NAMES_INCLUDE_SETTING,
            APMAgentSettings.APM_TRACING_NAMES_EXCLUDE_SETTING,
            APMAgentSettings.APM_AGENT_SETTINGS,
            APMAgentSettings.APM_TOKEN_SETTING
        );
    }
}
