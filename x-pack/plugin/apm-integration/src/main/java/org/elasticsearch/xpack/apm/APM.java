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
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

public class APM extends Plugin implements NetworkPlugin {
    public static final Set<String> TRACE_HEADERS = Set.of(Task.TRACE_PARENT_HTTP_HEADER, Task.TRACE_STATE);

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
        tracer.set(new APMTracer(settings, threadPool, clusterService));
        return List.of(tracer.get());
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            APMTracer.APM_ENABLED_SETTING,
            APMTracer.APM_ENDPOINT_SETTING,
            APMTracer.APM_TOKEN_SETTING,
            APMTracer.APM_TRACING_NAMES_INCLUDE_SETTING,
            APMTracer.APM_SAMPLE_RATE_SETTING
        );
    }
}
