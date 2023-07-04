/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.runtimefields;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

/**
 * The plugin class for all the runtime fields common functionality that requires large dependencies.
 * This plugin sets up the environment for the grok function to work in painless as part of the different
 * runtime fields contexts.
 */
public final class RuntimeFieldsCommonPlugin extends Plugin {

    static final Setting<TimeValue> GROK_WATCHDOG_INTERVAL = Setting.timeSetting(
        "runtime_fields.grok.watchdog.interval",
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );
    static final Setting<TimeValue> GROK_WATCHDOG_MAX_EXECUTION_TIME = Setting.timeSetting(
        "runtime_fields.grok.watchdog.max_execution_time",
        TimeValue.timeValueSeconds(1),
        Setting.Property.NodeScope
    );

    private final NamedGroupExtractor.GrokHelper grokHelper;

    public RuntimeFieldsCommonPlugin(Settings settings) {
        grokHelper = new NamedGroupExtractor.GrokHelper(
            GROK_WATCHDOG_INTERVAL.get(settings),
            GROK_WATCHDOG_MAX_EXECUTION_TIME.get(settings)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(GROK_WATCHDOG_INTERVAL, GROK_WATCHDOG_MAX_EXECUTION_TIME);
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
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        Tracer tracer,
        AllocationService allocationService,
        IndicesService indicesService
    ) {
        grokHelper.finishInitializing(threadPool);
        return List.of();
    }

    public NamedGroupExtractor.GrokHelper grokHelper() {
        return grokHelper;
    }
}
