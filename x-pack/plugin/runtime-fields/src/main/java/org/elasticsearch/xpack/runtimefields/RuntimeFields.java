/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.runtimefields.mapper.NamedGroupExtractor;
import org.elasticsearch.xpack.runtimefields.mapper.NamedGroupExtractor.GrokHelper;

import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

public final class RuntimeFields extends Plugin {

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

    public RuntimeFields(Settings settings) {
        grokHelper = new NamedGroupExtractor.GrokHelper(
            GROK_WATCHDOG_INTERVAL.get(settings),
            GROK_WATCHDOG_MAX_EXECUTION_TIME.get(settings)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return org.elasticsearch.common.collect.List.of(GROK_WATCHDOG_INTERVAL, GROK_WATCHDOG_MAX_EXECUTION_TIME);
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
        grokHelper.finishInitializing(threadPool);
        return org.elasticsearch.common.collect.List.of();
    }

    public GrokHelper grokHelper() {
        return grokHelper;
    }
}
