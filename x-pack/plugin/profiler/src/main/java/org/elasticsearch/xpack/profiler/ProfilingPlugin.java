/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.MultiBucketConsumerService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

public class ProfilingPlugin extends Plugin implements ActionPlugin {
    private static final Logger logger = LogManager.getLogger(ProfilingPlugin.class);
    public static final Setting<Boolean> PROFILING_ENABLED = Setting.boolSetting(
        "xpack.profiling.enabled",
        false,
        Setting.Property.NodeScope
    );
    private static final int REQUIRED_MAX_BUCKETS = 150_000;
    private final Settings settings;
    private final boolean enabled;

    public ProfilingPlugin(Settings settings) {
        this.settings = settings;
        this.enabled = PROFILING_ENABLED.get(settings);
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
        AllocationDeciders allocationDeciders
    ) {
        logger.info("Profiling is {}", enabled ? "enabled" : "disabled");
        return super.createComponents(
            client,
            clusterService,
            threadPool,
            resourceWatcherService,
            scriptService,
            xContentRegistry,
            environment,
            nodeEnvironment,
            namedWriteableRegistry,
            indexNameExpressionResolver,
            repositoriesServiceSupplier,
            tracer,
            allocationDeciders
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        final Settings settings,
        final RestController restController,
        final ClusterSettings clusterSettings,
        final IndexScopedSettings indexScopedSettings,
        final SettingsFilter settingsFilter,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<DiscoveryNodes> nodesInCluster
    ) {
        if (enabled) {
            return singletonList(new RestGetProfilingAction());
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(PROFILING_ENABLED);
    }

    @Override
    public Settings additionalSettings() {
        // workaround until https://github.com/elastic/elasticsearch/issues/91776 is implemented
        final Settings.Builder builder = Settings.builder();
        if (enabled) {
            if (MultiBucketConsumerService.MAX_BUCKET_SETTING.exists(settings) == false) {
                logger.debug("Overriding [{}] to [{}].", MultiBucketConsumerService.MAX_BUCKET_SETTING, REQUIRED_MAX_BUCKETS);
                builder.put(MultiBucketConsumerService.MAX_BUCKET_SETTING.getKey(), REQUIRED_MAX_BUCKETS);
            } else {
                Integer configuredMaxBuckets = MultiBucketConsumerService.MAX_BUCKET_SETTING.get(settings);
                if (configuredMaxBuckets != null && configuredMaxBuckets < REQUIRED_MAX_BUCKETS) {
                    final String message = String.format(
                        Locale.ROOT,
                        "Profiling requires [%s] to be set at least to [%d] but was configured to [%d].",
                        MultiBucketConsumerService.MAX_BUCKET_SETTING.getKey(),
                        REQUIRED_MAX_BUCKETS,
                        configuredMaxBuckets
                    );
                    throw new IllegalArgumentException(message);
                }
            }
        }
        return builder.build();
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(GetProfilingAction.INSTANCE, TransportGetProfilingAction.class));
    }
}
