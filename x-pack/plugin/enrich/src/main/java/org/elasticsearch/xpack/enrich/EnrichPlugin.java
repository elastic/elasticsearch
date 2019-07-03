/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.ListEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.action.CoordinatorProxyAction;
import org.elasticsearch.xpack.enrich.action.TransportDeleteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.action.TransportExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.action.TransportGetEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.action.TransportListEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.action.TransportPutEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.rest.RestDeleteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.rest.RestExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.rest.RestGetEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.rest.RestListEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.rest.RestPutEnrichPolicyAction;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.XPackSettings.ENRICH_ENABLED_SETTING;

public class EnrichPlugin extends Plugin implements ActionPlugin, IngestPlugin {

    static final Setting<Integer> ENRICH_FETCH_SIZE_SETTING =
        Setting.intSetting("index.xpack.enrich.fetch_size", 10000, 1, 1000000, Setting.Property.NodeScope);

    public static final Setting<Integer> COORDINATOR_PROXY_MAX_CONCURRENT_REQUESTS =
        Setting.intSetting("enrich.coordinator_proxy.max_concurrent_requests", 8, 1, 10000, Setting.Property.NodeScope);

    public static final Setting<Integer> COORDINATOR_PROXY_MAX_LOOKUPS_PER_REQUEST =
        Setting.intSetting("enrich.coordinator_proxy.max_lookups_per_request", 128, 1, 10000, Setting.Property.NodeScope);

    private static final String QUEUE_CAPACITY_SETTING_NAME = "enrich.coordinator_proxy.queue_capacity";
    public static final Setting<Integer> COORDINATOR_PROXY_QUEUE_CAPACITY = new Setting<>(QUEUE_CAPACITY_SETTING_NAME,
            settings -> {
                int maxConcurrentRequests = COORDINATOR_PROXY_MAX_CONCURRENT_REQUESTS.get(settings);
                int maxLookupsPerRequest = COORDINATOR_PROXY_MAX_LOOKUPS_PER_REQUEST.get(settings);
                return String.valueOf(maxConcurrentRequests * maxLookupsPerRequest);
            },
            val -> Setting.parseInt(val, 1, Integer.MAX_VALUE, QUEUE_CAPACITY_SETTING_NAME),
            Setting.Property.NodeScope);

    private final Settings settings;
    private final Boolean enabled;

    public EnrichPlugin(final Settings settings) {
        this.settings = settings;
        this.enabled = ENRICH_ENABLED_SETTING.get(settings);
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        EnrichProcessorFactory factory = new EnrichProcessorFactory(parameters.client);
        parameters.ingestService.addIngestClusterStateListener(factory);
        return Collections.singletonMap(EnrichProcessorFactory.TYPE, factory);
    }

    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled == false) {
            return Collections.emptyList();
        }

        return Arrays.asList(
            new ActionHandler<>(GetEnrichPolicyAction.INSTANCE, TransportGetEnrichPolicyAction.class),
            new ActionHandler<>(DeleteEnrichPolicyAction.INSTANCE, TransportDeleteEnrichPolicyAction.class),
            new ActionHandler<>(ListEnrichPolicyAction.INSTANCE, TransportListEnrichPolicyAction.class),
            new ActionHandler<>(PutEnrichPolicyAction.INSTANCE, TransportPutEnrichPolicyAction.class),
            new ActionHandler<>(ExecuteEnrichPolicyAction.INSTANCE, TransportExecuteEnrichPolicyAction.class),
            new ActionHandler<>(CoordinatorProxyAction.INSTANCE, CoordinatorProxyAction.TransportAction.class)
        );
    }

    public List<RestHandler> getRestHandlers(Settings settings, RestController restController, ClusterSettings clusterSettings,
                                             IndexScopedSettings indexScopedSettings, SettingsFilter settingsFilter,
                                             IndexNameExpressionResolver indexNameExpressionResolver,
                                             Supplier<DiscoveryNodes> nodesInCluster) {
        if (enabled == false) {
            return Collections.emptyList();
        }

        return Arrays.asList(
            new RestGetEnrichPolicyAction(settings, restController),
            new RestDeleteEnrichPolicyAction(settings, restController),
            new RestListEnrichPolicyAction(settings, restController),
            new RestPutEnrichPolicyAction(settings, restController),
            new RestExecuteEnrichPolicyAction(settings, restController)
        );
    }

    @Override
    public Collection<Object> createComponents(Client client, ClusterService clusterService, ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService, ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry, Environment environment,
                                               NodeEnvironment nodeEnvironment, NamedWriteableRegistry namedWriteableRegistry) {
        EnrichPolicyExecutor enrichPolicyExecutor = new EnrichPolicyExecutor(settings, clusterService, client, threadPool,
            new IndexNameExpressionResolver(), System::currentTimeMillis);
        return Arrays.asList(enrichPolicyExecutor, new CoordinatorProxyAction.Coordinator(client, settings));
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return Arrays.asList(
            new NamedWriteableRegistry.Entry(MetaData.Custom.class, EnrichMetadata.TYPE, EnrichMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, EnrichMetadata.TYPE,
                in -> EnrichMetadata.readDiffFrom(MetaData.Custom.class, EnrichMetadata.TYPE, in))
        );
    }

    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return Arrays.asList(
            new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(EnrichMetadata.TYPE), EnrichMetadata::fromXContent)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return Arrays.asList(
            ENRICH_FETCH_SIZE_SETTING,
            COORDINATOR_PROXY_MAX_CONCURRENT_REQUESTS,
            COORDINATOR_PROXY_MAX_LOOKUPS_PER_REQUEST,
            COORDINATOR_PROXY_QUEUE_CAPACITY
        );
    }
}
