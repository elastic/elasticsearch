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
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.action.EnrichCoordinatorProxyAction;
import org.elasticsearch.xpack.enrich.action.EnrichCoordinatorStatsAction;
import org.elasticsearch.xpack.enrich.action.EnrichInfoTransportAction;
import org.elasticsearch.xpack.enrich.action.EnrichShardMultiSearchAction;
import org.elasticsearch.xpack.enrich.action.TransportDeleteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.action.TransportEnrichStatsAction;
import org.elasticsearch.xpack.enrich.action.TransportExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.action.TransportGetEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.action.TransportPutEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.rest.RestDeleteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.rest.RestEnrichStatsAction;
import org.elasticsearch.xpack.enrich.rest.RestExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.rest.RestGetEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.rest.RestPutEnrichPolicyAction;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.XPackSettings.ENRICH_ENABLED_SETTING;

public class EnrichPlugin extends Plugin implements ActionPlugin, IngestPlugin {

    static final Setting<Integer> ENRICH_FETCH_SIZE_SETTING = Setting.intSetting(
        "enrich.fetch_size",
        10000,
        1,
        1000000,
        Setting.Property.NodeScope
    );

    static final Setting<Integer> ENRICH_MAX_CONCURRENT_POLICY_EXECUTIONS = Setting.intSetting(
        "enrich.max_concurrent_policy_executions",
        50,
        1,
        Setting.Property.NodeScope
    );

    static final Setting<TimeValue> ENRICH_CLEANUP_PERIOD = Setting.timeSetting(
        "enrich.cleanup_period",
        new TimeValue(15, TimeUnit.MINUTES),
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> COORDINATOR_PROXY_MAX_CONCURRENT_REQUESTS = Setting.intSetting(
        "enrich.coordinator_proxy.max_concurrent_requests",
        8,
        1,
        10000,
        Setting.Property.NodeScope
    );

    public static final Setting<Integer> COORDINATOR_PROXY_MAX_LOOKUPS_PER_REQUEST = Setting.intSetting(
        "enrich.coordinator_proxy.max_lookups_per_request",
        128,
        1,
        10000,
        Setting.Property.NodeScope
    );

    static final Setting<Integer> ENRICH_MAX_FORCE_MERGE_ATTEMPTS = Setting.intSetting(
        "enrich.max_force_merge_attempts",
        3,
        1,
        10,
        Setting.Property.NodeScope
    );

    private static final String QUEUE_CAPACITY_SETTING_NAME = "enrich.coordinator_proxy.queue_capacity";
    public static final Setting<Integer> COORDINATOR_PROXY_QUEUE_CAPACITY = new Setting<>(QUEUE_CAPACITY_SETTING_NAME, settings -> {
        int maxConcurrentRequests = COORDINATOR_PROXY_MAX_CONCURRENT_REQUESTS.get(settings);
        int maxLookupsPerRequest = COORDINATOR_PROXY_MAX_LOOKUPS_PER_REQUEST.get(settings);
        return String.valueOf(maxConcurrentRequests * maxLookupsPerRequest);
    }, val -> Setting.parseInt(val, 1, Integer.MAX_VALUE, QUEUE_CAPACITY_SETTING_NAME), Setting.Property.NodeScope);

    private final Settings settings;
    private final Boolean enabled;

    public EnrichPlugin(final Settings settings) {
        this.settings = settings;
        this.enabled = ENRICH_ENABLED_SETTING.get(settings);
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        if (enabled == false) {
            return Map.of();
        }

        EnrichProcessorFactory factory = new EnrichProcessorFactory(parameters.client, parameters.scriptService);
        parameters.ingestService.addIngestClusterStateListener(factory);
        return Map.of(EnrichProcessorFactory.TYPE, factory);
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        if (enabled == false) {
            return List.of(new ActionHandler<>(XPackInfoFeatureAction.ENRICH, EnrichInfoTransportAction.class));
        }

        return List.of(
            new ActionHandler<>(XPackInfoFeatureAction.ENRICH, EnrichInfoTransportAction.class),
            new ActionHandler<>(GetEnrichPolicyAction.INSTANCE, TransportGetEnrichPolicyAction.class),
            new ActionHandler<>(DeleteEnrichPolicyAction.INSTANCE, TransportDeleteEnrichPolicyAction.class),
            new ActionHandler<>(PutEnrichPolicyAction.INSTANCE, TransportPutEnrichPolicyAction.class),
            new ActionHandler<>(ExecuteEnrichPolicyAction.INSTANCE, TransportExecuteEnrichPolicyAction.class),
            new ActionHandler<>(EnrichStatsAction.INSTANCE, TransportEnrichStatsAction.class),
            new ActionHandler<>(EnrichCoordinatorProxyAction.INSTANCE, EnrichCoordinatorProxyAction.TransportAction.class),
            new ActionHandler<>(EnrichShardMultiSearchAction.INSTANCE, EnrichShardMultiSearchAction.TransportAction.class),
            new ActionHandler<>(EnrichCoordinatorStatsAction.INSTANCE, EnrichCoordinatorStatsAction.TransportAction.class)
        );
    }

    public List<RestHandler> getRestHandlers(
        Settings settings,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster
    ) {
        if (enabled == false) {
            return List.of();
        }

        return List.of(
            new RestGetEnrichPolicyAction(restController),
            new RestDeleteEnrichPolicyAction(restController),
            new RestPutEnrichPolicyAction(restController),
            new RestExecuteEnrichPolicyAction(restController),
            new RestEnrichStatsAction(restController)
        );
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
        NamedWriteableRegistry namedWriteableRegistry
    ) {
        if (enabled == false) {
            return List.of();
        }

        EnrichPolicyLocks enrichPolicyLocks = new EnrichPolicyLocks();
        EnrichPolicyMaintenanceService enrichPolicyMaintenanceService = new EnrichPolicyMaintenanceService(
            settings,
            client,
            clusterService,
            threadPool,
            enrichPolicyLocks
        );
        enrichPolicyMaintenanceService.initialize();
        return List.of(enrichPolicyLocks, new EnrichCoordinatorProxyAction.Coordinator(client, settings), enrichPolicyMaintenanceService);
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(MetaData.Custom.class, EnrichMetadata.TYPE, EnrichMetadata::new),
            new NamedWriteableRegistry.Entry(
                NamedDiff.class,
                EnrichMetadata.TYPE,
                in -> EnrichMetadata.readDiffFrom(MetaData.Custom.class, EnrichMetadata.TYPE, in)
            )
        );
    }

    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(MetaData.Custom.class, new ParseField(EnrichMetadata.TYPE), EnrichMetadata::fromXContent)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            ENRICH_ENABLED_SETTING,
            ENRICH_FETCH_SIZE_SETTING,
            ENRICH_MAX_CONCURRENT_POLICY_EXECUTIONS,
            ENRICH_CLEANUP_PERIOD,
            COORDINATOR_PROXY_MAX_CONCURRENT_REQUESTS,
            COORDINATOR_PROXY_MAX_LOOKUPS_PER_REQUEST,
            COORDINATOR_PROXY_QUEUE_CAPACITY,
            ENRICH_MAX_FORCE_MERGE_ATTEMPTS
        );
    }
}
