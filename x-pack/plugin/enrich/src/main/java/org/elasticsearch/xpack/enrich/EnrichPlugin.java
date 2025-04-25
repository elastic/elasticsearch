/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.MemorySizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.enrich.EnrichMetadata;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.EnrichStatsAction;
import org.elasticsearch.xpack.core.enrich.action.ExecuteEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;
import org.elasticsearch.xpack.core.enrich.action.PutEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.action.EnrichCoordinatorProxyAction;
import org.elasticsearch.xpack.enrich.action.EnrichCoordinatorStatsAction;
import org.elasticsearch.xpack.enrich.action.EnrichInfoTransportAction;
import org.elasticsearch.xpack.enrich.action.EnrichReindexAction;
import org.elasticsearch.xpack.enrich.action.EnrichShardMultiSearchAction;
import org.elasticsearch.xpack.enrich.action.EnrichUsageTransportAction;
import org.elasticsearch.xpack.enrich.action.InternalExecutePolicyAction;
import org.elasticsearch.xpack.enrich.action.TransportDeleteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.action.TransportEnrichReindexAction;
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
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.core.enrich.EnrichPolicy.ENRICH_INDEX_PATTERN;

public class EnrichPlugin extends Plugin implements SystemIndexPlugin, IngestPlugin {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(EnrichPlugin.class);

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

    public static final String CACHE_SIZE_SETTING_NAME = "enrich.cache_size";
    public static final Setting<FlatNumberOrByteSizeValue> CACHE_SIZE = new Setting<>(
        CACHE_SIZE_SETTING_NAME,
        (String) null,
        (String s) -> FlatNumberOrByteSizeValue.parse(
            s,
            CACHE_SIZE_SETTING_NAME,
            new FlatNumberOrByteSizeValue(ByteSizeValue.ofBytes((long) (0.01 * JvmInfo.jvmInfo().getConfiguredMaxHeapSize())))
        ),
        Setting.Property.NodeScope
    );

    /**
     * This setting solely exists because the original setting was accidentally renamed in
     * https://github.com/elastic/elasticsearch/pull/111412.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DATA_MANAGEMENT)
    public static final String CACHE_SIZE_SETTING_BWC_NAME = "enrich.cache.size";
    public static final Setting<FlatNumberOrByteSizeValue> CACHE_SIZE_BWC = new Setting<>(
        CACHE_SIZE_SETTING_BWC_NAME,
        (String) null,
        (String s) -> FlatNumberOrByteSizeValue.parse(
            s,
            CACHE_SIZE_SETTING_BWC_NAME,
            new FlatNumberOrByteSizeValue(ByteSizeValue.ofBytes((long) (0.01 * JvmInfo.jvmInfo().getConfiguredMaxHeapSize())))
        ),
        Setting.Property.NodeScope,
        Setting.Property.Deprecated
    );

    private final Settings settings;
    private final EnrichCache enrichCache;
    private final long maxCacheSize;

    public EnrichPlugin(final Settings settings) {
        this.settings = settings;
        FlatNumberOrByteSizeValue maxSize;
        if (settings.hasValue(CACHE_SIZE_SETTING_BWC_NAME)) {
            if (settings.hasValue(CACHE_SIZE_SETTING_NAME)) {
                throw new IllegalArgumentException(
                    Strings.format(
                        "Both [%s] and [%s] are set, please use [%s]",
                        CACHE_SIZE_SETTING_NAME,
                        CACHE_SIZE_SETTING_BWC_NAME,
                        CACHE_SIZE_SETTING_NAME
                    )
                );
            }
            deprecationLogger.warn(
                DeprecationCategory.SETTINGS,
                "enrich_cache_size_name",
                "The [{}] setting is deprecated and will be removed in a future version. Please use [{}] instead.",
                CACHE_SIZE_SETTING_BWC_NAME,
                CACHE_SIZE_SETTING_NAME
            );
            maxSize = CACHE_SIZE_BWC.get(settings);
        } else {
            maxSize = CACHE_SIZE.get(settings);
        }
        if (maxSize.byteSizeValue() != null) {
            this.enrichCache = new EnrichCache(maxSize.byteSizeValue());
            this.maxCacheSize = maxSize.byteSizeValue().getBytes();
        } else {
            this.enrichCache = new EnrichCache(maxSize.flatNumber());
            this.maxCacheSize = maxSize.flatNumber();
        }
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        EnrichProcessorFactory factory = new EnrichProcessorFactory(parameters.client, parameters.scriptService, enrichCache);
        parameters.ingestService.addIngestClusterStateListener(factory);
        return Map.of(EnrichProcessorFactory.TYPE, factory);
    }

    protected XPackLicenseState getLicenseState() {
        return XPackPlugin.getSharedLicenseState();
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(XPackInfoFeatureAction.ENRICH, EnrichInfoTransportAction.class),
            new ActionHandler(XPackUsageFeatureAction.ENRICH, EnrichUsageTransportAction.class),
            new ActionHandler(GetEnrichPolicyAction.INSTANCE, TransportGetEnrichPolicyAction.class),
            new ActionHandler(DeleteEnrichPolicyAction.INSTANCE, TransportDeleteEnrichPolicyAction.class),
            new ActionHandler(PutEnrichPolicyAction.INSTANCE, TransportPutEnrichPolicyAction.class),
            new ActionHandler(ExecuteEnrichPolicyAction.INSTANCE, TransportExecuteEnrichPolicyAction.class),
            new ActionHandler(EnrichStatsAction.INSTANCE, TransportEnrichStatsAction.class),
            new ActionHandler(EnrichCoordinatorProxyAction.INSTANCE, EnrichCoordinatorProxyAction.TransportAction.class),
            new ActionHandler(EnrichShardMultiSearchAction.INSTANCE, EnrichShardMultiSearchAction.TransportAction.class),
            new ActionHandler(EnrichCoordinatorStatsAction.INSTANCE, EnrichCoordinatorStatsAction.TransportAction.class),
            new ActionHandler(EnrichReindexAction.INSTANCE, TransportEnrichReindexAction.class),
            new ActionHandler(InternalExecutePolicyAction.INSTANCE, InternalExecutePolicyAction.Transport.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings unused,
        NamedWriteableRegistry namedWriteableRegistry,
        RestController restController,
        ClusterSettings clusterSettings,
        IndexScopedSettings indexScopedSettings,
        SettingsFilter settingsFilter,
        IndexNameExpressionResolver indexNameExpressionResolver,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(
            new RestGetEnrichPolicyAction(),
            new RestDeleteEnrichPolicyAction(),
            new RestPutEnrichPolicyAction(),
            new RestExecuteEnrichPolicyAction(),
            new RestEnrichStatsAction()
        );
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        EnrichPolicyLocks enrichPolicyLocks = new EnrichPolicyLocks();
        EnrichPolicyExecutor enrichPolicyExecutor = new EnrichPolicyExecutor(
            settings,
            services.clusterService(),
            services.indicesService(),
            services.client(),
            services.threadPool(),
            services.indexNameExpressionResolver(),
            enrichPolicyLocks,
            System::currentTimeMillis
        );
        EnrichPolicyMaintenanceService enrichPolicyMaintenanceService = new EnrichPolicyMaintenanceService(
            settings,
            services.client(),
            services.clusterService(),
            services.threadPool(),
            enrichPolicyLocks
        );
        enrichPolicyMaintenanceService.initialize();
        return List.of(
            enrichPolicyLocks,
            new EnrichCoordinatorProxyAction.Coordinator(services.client(), settings),
            enrichPolicyMaintenanceService,
            enrichPolicyExecutor,
            enrichCache
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, EnrichMetadata.TYPE, EnrichMetadata::new),
            new NamedWriteableRegistry.Entry(
                NamedDiff.class,
                EnrichMetadata.TYPE,
                in -> EnrichMetadata.readDiffFrom(Metadata.ProjectCustom.class, EnrichMetadata.TYPE, in)
            )
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(Metadata.ProjectCustom.class, new ParseField(EnrichMetadata.TYPE), EnrichMetadata::fromXContent)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            ENRICH_FETCH_SIZE_SETTING,
            ENRICH_MAX_CONCURRENT_POLICY_EXECUTIONS,
            ENRICH_CLEANUP_PERIOD,
            COORDINATOR_PROXY_MAX_CONCURRENT_REQUESTS,
            COORDINATOR_PROXY_MAX_LOOKUPS_PER_REQUEST,
            COORDINATOR_PROXY_QUEUE_CAPACITY,
            ENRICH_MAX_FORCE_MERGE_ATTEMPTS,
            CACHE_SIZE,
            CACHE_SIZE_BWC
        );
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings unused) {
        return List.of(
            SystemIndexDescriptor.builder()
                .setIndexPattern(ENRICH_INDEX_PATTERN)
                .setDescription("Contains data to support enrich ingest processors.")
                .setType(SystemIndexDescriptor.Type.INTERNAL_UNMANAGED)
                .setAllowedElasticProductOrigins(List.of())
                .build()
        );
    }

    @Override
    public String getFeatureName() {
        return "enrich";
    }

    @Override
    public String getFeatureDescription() {
        return "Manages data related to Enrich policies";
    }

    // Visible for testing
    long getMaxCacheSize() {
        return maxCacheSize;
    }

    /**
     * A class that specifies either a flat (unit-less) number or a byte size value.
     */
    public static class FlatNumberOrByteSizeValue {

        @Nullable
        private final Long flatNumber;
        @Nullable
        private final ByteSizeValue byteSizeValue;

        public FlatNumberOrByteSizeValue(ByteSizeValue byteSizeValue) {
            this.byteSizeValue = byteSizeValue;
            this.flatNumber = null;
        }

        public FlatNumberOrByteSizeValue(Long flatNumber) {
            this.flatNumber = flatNumber;
            this.byteSizeValue = null;
        }

        public static FlatNumberOrByteSizeValue parse(String value, String settingName, FlatNumberOrByteSizeValue defaultValue) {
            if (Strings.hasText(value) == false) {
                return defaultValue;
            }
            if (Character.isDigit(value.charAt(value.length() - 1)) == false) {
                return new FlatNumberOrByteSizeValue(MemorySizeValue.parseBytesSizeValueOrHeapRatio(value, settingName));
            }
            return new FlatNumberOrByteSizeValue(Long.parseLong(value));
        }

        @Nullable
        public ByteSizeValue byteSizeValue() {
            return byteSizeValue;
        }

        @Nullable
        public Long flatNumber() {
            return flatNumber;
        }
    }
}
