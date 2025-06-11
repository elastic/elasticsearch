/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.NamedDiff;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Strings;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.ingest.EnterpriseGeoIpTask.EnterpriseGeoIpTaskParams;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.geoip.direct.DatabaseConfiguration;
import org.elasticsearch.ingest.geoip.direct.DeleteDatabaseConfigurationAction;
import org.elasticsearch.ingest.geoip.direct.GetDatabaseConfigurationAction;
import org.elasticsearch.ingest.geoip.direct.PutDatabaseConfigurationAction;
import org.elasticsearch.ingest.geoip.direct.RestDeleteDatabaseConfigurationAction;
import org.elasticsearch.ingest.geoip.direct.RestGetDatabaseConfigurationAction;
import org.elasticsearch.ingest.geoip.direct.RestPutDatabaseConfigurationAction;
import org.elasticsearch.ingest.geoip.direct.TransportDeleteDatabaseConfigurationAction;
import org.elasticsearch.ingest.geoip.direct.TransportGetDatabaseConfigurationAction;
import org.elasticsearch.ingest.geoip.direct.TransportPutDatabaseConfigurationAction;
import org.elasticsearch.ingest.geoip.stats.GeoIpDownloaderStats;
import org.elasticsearch.ingest.geoip.stats.GeoIpStatsAction;
import org.elasticsearch.ingest.geoip.stats.GeoIpStatsTransportAction;
import org.elasticsearch.ingest.geoip.stats.RestGeoIpStatsAction;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static java.util.Map.entry;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.ingest.EnterpriseGeoIpTask.ENTERPRISE_GEOIP_DOWNLOADER;
import static org.elasticsearch.ingest.IngestService.INGEST_ORIGIN;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.DATABASES_INDEX;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.DATABASES_INDEX_PATTERN;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.GEOIP_DOWNLOADER;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class IngestGeoIpPlugin extends Plugin
    implements
        IngestPlugin,
        SystemIndexPlugin,
        Closeable,
        PersistentTaskPlugin,
        ActionPlugin,
        ReloadablePlugin {
    private static final Setting<Long> CACHE_SIZE_COUNT = Setting.longSetting(
        "ingest.geoip.cache_size",
        1000,
        0,
        Setting.Property.NodeScope
    );
    private static final Setting<ByteSizeValue> CACHE_SIZE_BYTES = Setting.byteSizeSetting(
        "ingest.geoip.cache_memory_size",
        ByteSizeValue.MINUS_ONE,
        Setting.Property.NodeScope
    );
    private static final int GEOIP_INDEX_MAPPINGS_VERSION = 1;
    /**
     * No longer used for determining the age of mappings, but system index descriptor
     * code requires <em>something</em> be set. We use a value that can be parsed by
     * old nodes in mixed-version clusters, just in case any old code exists that
     * tries to parse <code>version</code> from index metadata, and that will indicate
     * to these old nodes that the mappings are newer than they are.
     */
    private static final String LEGACY_VERSION_FIELD_VALUE = "8.12.0";

    private final SetOnce<IngestService> ingestService = new SetOnce<>();
    private final SetOnce<DatabaseNodeService> databaseRegistry = new SetOnce<>();
    private GeoIpDownloaderTaskExecutor geoIpDownloaderTaskExecutor;
    private EnterpriseGeoIpDownloaderTaskExecutor enterpriseGeoIpDownloaderTaskExecutor;

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            CACHE_SIZE_COUNT,
            CACHE_SIZE_BYTES,
            GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING,
            GeoIpDownloaderTaskExecutor.ENABLED_SETTING,
            GeoIpDownloader.ENDPOINT_SETTING,
            GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING,
            EnterpriseGeoIpDownloaderTaskExecutor.MAXMIND_LICENSE_KEY_SETTING,
            EnterpriseGeoIpDownloaderTaskExecutor.IPINFO_TOKEN_SETTING
        );
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        ingestService.set(parameters.ingestService);

        GeoIpCache geoIpCache = createGeoIpCache(parameters.env.settings());
        DatabaseNodeService registry = new DatabaseNodeService(
            parameters.env,
            parameters.client,
            geoIpCache,
            parameters.genericExecutor,
            parameters.ingestService.getClusterService()
        );
        databaseRegistry.set(registry);
        return Map.ofEntries(
            entry(GeoIpProcessor.GEOIP_TYPE, new GeoIpProcessor.Factory(GeoIpProcessor.GEOIP_TYPE, registry)),
            entry(GeoIpProcessor.IP_LOCATION_TYPE, new GeoIpProcessor.Factory(GeoIpProcessor.IP_LOCATION_TYPE, registry))
        );
    }

    private static GeoIpCache createGeoIpCache(Settings settings) {
        if (settings.hasValue(CACHE_SIZE_BYTES.getKey())) {
            if (settings.hasValue(CACHE_SIZE_COUNT.getKey())) {
                // Both CACHE_SIZE_COUNT and CACHE_SIZE_BYTES are set, which is an error:
                throw new IllegalArgumentException(
                    Strings.format(
                        "Both %s and %s are set: please use either %s to set a size based on count or %s to set a size based on bytes of memory",
                        CACHE_SIZE_COUNT.getKey(),
                        CACHE_SIZE_BYTES.getKey(),
                        CACHE_SIZE_COUNT.getKey(),
                        CACHE_SIZE_BYTES.getKey()
                    )
                );
            } else {
                // Only CACHE_SIZE_BYTES is set, so use that:
                return GeoIpCache.createGeoIpCacheWithMaxBytes(CACHE_SIZE_BYTES.get(settings));
            }
        } else {
            // CACHE_SIZE_BYTES is not set, so use either the explicit or default value of CACHE_SIZE_COUNT:
            return GeoIpCache.createGeoIpCacheWithMaxCount(CACHE_SIZE_COUNT.get(settings));
        }
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        try {
            String nodeId = services.nodeEnvironment().nodeId();
            databaseRegistry.get().initialize(nodeId, services.resourceWatcherService(), ingestService.get());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        geoIpDownloaderTaskExecutor = new GeoIpDownloaderTaskExecutor(
            services.client(),
            new HttpClient(),
            services.clusterService(),
            services.threadPool()
        );
        geoIpDownloaderTaskExecutor.init();

        enterpriseGeoIpDownloaderTaskExecutor = new EnterpriseGeoIpDownloaderTaskExecutor(
            services.client(),
            new HttpClient(),
            services.clusterService(),
            services.threadPool()
        );
        enterpriseGeoIpDownloaderTaskExecutor.init();

        return List.of(databaseRegistry.get(), geoIpDownloaderTaskExecutor, enterpriseGeoIpDownloaderTaskExecutor);
    }

    @Override
    public void close() throws IOException {
        databaseRegistry.get().shutdown();
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        return List.of(geoIpDownloaderTaskExecutor, enterpriseGeoIpDownloaderTaskExecutor);
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(GeoIpStatsAction.INSTANCE, GeoIpStatsTransportAction.class),
            new ActionHandler(GetDatabaseConfigurationAction.INSTANCE, TransportGetDatabaseConfigurationAction.class),
            new ActionHandler(DeleteDatabaseConfigurationAction.INSTANCE, TransportDeleteDatabaseConfigurationAction.class),
            new ActionHandler(PutDatabaseConfigurationAction.INSTANCE, TransportPutDatabaseConfigurationAction.class)
        );
    }

    @Override
    public List<RestHandler> getRestHandlers(
        Settings settings,
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
            new RestGeoIpStatsAction(),
            new RestGetDatabaseConfigurationAction(),
            new RestDeleteDatabaseConfigurationAction(),
            new RestPutDatabaseConfigurationAction()
        );
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(
                Metadata.ProjectCustom.class,
                new ParseField(IngestGeoIpMetadata.TYPE),
                IngestGeoIpMetadata::fromXContent
            ),
            new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(GEOIP_DOWNLOADER), GeoIpTaskParams::fromXContent),
            new NamedXContentRegistry.Entry(PersistentTaskState.class, new ParseField(GEOIP_DOWNLOADER), GeoIpTaskState::fromXContent),
            new NamedXContentRegistry.Entry(
                PersistentTaskParams.class,
                new ParseField(ENTERPRISE_GEOIP_DOWNLOADER),
                EnterpriseGeoIpTaskParams::fromXContent
            ),
            new NamedXContentRegistry.Entry(
                PersistentTaskState.class,
                new ParseField(ENTERPRISE_GEOIP_DOWNLOADER),
                EnterpriseGeoIpTaskState::fromXContent
            )
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(Metadata.ProjectCustom.class, IngestGeoIpMetadata.TYPE, IngestGeoIpMetadata::new),
            new NamedWriteableRegistry.Entry(NamedDiff.class, IngestGeoIpMetadata.TYPE, IngestGeoIpMetadata.GeoIpMetadataDiff::new),
            new NamedWriteableRegistry.Entry(PersistentTaskState.class, GEOIP_DOWNLOADER, GeoIpTaskState::new),
            new NamedWriteableRegistry.Entry(PersistentTaskParams.class, GEOIP_DOWNLOADER, GeoIpTaskParams::new),
            new NamedWriteableRegistry.Entry(PersistentTaskState.class, ENTERPRISE_GEOIP_DOWNLOADER, EnterpriseGeoIpTaskState::new),
            new NamedWriteableRegistry.Entry(PersistentTaskParams.class, ENTERPRISE_GEOIP_DOWNLOADER, EnterpriseGeoIpTaskParams::new),
            new NamedWriteableRegistry.Entry(Task.Status.class, GEOIP_DOWNLOADER, GeoIpDownloaderStats::new),
            new NamedWriteableRegistry.Entry(
                DatabaseConfiguration.Provider.class,
                DatabaseConfiguration.Maxmind.NAME,
                DatabaseConfiguration.Maxmind::new
            ),
            new NamedWriteableRegistry.Entry(
                DatabaseConfiguration.Provider.class,
                DatabaseConfiguration.Ipinfo.NAME,
                DatabaseConfiguration.Ipinfo::new
            ),
            new NamedWriteableRegistry.Entry(
                DatabaseConfiguration.Provider.class,
                DatabaseConfiguration.Local.NAME,
                DatabaseConfiguration.Local::new
            ),
            new NamedWriteableRegistry.Entry(
                DatabaseConfiguration.Provider.class,
                DatabaseConfiguration.Web.NAME,
                DatabaseConfiguration.Web::new
            )
        );
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        SystemIndexDescriptor geoipDatabasesIndex = SystemIndexDescriptor.builder()
            .setIndexPattern(DATABASES_INDEX_PATTERN)
            .setDescription("GeoIP databases")
            .setMappings(mappings())
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                    .build()
            )
            .setOrigin(INGEST_ORIGIN)
            .setPrimaryIndex(DATABASES_INDEX)
            .setNetNew()
            .build();
        return List.of(geoipDatabasesIndex);
    }

    @Override
    public String getFeatureName() {
        return "geoip";
    }

    @Override
    public String getFeatureDescription() {
        return "Manages data related to GeoIP database downloader";
    }

    private static XContentBuilder mappings() {
        try {
            return jsonBuilder().startObject()
                .startObject(SINGLE_MAPPING_NAME)
                .startObject("_meta")
                .field("version", LEGACY_VERSION_FIELD_VALUE)
                .field(SystemIndexDescriptor.VERSION_META_KEY, GEOIP_INDEX_MAPPINGS_VERSION)
                .endObject()
                .field("dynamic", "strict")
                .startObject("properties")
                .startObject("name")
                .field("type", "keyword")
                .endObject()
                .startObject("chunk")
                .field("type", "integer")
                .endObject()
                .startObject("data")
                .field("type", "binary")
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to build mappings for " + DATABASES_INDEX, e);
        }
    }

    @Override
    public void reload(Settings settings) {
        enterpriseGeoIpDownloaderTaskExecutor.reload(settings);
    }
}
