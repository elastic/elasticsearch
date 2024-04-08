/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.IndexScopedSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.geoip.stats.GeoIpDownloaderStats;
import org.elasticsearch.ingest.geoip.stats.GeoIpDownloaderStatsAction;
import org.elasticsearch.ingest.geoip.stats.GeoIpDownloaderStatsTransportAction;
import org.elasticsearch.ingest.geoip.stats.RestGeoIpDownloaderStatsAction;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
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

import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.ingest.IngestService.INGEST_ORIGIN;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.DATABASES_INDEX;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.DATABASES_INDEX_PATTERN;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.GEOIP_DOWNLOADER;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class IngestGeoIpPlugin extends Plugin implements IngestPlugin, SystemIndexPlugin, Closeable, PersistentTaskPlugin, ActionPlugin {
    public static final Setting<Long> CACHE_SIZE = Setting.longSetting("ingest.geoip.cache_size", 1000, 0, Setting.Property.NodeScope);
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

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(
            CACHE_SIZE,
            GeoIpDownloaderTaskExecutor.EAGER_DOWNLOAD_SETTING,
            GeoIpDownloaderTaskExecutor.ENABLED_SETTING,
            GeoIpDownloader.ENDPOINT_SETTING,
            GeoIpDownloaderTaskExecutor.POLL_INTERVAL_SETTING
        );
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        ingestService.set(parameters.ingestService);

        long cacheSize = CACHE_SIZE.get(parameters.env.settings());
        GeoIpCache geoIpCache = new GeoIpCache(cacheSize);
        DatabaseNodeService registry = new DatabaseNodeService(
            parameters.env,
            parameters.client,
            geoIpCache,
            parameters.genericExecutor,
            parameters.ingestService.getClusterService()
        );
        databaseRegistry.set(registry);
        return Map.of(GeoIpProcessor.TYPE, new GeoIpProcessor.Factory(registry));
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
        return List.of(databaseRegistry.get(), geoIpDownloaderTaskExecutor);
    }

    @Override
    public void close() throws IOException {
        databaseRegistry.get().close();
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(
        ClusterService clusterService,
        ThreadPool threadPool,
        Client client,
        SettingsModule settingsModule,
        IndexNameExpressionResolver expressionResolver
    ) {
        return List.of(geoIpDownloaderTaskExecutor);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return List.of(new ActionHandler<>(GeoIpDownloaderStatsAction.INSTANCE, GeoIpDownloaderStatsTransportAction.class));
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
        return List.of(new RestGeoIpDownloaderStatsAction());
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(
            new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(GEOIP_DOWNLOADER), GeoIpTaskParams::fromXContent),
            new NamedXContentRegistry.Entry(PersistentTaskState.class, new ParseField(GEOIP_DOWNLOADER), GeoIpTaskState::fromXContent)
        );
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(
            new NamedWriteableRegistry.Entry(PersistentTaskState.class, GEOIP_DOWNLOADER, GeoIpTaskState::new),
            new NamedWriteableRegistry.Entry(PersistentTaskParams.class, GEOIP_DOWNLOADER, GeoIpTaskParams::new),
            new NamedWriteableRegistry.Entry(Task.Status.class, GEOIP_DOWNLOADER, GeoIpDownloaderStats::new)
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
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                    .build()
            )
            .setOrigin(INGEST_ORIGIN)
            .setVersionMetaKey("version")
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
}
