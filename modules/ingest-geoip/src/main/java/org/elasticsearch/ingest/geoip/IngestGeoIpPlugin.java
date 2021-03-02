/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.PersistentTaskPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.index.mapper.MapperService.SINGLE_MAPPING_NAME;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.DATABASES_INDEX;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.GEOIP_DOWNLOADER;
import static org.elasticsearch.ingest.geoip.GeoIpDownloader.GEOIP_V2_FEATURE_FLAG_ENABLED;

public class IngestGeoIpPlugin extends Plugin implements IngestPlugin, SystemIndexPlugin, Closeable, PersistentTaskPlugin {
    public static final Setting<Long> CACHE_SIZE =
        Setting.longSetting("ingest.geoip.cache_size", 1000, 0, Setting.Property.NodeScope);

    static String[] DEFAULT_DATABASE_FILENAMES = new String[]{"GeoLite2-ASN.mmdb", "GeoLite2-City.mmdb", "GeoLite2-Country.mmdb"};

    private final SetOnce<LocalDatabases> localDatabases = new SetOnce<>();

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>(Arrays.asList(CACHE_SIZE,
            GeoIpDownloader.ENDPOINT_SETTING,
            GeoIpDownloader.POLL_INTERVAL_SETTING));
        if (GEOIP_V2_FEATURE_FLAG_ENABLED) {
            settings.add(GeoIpDownloaderTaskExecutor.ENABLED_SETTING);
        }
        return settings;
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        final long cacheSize = CACHE_SIZE.get(parameters.env.settings());
        final GeoIpCache cache = new GeoIpCache(cacheSize);
        LocalDatabases localDatabases = new LocalDatabases(parameters.env, cache);
        this.localDatabases.set(localDatabases);
        return Collections.singletonMap(GeoIpProcessor.TYPE, new GeoIpProcessor.Factory(localDatabases));
    }

    @Override
    public Collection<Object> createComponents(Client client,
                                               ClusterService clusterService,
                                               ThreadPool threadPool,
                                               ResourceWatcherService resourceWatcherService,
                                               ScriptService scriptService,
                                               NamedXContentRegistry xContentRegistry,
                                               Environment environment,
                                               NodeEnvironment nodeEnvironment,
                                               NamedWriteableRegistry namedWriteableRegistry,
                                               IndexNameExpressionResolver indexNameExpressionResolver,
                                               Supplier<RepositoriesService> repositoriesServiceSupplier) {
        try {
            localDatabases.get().initialize(resourceWatcherService);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
                }
        return List.of();
    }

    @Override
    public void close() throws IOException {
        localDatabases.get().close();
    }

    @Override
    public List<PersistentTasksExecutor<?>> getPersistentTasksExecutor(ClusterService clusterService, ThreadPool threadPool,
                                                                       Client client, SettingsModule settingsModule,
                                                                       IndexNameExpressionResolver expressionResolver) {
        if (GEOIP_V2_FEATURE_FLAG_ENABLED) {
            Settings settings = settingsModule.getSettings();
            return List.of(new GeoIpDownloaderTaskExecutor(client, new HttpClient(), clusterService, threadPool, settings));
        } else {
            return List.of();
        }
    }

    @Override
    public List<NamedXContentRegistry.Entry> getNamedXContent() {
        return List.of(new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(GEOIP_DOWNLOADER),
                GeoIpTaskParams::fromXContent),
            new NamedXContentRegistry.Entry(PersistentTaskState.class, new ParseField(GEOIP_DOWNLOADER), GeoIpTaskState::fromXContent));
    }

    @Override
    public List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return List.of(new NamedWriteableRegistry.Entry(PersistentTaskState.class, GEOIP_DOWNLOADER, GeoIpTaskState::new),
            new NamedWriteableRegistry.Entry(PersistentTaskParams.class, GEOIP_DOWNLOADER, GeoIpTaskParams::new));
    }

    @Override
    public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
        SystemIndexDescriptor geoipDatabasesIndex = SystemIndexDescriptor.builder()
            .setIndexPattern(DATABASES_INDEX)
            .setDescription("GeoIP databases")
            .setMappings(mappings())
            .setSettings(Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_AUTO_EXPAND_REPLICAS, "0-1")
                .build())
            .setOrigin("geoip")
            .setVersionMetaKey("version")
            .setPrimaryIndex(DATABASES_INDEX)
            .build();
        return Collections.singleton(geoipDatabasesIndex);
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
            return jsonBuilder()
                .startObject()
                .startObject(SINGLE_MAPPING_NAME)
                .startObject("_meta")
                .field("version", Version.CURRENT)
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
