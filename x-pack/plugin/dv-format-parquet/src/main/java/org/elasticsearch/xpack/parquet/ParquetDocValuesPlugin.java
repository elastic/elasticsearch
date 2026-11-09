/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.parquet.docvalues.ParquetBackedSettings;
import org.elasticsearch.xpack.parquet.docvalues.ParquetBlobCache;
import org.elasticsearch.xpack.parquet.docvalues.ParquetEngineFactory;
import org.elasticsearch.xpack.parquet.mount.MountParquetAction;
import org.elasticsearch.xpack.parquet.mount.PreviewParquetAction;
import org.elasticsearch.xpack.parquet.mount.RestMountParquetAction;
import org.elasticsearch.xpack.parquet.mount.RestPreviewParquetAction;
import org.elasticsearch.xpack.parquet.mount.TransportMountParquetAction;
import org.elasticsearch.xpack.parquet.mount.TransportPreviewParquetAction;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Registers the Parquet-backed doc values format. The format itself is resolved by name through Lucene SPI when a segment is read
 * (see {@code META-INF/services/org.apache.lucene.codecs.DocValuesFormat}), and the codec of the stub segments the same way. The
 * plugin class selects the read-only engine for indices that are marked as Parquet-backed.
 */
public class ParquetDocValuesPlugin extends Plugin implements EnginePlugin, ActionPlugin, Closeable {

    private static final Logger logger = LogManager.getLogger(ParquetDocValuesPlugin.class);

    private final SetOnce<RepositoriesService> repositories = new SetOnce<>();
    private final SetOnce<ParquetBlobCache> cache = new SetOnce<>();

    @Override
    public Collection<?> createComponents(PluginServices services) {
        repositories.set(services.repositoriesService());
        if (ParquetBlobCache.isEnabled(services.environment().settings())) {
            if (services.nodeEnvironment().nodeDataPaths().length != 1) {
                logger.warn(
                    "[{}] is set, but the node has [{}] data paths and the cache needs exactly one: reads go straight to the repository",
                    ParquetBlobCache.CACHE_SIZE.getKey(),
                    services.nodeEnvironment().nodeDataPaths().length
                );
                return List.of();
            }
            try {
                ParquetBlobCache blobCache = new ParquetBlobCache(
                    services.nodeEnvironment(),
                    services.environment().settings(),
                    services.threadPool(),
                    // No metrics: BlobCacheMetrics registers fixed metric names, and the searchable snapshots plugin of the default
                    // distribution registers the same ones, so a second registration stops the node with "already registered".
                    new BlobCacheMetrics(MeterRegistry.NOOP, services.threadPool())
                );
                cache.set(blobCache);
                return List.of(blobCache);
            } catch (RuntimeException e) {
                // a cache is an optimization, so a node that cannot create it (a full disk, for example) must still start
                logger.warn(
                    "cannot create the Parquet cache of [{}]: reads go straight to the repository",
                    ParquetBlobCache.CACHE_SIZE.get(services.environment().settings()),
                    e
                );
            }
        }
        return List.of();
    }

    @Override
    public void close() {
        ParquetBlobCache blobCache = cache.get();
        if (blobCache != null) {
            blobCache.close();
        }
    }

    @Override
    public Collection<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(PreviewParquetAction.INSTANCE, TransportPreviewParquetAction.class),
            new ActionHandler(MountParquetAction.INSTANCE, TransportMountParquetAction.class)
        );
    }

    @Override
    public Collection<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return List.of(new RestPreviewParquetAction(), new RestMountParquetAction());
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>(ParquetBackedSettings.all());
        settings.addAll(ParquetBlobCache.settings());
        return settings;
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (ParquetBackedSettings.isParquetBacked(indexSettings.getSettings())) {
            return Optional.of(new ParquetEngineFactory(repositories::get, cache::get));
        }
        return Optional.empty();
    }
}
