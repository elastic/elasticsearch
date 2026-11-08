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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.xpack.parquet.dv.ScanRegistry;
import org.elasticsearch.xpack.parquet.engine.ParquetEngineFactory;
import org.elasticsearch.xpack.parquet.mount.MountParquetAction;
import org.elasticsearch.xpack.parquet.mount.PreviewParquetAction;
import org.elasticsearch.xpack.parquet.mount.RestMountParquetAction;
import org.elasticsearch.xpack.parquet.mount.RestPreviewParquetAction;
import org.elasticsearch.xpack.parquet.mount.TransportMountParquetAction;
import org.elasticsearch.xpack.parquet.mount.TransportPreviewParquetAction;
import org.elasticsearch.xpack.parquet.repository.ParquetBackedSettings;
import org.elasticsearch.xpack.parquet.repository.ParquetBlobCache;
import org.elasticsearch.xpack.parquet.repository.ParquetFiles;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * Serves indices that are backed by Parquet files. Lucene resolves the doc values format by name through SPI when it opens a segment
 * (see {@code META-INF/services/org.apache.lucene.codecs.DocValuesFormat}), so the format needs no registration here. The plugin
 * selects a read-only engine for the indices that name a repository, and the engine attaches the rows of their files as stub segments.
 * The mount API creates such indices. Decoding is not done in this plugin: the format reads through the ES|QL Parquet reader of the
 * {@code esql-datasource-parquet} plugin, which this plugin extends.
 */
public class ParquetDocValuesPlugin extends Plugin implements EnginePlugin, ActionPlugin, Closeable {

    /** How often the idle scans are closed, see {@link ScanRegistry#sweep()}. */
    private static final TimeValue SWEEP_INTERVAL = TimeValue.timeValueSeconds(5);

    private static final Logger logger = LogManager.getLogger(ParquetDocValuesPlugin.class);

    private final SetOnce<ParquetFiles> files = new SetOnce<>();
    private final SetOnce<Scheduler.Cancellable> sweeper = new SetOnce<>();
    private final SetOnce<ParquetBlobCache> cache = new SetOnce<>();

    /**
     * Keeps the repositories service, through which the engine reads the files, starts the sweep that closes the scans that no
     * iterator uses any more, and creates the node-local disk cache of file ranges when {@code xpack.parquet.cache.size} asks for one.
     * A cache is an optimization, so a node that cannot create it (the data path is full, or there are several data paths) still starts
     * and reads the repository directly.
     */
    @Override
    public Collection<?> createComponents(PluginServices services) {
        ParquetFiles parquetFiles = new ParquetFiles(services::repositoriesService, services.clusterService());
        files.set(parquetFiles);
        // the mount actions get it by injection, so it is a component
        List<Object> components = new ArrayList<>(List.of(parquetFiles));
        sweeper.set(
            services.threadPool().scheduleWithFixedDelay(ScanRegistry.INSTANCE::sweep, SWEEP_INTERVAL, services.threadPool().generic())
        );
        Settings settings = services.environment().settings();
        if (ParquetBlobCache.isEnabled(settings)) {
            if (services.nodeEnvironment().nodeDataPaths().length != 1) {
                logger.warn(
                    "[{}] is set, but the node has [{}] data paths and the cache needs exactly one: reads go straight to the repository",
                    ParquetBlobCache.CACHE_SIZE.getKey(),
                    services.nodeEnvironment().nodeDataPaths().length
                );
                return components;
            }
            try {
                ParquetBlobCache blobCache = new ParquetBlobCache(
                    services.nodeEnvironment(),
                    settings,
                    services.threadPool(),
                    // No metrics: BlobCacheMetrics registers fixed metric names, and the searchable snapshots plugin of the default
                    // distribution registers the same ones, so a second registration stops the node with "already registered".
                    new BlobCacheMetrics(MeterRegistry.NOOP, services.threadPool())
                );
                cache.set(blobCache);
                components.add(blobCache);
                return components;
            } catch (RuntimeException e) {
                logger.warn(
                    "cannot create the Parquet cache of [{}]: reads go straight to the repository",
                    ParquetBlobCache.CACHE_SIZE.get(settings),
                    e
                );
            }
        }
        return components;
    }

    @Override
    public void close() {
        Scheduler.Cancellable cancellable = sweeper.get();
        if (cancellable != null) {
            cancellable.cancel();
        }
        ParquetBlobCache blobCache = cache.get();
        if (blobCache != null) {
            blobCache.close();
        }
    }

    @Override
    public List<ActionHandler> getActions() {
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
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return List.of(ParquetBlobCache.fetchExecutorBuilder(settings));
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
            return Optional.of(new ParquetEngineFactory(files::get, cache::get));
        }
        return Optional.empty();
    }
}
