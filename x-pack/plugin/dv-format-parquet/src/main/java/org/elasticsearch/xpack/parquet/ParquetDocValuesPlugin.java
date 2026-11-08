/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestHandler;
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

import java.io.Closeable;
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

    private final SetOnce<RepositoriesService> repositories = new SetOnce<>();
    private final SetOnce<Scheduler.Cancellable> sweeper = new SetOnce<>();

    /**
     * Keeps the repositories service, through which the engine reads the files, and starts the sweep that closes the scans that no
     * iterator uses any more; the plugin has no components.
     */
    @Override
    public Collection<?> createComponents(PluginServices services) {
        repositories.set(services.repositoriesService());
        sweeper.set(
            services.threadPool().scheduleWithFixedDelay(ScanRegistry.INSTANCE::sweep, SWEEP_INTERVAL, services.threadPool().generic())
        );
        return List.of();
    }

    @Override
    public void close() {
        Scheduler.Cancellable cancellable = sweeper.get();
        if (cancellable != null) {
            cancellable.cancel();
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
    public List<Setting<?>> getSettings() {
        return ParquetBackedSettings.all();
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (ParquetBackedSettings.isParquetBacked(indexSettings.getSettings())) {
            return Optional.of(new ParquetEngineFactory(repositories::get));
        }
        return Optional.empty();
    }
}
