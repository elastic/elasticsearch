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
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.parquet.docvalues.ParquetBackedSettings;
import org.elasticsearch.xpack.parquet.docvalues.ParquetEngineFactory;
import org.elasticsearch.xpack.parquet.mount.MountParquetAction;
import org.elasticsearch.xpack.parquet.mount.PreviewParquetAction;
import org.elasticsearch.xpack.parquet.mount.RestMountParquetAction;
import org.elasticsearch.xpack.parquet.mount.RestPreviewParquetAction;
import org.elasticsearch.xpack.parquet.mount.TransportMountParquetAction;
import org.elasticsearch.xpack.parquet.mount.TransportPreviewParquetAction;

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
public class ParquetDocValuesPlugin extends Plugin implements EnginePlugin, ActionPlugin {

    private final SetOnce<RepositoriesService> repositories = new SetOnce<>();

    @Override
    public Collection<?> createComponents(PluginServices services) {
        repositories.set(services.repositoriesService());
        return List.of();
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
