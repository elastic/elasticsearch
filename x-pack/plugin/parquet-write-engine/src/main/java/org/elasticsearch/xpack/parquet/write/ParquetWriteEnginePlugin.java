/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.parquet.write.engine.ParquetWriteEngine;
import org.elasticsearch.xpack.parquet.write.table.CatalogRegistration;
import org.elasticsearch.xpack.parquet.write.table.RepositoryCatalog;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;
import org.elasticsearch.xpack.parquet.write.table.TableLocation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * A prototype of an index that stores its documents only as Parquet files. An index opts in with
 * {@code index.storage_format: parquet}; every other index keeps the engine that it has without this plugin.
 */
public class ParquetWriteEnginePlugin extends Plugin implements EnginePlugin {

    // set when the node creates its components, which is before any index (and so any engine) is created
    private volatile RepositoriesService repositoriesService;
    private volatile ClusterService clusterService;
    private volatile Environment environment;
    private volatile ThreadPool threadPool;

    @Override
    public Collection<?> createComponents(PluginServices services) {
        this.repositoriesService = services.repositoriesService();
        this.clusterService = services.clusterService();
        this.environment = services.environment();
        this.threadPool = services.threadPool();
        return List.of();
    }

    @Override
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        return ParquetThreadPools.builders(settings);
    }

    @Override
    public List<Setting<?>> getSettings() {
        return ParquetFormatSettings.settings();
    }

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
        return List.of(new ParquetIndexSettingProvider());
    }

    @Override
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        if (ParquetFormatSettings.INDEX_FORMAT.get(indexSettings.getSettings()) == ParquetFormatSettings.IndexFormat.PARQUET) {
            return Optional.of(config -> new ParquetWriteEngine(config, this::openTable));
        }
        return Optional.empty();
    }

    /**
     * The table of the index of the shard, in the repository that the index names. Protected so that a test can replace it: it is the
     * place where a flush first needs the libraries of the table.
     */
    protected ShardTable openTable(ShardId shardId, Settings indexSettings) throws IOException {
        final RepositoriesService repositories = repositoriesService;
        if (repositories == null) {
            throw new IOException("the node has not finished starting");
        }
        final String repositoryName = ParquetFormatSettings.REPOSITORY.get(indexSettings);
        final ProjectId projectId = clusterService.state().metadata().projectFor(shardId.getIndex()).id();
        final Repository repository = repositories.repository(projectId, repositoryName);
        if (repository instanceof BlobStoreRepository == false) {
            throw new IOException("repository [" + repositoryName + "] does not store blobs");
        }
        final BlobStoreRepository blobStoreRepository = (BlobStoreRepository) repository;
        final BlobStore blobStore = blobStoreOf(blobStoreRepository);
        final TableLocation location = TableLocation.of(
            blobStoreRepository.getMetadata(),
            blobStoreRepository.basePath(),
            tablePath(shardId, indexSettings),
            environment::resolveRepoDir
        );
        return ShardTable.open(
            blobStore,
            location,
            shardId.id(),
            catalogRegistration(blobStoreRepository, blobStore, shardId, indexSettings)
        );
    }

    /** Where the commits of the table write its entry in the catalog of the repository, or null when the index has no catalog. */
    private CatalogRegistration catalogRegistration(
        BlobStoreRepository repository,
        BlobStore blobStore,
        ShardId shardId,
        Settings indexSettings
    ) {
        if (ParquetFormatSettings.CATALOG_ENABLED.get(indexSettings) == false) {
            return null;
        }
        // the table path is empty: the location of the base path of the repository itself
        final TableLocation repositoryRoot = TableLocation.of(
            repository.getMetadata(),
            repository.basePath(),
            List.of(),
            environment::resolveRepoDir
        );
        return new CatalogRegistration(
            new RepositoryCatalog(blobStore, repositoryRoot.blobPath(), repositoryRoot.rootUri()),
            List.of(ParquetFormatSettings.CATALOG_NAMESPACE.get(indexSettings).split("\\.")),
            shardId.getIndexName(),
            shardId.getIndexName(),
            shardId.getIndex().getUUID(),
            clusterService.state().metadata().clusterUUID()
        );
    }

    /**
     * A repository gives its blob store only to threads of the snapshot pools and of {@code generic}, and a flush runs on a pool of its
     * own (see {@link ParquetThreadPools}). So the flush asks a {@code generic} thread for the blob store once, when it opens the table,
     * and keeps the store: the table and the files use it directly from then on.
     */
    private BlobStore blobStoreOf(BlobStoreRepository repository) {
        final PlainActionFuture<BlobStore> future = new PlainActionFuture<>();
        threadPool.generic().execute(ActionRunnable.supply(future, repository::blobStore));
        return future.actionGet();
    }

    /** The path of the table below the base path of the repository: {@code index.parquet.base_path}, or the name and uuid of the index. */
    static List<String> tablePath(ShardId shardId, Settings indexSettings) {
        final String custom = ParquetFormatSettings.BASE_PATH.get(indexSettings);
        if (custom.isEmpty()) {
            return List.of(shardId.getIndexName(), shardId.getIndex().getUUID());
        }
        final List<String> path = new ArrayList<>();
        for (String segment : custom.split("/")) {
            if (segment.isEmpty() == false) {
                path.add(segment);
            }
        }
        return path;
    }
}
