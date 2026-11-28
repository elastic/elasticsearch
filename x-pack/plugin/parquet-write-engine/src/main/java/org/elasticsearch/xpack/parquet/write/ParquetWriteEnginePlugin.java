/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.write;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.PostWriteRefresh;
import org.elasticsearch.blobcache.BlobCacheMetrics;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.Murmur3HashFunction;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.IndexSettingProvider;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.ExecutorBuilder;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.parquet.read.dv.ParquetReadContext;
import org.elasticsearch.xpack.parquet.read.dv.ScanRegistry;
import org.elasticsearch.xpack.parquet.read.engine.ParquetEngineFactory;
import org.elasticsearch.xpack.parquet.read.engine.ParquetTableFollowEngine;
import org.elasticsearch.xpack.parquet.read.mount.MountParquetAction;
import org.elasticsearch.xpack.parquet.read.mount.PreviewParquetAction;
import org.elasticsearch.xpack.parquet.read.mount.RestMountParquetAction;
import org.elasticsearch.xpack.parquet.read.mount.RestPreviewParquetAction;
import org.elasticsearch.xpack.parquet.read.mount.TransportMountParquetAction;
import org.elasticsearch.xpack.parquet.read.mount.TransportPreviewParquetAction;
import org.elasticsearch.xpack.parquet.read.repository.FileLocation;
import org.elasticsearch.xpack.parquet.read.repository.ParquetBackedSettings;
import org.elasticsearch.xpack.parquet.read.repository.ParquetBlobCache;
import org.elasticsearch.xpack.parquet.read.repository.ParquetFiles;
import org.elasticsearch.xpack.parquet.read.repository.RepositoryLocation;
import org.elasticsearch.xpack.parquet.write.engine.ParquetWriteEngine;
import org.elasticsearch.xpack.parquet.write.table.CatalogRegistration;
import org.elasticsearch.xpack.parquet.write.table.RepositoryCatalog;
import org.elasticsearch.xpack.parquet.write.table.ShardTable;
import org.elasticsearch.xpack.parquet.write.table.TableLocation;
import org.elasticsearch.xpack.parquet.write.table.TableReader;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.function.Supplier;

/**
 * A prototype of indices whose documents live only in Parquet files.
 * <ul>
 *   <li>An index opts in to the write engine with {@code index.storage_format: parquet}: it stores its documents as Parquet files in an
 *       Iceberg table.</li>
 *   <li>The mount API ({@code _parquet/_preview}, {@code _parquet/_mount}) creates read-only indices over Parquet files that already
 *       exist. Their segments are stubs that reference the files, and their doc values are read from the files by the ES|QL Parquet
 *       reader of the {@code esql-datasource-parquet} plugin, which this plugin extends. Lucene resolves the doc values format by name
 *       through SPI when it opens a segment (see {@code META-INF/services/org.apache.lucene.codecs.DocValuesFormat}).</li>
 * </ul>
 * Every other index keeps the engine that it has without this plugin.
 */
public class ParquetWriteEnginePlugin extends Plugin implements EnginePlugin, ClusterPlugin, ActionPlugin, Closeable {

    /** How often the idle scans are closed, see {@link ScanRegistry#sweep()}. */
    private static final TimeValue SWEEP_INTERVAL = TimeValue.timeValueSeconds(5);

    private static final Logger logger = LogManager.getLogger(ParquetWriteEnginePlugin.class);

    // set when the node creates its components, which is before any index (and so any engine) is created
    private volatile RepositoriesService repositoriesService;
    private volatile ClusterService clusterService;
    private volatile Environment environment;
    private volatile ThreadPool threadPool;
    private volatile Client client;

    private final SetOnce<ParquetFiles> files = new SetOnce<>();
    private final SetOnce<TableReaders> tableReaders = new SetOnce<>();
    private final SetOnce<Scheduler.Cancellable> sweeper = new SetOnce<>();
    private final SetOnce<ParquetBlobCache> cache = new SetOnce<>();

    /**
     * Keeps the services that the engines need, starts the sweep that closes the scans that no iterator uses any more, and creates the
     * node-local disk cache of file ranges when {@code xpack.parquet.cache.size} asks for one. A cache is an optimization, so a node that
     * cannot create it (the data path is full, or there are several data paths) still starts and reads the repository directly.
     */
    @Override
    public Collection<?> createComponents(PluginServices services) {
        this.repositoriesService = services.repositoriesService();
        this.clusterService = services.clusterService();
        this.environment = services.environment();
        this.threadPool = services.threadPool();
        this.client = services.client();

        ParquetFiles parquetFiles = new ParquetFiles(services::repositoriesService, services.clusterService());
        files.set(parquetFiles);
        tableReaders.set(new TableReaders(services::repositoriesService, services.environment(), services.threadPool()));
        // the mount actions get them by injection, so they are components
        List<Object> components = new ArrayList<>(List.of(parquetFiles, tableReaders.get()));
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
    public List<ExecutorBuilder<?>> getExecutorBuilders(Settings settings) {
        List<ExecutorBuilder<?>> builders = new ArrayList<>(ParquetThreadPools.builders(settings));
        builders.add(ParquetBlobCache.fetchExecutorBuilder(settings));
        return builders;
    }

    /** Pins the primary of a parquet index, whose local files do not move with a shard, and lets its search replicas go anywhere. */
    @Override
    public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
        return List.of(new ParquetAllocationDecider());
    }

    /** The primary of a parquet index only indexes, and its replicas only search: no read ever reaches the primary. */
    @Override
    public ShardRoutingRoleStrategy getShardRoutingRoleStrategy() {
        return new ParquetShardRoutingRoleStrategy();
    }

    @Override
    public List<Setting<?>> getSettings() {
        List<Setting<?>> settings = new ArrayList<>(ParquetFormatSettings.settings());
        settings.addAll(ParquetBackedSettings.all());
        settings.addAll(ParquetBlobCache.settings());
        return settings;
    }

    @Override
    public Collection<IndexSettingProvider> getAdditionalIndexSettingProviders(IndexSettingProvider.Parameters parameters) {
        return List.of(new ParquetIndexSettingProvider());
    }

    @Override
    public List<ActionHandler> getActions() {
        return List.of(
            new ActionHandler(PreviewParquetAction.INSTANCE, TransportPreviewParquetAction.class),
            new ActionHandler(MountParquetAction.INSTANCE, TransportMountParquetAction.class),
            new ActionHandler(PublishTableVersionAction.TYPE, PublishTableVersionAction.TransportAction.class)
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
    public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        Settings settings = indexSettings.getSettings();
        if (ParquetFormatSettings.isParquet(settings)) {
            // The primary writes, and the search replicas follow the table. The role strategy makes the primary the only promotable
            // copy, and routes every read to the search replicas, so the write engine never serves a read.
            return Optional.of(config -> {
                if (config.isPromotableToPrimary()) {
                    return new ParquetWriteEngine(config, this::openTable, this::publishTableVersion);
                }
                return newSearchReplicaEngine(config);
            });
        }
        if (ParquetBackedSettings.isTableMount(settings)) {
            return Optional.of(this::newMountedTableEngine);
        }
        if (ParquetBackedSettings.isParquetBacked(settings)) {
            return Optional.of(new ParquetEngineFactory(files::get, cache::get));
        }
        return Optional.empty();
    }

    /**
     * Tells the search replicas of a shard of the write engine that its primary committed a new version of the table, through the refresh
     * of the unpromotable copies. The request runs as the internal user of {@code post_write_refresh}, which a refresh after a write uses
     * too, because the flush or the compaction that calls this has no user.
     */
    private void publishTableVersion(ShardId shardId, long primaryTerm, long version) {
        final ThreadContext threadContext = threadPool.getThreadContext();
        try (ThreadContext.StoredContext ignored = threadContext.stashWithOrigin(PostWriteRefresh.POST_WRITE_REFRESH_ORIGIN)) {
            client.execute(
                PublishTableVersionAction.TYPE,
                new PublishTableVersionAction.Request(shardId, primaryTerm, version),
                ActionListener.wrap(
                    r -> logger.trace("the search replicas of {} show version [{}] of the table", shardId, version),
                    // the replicas still read the table on their scheduled refresh
                    e -> logger.debug(() -> "failed to announce version [" + version + "] of the table of " + shardId, e)
                )
            );
        }
    }

    /**
     * The engine of a search replica of an index of the write engine: it serves the files of its shard in the table of the index, of
     * every history of the shard, in the order of their histories and sequence numbers.
     */
    private Engine newSearchReplicaEngine(EngineConfig config) {
        final ShardId shardId = config.getShardId();
        final Settings indexSettings = config.getIndexSettings().getSettings();
        final String repositoryName = ParquetFormatSettings.REPOSITORY.get(indexSettings);
        final List<String> tablePath = tablePath(shardId, indexSettings);
        final SetOnce<TableReader> reader = new SetOnce<>();
        final ParquetReadContext.Resolver resolver = ParquetEngineFactory.resolver(files::get, cache::get);
        final ParquetTableFollowEngine.TableAccess access = new ParquetTableFollowEngine.TableAccess() {
            @Override
            public TableReader tableReader() throws IOException {
                if (reader.get() == null) {
                    reader.trySet(openTableReader(shardId, indexSettings));
                }
                return reader.get();
            }

            @Override
            public FileLocation locate(TableReader.DataFileEntry file) {
                final TableReader tableReader = reader.get();
                assert tableReader != null : "a file is located after the table was read";
                return new RepositoryLocation(repositoryName, String.join("/", tablePath) + "/" + file.pathInTable(tableReader.location()));
            }

            @Override
            public ParquetReadContext.Resolver resolver() {
                return resolver;
            }
        };
        return new ParquetTableFollowEngine(
            config,
            access,
            contents -> filesOfShard(shardId, contents),
            ParquetTableFollowEngine.MAPPED_FIELDS,
            // the reads of the table use the blob store of the repository, which only generic and snapshot threads may get
            threadPool.generic()
        );
    }

    /**
     * The engine of a copy of an index that mounts an Iceberg table: it serves the share of the data files of the table that belongs to
     * its shard, with the fields that the mount derived.
     */
    private Engine newMountedTableEngine(EngineConfig config) {
        final ShardId shardId = config.getShardId();
        final Settings indexSettings = config.getIndexSettings().getSettings();
        final String repositoryName = ParquetBackedSettings.REPOSITORY.get(indexSettings);
        final List<String> tablePath = ParquetBackedSettings.tablePathSegments(ParquetBackedSettings.TABLE_PATH.get(indexSettings));
        final List<ParquetBackedSettings.FieldSpec> fields = ParquetBackedSettings.fieldSpecs(indexSettings);
        final int numberOfShards = config.getIndexSettings().getNumberOfShards();
        final SetOnce<TableReader> reader = new SetOnce<>();
        final ParquetReadContext.Resolver resolver = ParquetEngineFactory.resolver(files::get, cache::get);
        final ParquetTableFollowEngine.TableAccess access = new ParquetTableFollowEngine.TableAccess() {
            @Override
            public TableReader tableReader() throws IOException {
                if (reader.get() == null) {
                    final ProjectId projectId = clusterService.state().metadata().projectFor(shardId.getIndex()).id();
                    reader.trySet(tableReaders.get().open(projectId, repositoryName, tablePath));
                }
                return reader.get();
            }

            @Override
            public FileLocation locate(TableReader.DataFileEntry file) {
                return new RepositoryLocation(
                    repositoryName,
                    String.join("/", tablePath) + "/" + file.pathInTable(reader.get().location())
                );
            }

            @Override
            public ParquetReadContext.Resolver resolver() {
                return resolver;
            }
        };
        return new ParquetTableFollowEngine(
            config,
            access,
            contents -> filesOfMountedShard(shardId.id(), numberOfShards, contents),
            mappingLookup -> fields,
            // the reads of the table use the blob store of the repository, which only generic and snapshot threads may get
            threadPool.generic()
        );
    }

    /**
     * The data files of a table that belong to one shard of an index that mounts it: a file belongs to the shard that the hash of its
     * path chooses, so it stays in its shard whatever other files the table gains or loses. The files are ordered by the commit that
     * added them, so that an append adds stubs after the ones that the shard has.
     */
    static List<TableReader.DataFileEntry> filesOfMountedShard(int shard, int numberOfShards, TableReader.Contents contents) {
        final List<TableReader.DataFileEntry> files = new ArrayList<>();
        for (TableReader.DataFileEntry file : contents.files()) {
            if (Math.floorMod(Murmur3HashFunction.hash(file.location()), numberOfShards) == shard) {
                files.add(file);
            }
        }
        files.sort(Comparator.comparingLong(TableReader.DataFileEntry::sequenceNumber).thenComparing(TableReader.DataFileEntry::location));
        return files;
    }

    /**
     * The files of one shard of the table of an index of the write engine, of all the histories of the shard. A file of the write
     * engine is named by its shard, its history and its range of sequence numbers, and the files are ordered by history and range.
     *
     * @throws IllegalStateException if another index owns the table
     */
    static List<TableReader.DataFileEntry> filesOfShard(ShardId shardId, TableReader.Contents contents) {
        final String owner = TableReader.ownerIndexUuid(contents.properties());
        if (owner != null && owner.equals(shardId.getIndex().getUUID()) == false) {
            throw new IllegalStateException(
                "the table of " + shardId.getIndex() + " belongs to the index [" + owner + "]; set another [index.parquet.base_path]"
            );
        }
        final List<TableReader.DataFileEntry> files = new ArrayList<>();
        final Map<TableReader.DataFileEntry, TableReader.ShardFileName> names = new HashMap<>();
        for (TableReader.DataFileEntry file : contents.files()) {
            final TableReader.ShardFileName name = TableReader.parseShardFileName(file.location());
            if (name != null && name.shard() == shardId.id()) {
                files.add(file);
                names.put(file, name);
            }
        }
        files.sort(
            Comparator.comparing((TableReader.DataFileEntry file) -> names.get(file).historyUuid())
                .thenComparingLong(file -> names.get(file).fromSeqNo())
        );
        return files;
    }

    /** A reader of the table of the index of the shard, which never creates or writes the table. */
    private TableReader openTableReader(ShardId shardId, Settings indexSettings) throws IOException {
        final ProjectId projectId = clusterService.state().metadata().projectFor(shardId.getIndex()).id();
        return tableReaders.get().open(projectId, ParquetFormatSettings.REPOSITORY.get(indexSettings), tablePath(shardId, indexSettings));
    }

    private BlobStoreRepository blobStoreRepository(ShardId shardId, Settings indexSettings) throws IOException {
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
        return (BlobStoreRepository) repository;
    }

    /**
     * The table of the index of the shard, in the repository that the index names. Protected so that a test can replace it: it is the
     * place where a flush first needs the libraries of the table.
     */
    protected ShardTable openTable(ShardId shardId, Settings indexSettings, String historyUuid) throws IOException {
        final BlobStoreRepository blobStoreRepository = blobStoreRepository(shardId, indexSettings);
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
            shardId.getIndexName(),
            shardId.getIndex().getUUID(),
            historyUuid,
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
        return TableReaders.blobStoreOf(threadPool, repository);
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
