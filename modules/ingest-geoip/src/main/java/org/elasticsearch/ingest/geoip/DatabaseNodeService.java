/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.ingest.geoip.stats.CacheStats;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.Closeable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.ingest.geoip.EnterpriseGeoIpTaskState.getEnterpriseGeoIpTaskState;
import static org.elasticsearch.ingest.geoip.GeoIpDownloaderTaskExecutor.getTaskId;
import static org.elasticsearch.ingest.geoip.GeoIpTaskState.getGeoIpTaskState;

/**
 * A component that is responsible for making the databases maintained by {@link GeoIpDownloader}
 * available to ingest processors on each ingest node.
 * <p>
 * Also provides a lookup mechanism for geoip processors with fallback to {@link ConfigDatabases}.
 * All databases are downloaded into a geoip tmp directory, which is created at node startup.
 * <p>
 * The following high level steps are executed after each cluster state update:
 * 1) Check which databases are available in {@link GeoIpTaskState},
 * which is part of the geoip downloader persistent task.
 * 2) For each database check whether the databases have changed
 * by comparing the local and remote md5 hash or are locally missing.
 * 3) For each database identified in step 2 start downloading the database
 * chunks. Each chunk is appended to a tmp file (inside geoip tmp dir) and
 * after all chunks have been downloaded, the database is uncompressed and
 * renamed to the final filename.After this the database is loaded and
 * if there is an old instance of this database then that is closed.
 * 4) Cleanup locally loaded databases that are no longer mentioned in {@link GeoIpTaskState}.
 */
public final class DatabaseNodeService implements IpDatabaseProvider {

    private static final Logger logger = LogManager.getLogger(DatabaseNodeService.class);

    private final Client client;
    private final GeoIpCache cache;
    private final Path geoipTmpBaseDirectory;
    private Path geoipTmpDirectory;
    private final ConfigDatabases configDatabases;
    private final Consumer<Runnable> genericExecutor;
    private final ClusterService clusterService;
    private IngestService ingestService;
    private ProjectResolver projectResolver;

    private final ConcurrentMap<ProjectId, ConcurrentMap<String, DatabaseReaderLazyLoader>> databases = new ConcurrentHashMap<>();

    DatabaseNodeService(
        Environment environment,
        Client client,
        GeoIpCache cache,
        Consumer<Runnable> genericExecutor,
        ClusterService clusterService
    ) {
        this(
            environment.tmpDir(),
            new OriginSettingClient(client, IngestService.INGEST_ORIGIN),
            cache,
            new ConfigDatabases(environment, cache),
            genericExecutor,
            clusterService
        );
    }

    DatabaseNodeService(
        Path tmpDir,
        Client client,
        GeoIpCache cache,
        ConfigDatabases configDatabases,
        Consumer<Runnable> genericExecutor,
        ClusterService clusterService
    ) {
        this.client = client;
        this.cache = cache;
        this.geoipTmpBaseDirectory = tmpDir.resolve("geoip-databases");
        this.configDatabases = configDatabases;
        this.genericExecutor = genericExecutor;
        this.clusterService = clusterService;
    }

    public void initialize(
        String nodeId,
        ResourceWatcherService resourceWatcher,
        IngestService ingestServiceArg,
        ProjectResolver projectResolver
    ) throws IOException {
        configDatabases.initialize(resourceWatcher);
        geoipTmpDirectory = geoipTmpBaseDirectory.resolve(nodeId);
        // delete all stale files in the geoip tmp directory
        Files.walkFileTree(geoipTmpDirectory, new FileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                try {
                    logger.info("deleting stale file [{}]", file);
                    Files.deleteIfExists(file);
                } catch (IOException e) {
                    logger.warn("can't delete stale file [" + file + "]", e);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException e) {
                if (e instanceof NoSuchFileException == false) {
                    logger.warn("can't delete stale file [{}]", file, e);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) {
                return FileVisitResult.CONTINUE;
            }
        });
        if (Files.exists(geoipTmpDirectory) == false) {
            Files.createDirectories(geoipTmpDirectory);
        }
        logger.debug("initialized database node service, using geoip-databases directory [{}]", geoipTmpDirectory);
        this.ingestService = ingestServiceArg;
        clusterService.addListener(event -> checkDatabases(event.state()));
        this.projectResolver = projectResolver;
    }

    @Override
    public Boolean isValid(ProjectId projectId, String databaseFile) {
        ProjectState projectState = clusterService.state().projectState(projectId);
        assert projectState != null;

        GeoIpTaskState state = getGeoIpTaskState(projectState.metadata(), getTaskId(projectId, projectResolver.supportsMultipleProjects()));
        if (state == null) {
            return true;
        }

        GeoIpTaskState.Metadata metadata = state.getDatabases().get(databaseFile);
        // we never remove metadata from cluster state, if metadata is null we deal with built-in database, which is always valid
        if (metadata == null) {
            return true;
        }

        boolean valid = metadata.isNewEnough(projectState.cluster().metadata().settings());
        if (valid && metadata.isCloseToExpiration()) {
            HeaderWarning.addWarning(
                "database [{}] was not updated for over 25 days, geoip processor will stop working if there is no update for 30 days",
                databaseFile
            );
        }

        return valid;
    }

    // for testing only:
    DatabaseReaderLazyLoader getDatabaseReaderLazyLoader(ProjectId projectId, String name) {
        // There is a need for reference counting in order to avoid using an instance
        // that gets closed while using it. (this can happen during a database update)
        while (true) {
            DatabaseReaderLazyLoader instance = getProjectLazyLoader(projectId, name);
            if (instance == null) {
                instance = configDatabases.getDatabase(name);
            }
            if (instance == null || instance.preLookup()) {
                return instance;
            }
            // instance is closed after incrementing its usage,
            // drop this instance and fetch another one.
        }
    }

    @Override
    public IpDatabase getDatabase(ProjectId projectId, String name) {
        return getDatabaseReaderLazyLoader(projectId, name);
    }

    List<DatabaseReaderLazyLoader> getAllDatabases() {
        List<DatabaseReaderLazyLoader> all = new ArrayList<>(configDatabases.getConfigDatabases().values());
        this.databases.forEach((key, value) -> all.addAll(value.values()));
        return all;
    }

    // for testing only:
    DatabaseReaderLazyLoader get(ProjectId projectId, String key) {
        return databases.computeIfAbsent(projectId, (k) -> new ConcurrentHashMap<>()).get(key);
    }

    public void shutdown() throws IOException {
        // this is a little 'fun' looking, but it's just adapting IOUtils.close() into something
        // that can call a bunch of shutdown methods (rather than close methods)
        final var loadersToShutdown = databases.values()
            .stream()
            .flatMap(map -> map.values().stream())
            .map(ShutdownCloseable::new)
            .toList();
        databases.clear();
        IOUtils.close(loadersToShutdown);
    }

    private record ShutdownCloseable(DatabaseReaderLazyLoader loader) implements Closeable {
        @Override
        public void close() throws IOException {
            if (loader != null) {
                loader.shutdown();
            }
        }
    }

    @FixForMultiProject(description = "revisit, may need to use project specific settings")
    void checkDatabases(ClusterState state) {
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        DiscoveryNode localNode = state.nodes().getLocalNode();
        if (localNode.isIngestNode() == false) {
            logger.trace("Not checking databases because local node is not ingest node");
            return;
        }

        // Optimization: only load the .geoip_databases index for projects that are allocated to this node
        for (ProjectMetadata projectMetadata : state.getMetadata().projects().values()) {
            ProjectId projectId = projectMetadata.id();

            PersistentTasksCustomMetadata persistentTasks = state.metadata()
                .getProject(projectId)
                .custom(PersistentTasksCustomMetadata.TYPE);
            if (persistentTasks == null) {
                logger.trace("Not checking databases for project [{}] because persistent tasks are null", projectId);
                continue;
            }

            IndexAbstraction databasesAbstraction = projectMetadata.getIndicesLookup().get(GeoIpDownloader.DATABASES_INDEX);
            if (databasesAbstraction == null) {
                logger.trace("Not checking databases because geoip databases index does not exist for project [{}]", projectId);
                return;
            } else {
                // regardless of whether DATABASES_INDEX is an alias, resolve it to a concrete index
                Index databasesIndex = databasesAbstraction.getWriteIndex();
                IndexRoutingTable databasesIndexRT = state.routingTable(projectId).index(databasesIndex);
                if (databasesIndexRT == null || databasesIndexRT.allPrimaryShardsActive() == false) {
                    logger.trace(
                        "Not checking databases because geoip databases index does not have all active primary shards for"
                            + " project [{}]",
                        projectId
                    );
                    return;
                }
            }

            // we'll consult each of the geoip downloaders to build up a list of database metadatas to work with
            List<Tuple<String, GeoIpTaskState.Metadata>> validMetadatas = new ArrayList<>();

            // process the geoip task state for the (ordinary) geoip downloader
            {
                GeoIpTaskState taskState = getGeoIpTaskState(
                    projectMetadata,
                    getTaskId(projectId, projectResolver.supportsMultipleProjects())
                );
                if (taskState == null) {
                    // Note: an empty state will purge stale entries in databases map
                    taskState = GeoIpTaskState.EMPTY;
                }
                validMetadatas.addAll(
                    taskState.getDatabases()
                        .entrySet()
                        .stream()
                        // TODO: revisit to check project specific settings when it is implemented
                        .filter(e -> e.getValue().isNewEnough(state.getMetadata().settings()))
                        .map(entry -> Tuple.tuple(entry.getKey(), entry.getValue()))
                        .toList()
                );
            }

            // process the geoip task state for the enterprise geoip downloader
            {
                EnterpriseGeoIpTaskState taskState = getEnterpriseGeoIpTaskState(state);
                if (taskState == null) {
                    // Note: an empty state will purge stale entries in databases map
                    taskState = EnterpriseGeoIpTaskState.EMPTY;
                }
                validMetadatas.addAll(
                    taskState.getDatabases()
                        .entrySet()
                        .stream()
                        // TODO: revisit project specific settings when it is implemented
                        .filter(e -> e.getValue().isNewEnough(state.getMetadata().settings()))
                        .map(entry -> Tuple.tuple(entry.getKey(), entry.getValue()))
                        .toList()
                );
            }

            // run through all the valid metadatas, regardless of source, and retrieve them if the persistent downloader task
            // has downloaded a new version of the databases
            validMetadatas.forEach(e -> {
                String name = e.v1();
                GeoIpTaskState.Metadata metadata = e.v2();
                DatabaseReaderLazyLoader reference = getProjectLazyLoader(projectId, name);
                String remoteMd5 = metadata.md5();
                String localMd5 = reference != null ? reference.getMd5() : null;
                if (Objects.equals(localMd5, remoteMd5)) {
                    logger.debug("[{}] is up to date [{}] with cluster state [{}]", name, localMd5, remoteMd5);
                    return;
                }

                try {
                    retrieveAndUpdateDatabase(projectId, name, metadata);
                } catch (Exception ex) {
                    logger.error(() -> "failed to retrieve database [" + name + "]", ex);
                }
            });

            // TODO perhaps we need to handle the license flap persistent task state better than we do
            // i think the ideal end state is that we *do not* drop the files that the enterprise downloader
            // handled if they fall out -- which means we need to track that in the databases map itself

            // start with the list of all databases we currently know about in this service,
            // then drop the ones that didn't check out as valid from the task states
            Set<String> staleDatabases = databases.get(projectId) == null ? Set.of() : databases.get(projectId).keySet();
            staleDatabases.removeAll(validMetadatas.stream().map(Tuple::v1).collect(Collectors.toSet()));
            removeStaleEntries(projectId, staleDatabases);
        }
    }

    void retrieveAndUpdateDatabase(ProjectId projectId, String databaseName, GeoIpTaskState.Metadata metadata) throws IOException {
        logger.trace("retrieving database [{}]", databaseName);
        final String recordedMd5 = metadata.md5();

        Path databaseTmpDirectory = getDatabaseTmpDirectory(projectId);
        // This acts as a lock to avoid multiple retrievals of the same database at the same time. If this method for a specific db is
        // executed later again while a previous retrival of this db is still ongoing then FileAlreadyExistsException is thrown and
        // this method silently returns.
        // (this method is never invoked concurrently and is invoked by a cluster state applier thread)
        final Path retrievedFile;
        try {
            // TODO: save to project specific tmp directory
            retrievedFile = Files.createFile(databaseTmpDirectory.resolve(databaseName + ".tmp.retrieved"));
        } catch (FileAlreadyExistsException e) {
            logger.debug("database update [{}] already in progress, skipping...", databaseName);
            return;
        }

        // 2 types of threads:
        // 1) The thread that checks whether database should be retrieved / updated and creates (^) tmp file (cluster state applied thread)
        // 2) the thread that retrieves the db file from the .geoip_databases index, updates the databases map and then removes the tmp file
        // Thread 2 may have updated the databases map after thread 1 detects that there is no entry (or md5 mismatch) for a database.
        // If thread 2 then also removes the tmp file before thread 1 attempts to create it then we're about to retrieve the same database
        // twice. This check is here to avoid this:
        DatabaseReaderLazyLoader lazyLoader = getProjectLazyLoader(projectId, databaseName);
        if (lazyLoader != null && recordedMd5.equals(lazyLoader.getMd5())) {
            logger.debug("deleting tmp file because database [{}] has already been updated.", databaseName);
            Files.delete(retrievedFile);
            return;
        }

        final Path databaseTmpFile = Files.createFile(databaseTmpDirectory.resolve(databaseName + ".tmp"));
        logger.debug("retrieving database [{}] from [{}] to [{}]", databaseName, GeoIpDownloader.DATABASES_INDEX, retrievedFile);
        // TODO: save to project specific tmp directory
        retrieveDatabase(
            projectId,
            databaseName,
            recordedMd5,
            metadata,
            bytes -> Files.write(retrievedFile, bytes, StandardOpenOption.APPEND),
            () -> {
                final Path databaseFile = databaseTmpDirectory.resolve(databaseName);

                boolean isTarGz = MMDBUtil.isGzip(retrievedFile);
                if (isTarGz) {
                    // tarball contains <database_name>.mmdb, LICENSE.txt, COPYRIGHTS.txt and optional README.txt files.
                    // we store mmdb file as is and prepend database name to all other entries to avoid conflicts
                    logger.debug("decompressing [{}]", retrievedFile.getFileName());
                    try (TarInputStream is = new TarInputStream(new GZIPInputStream(Files.newInputStream(retrievedFile), 8192))) {
                        TarInputStream.TarEntry entry;
                        while ((entry = is.getNextEntry()) != null) {
                            // there might be ./ entry in tar, we should skip it
                            if (entry.notFile()) {
                                continue;
                            }
                            // flatten structure, remove any directories present from the path (should be ./ only)
                            String name = entry.name().substring(entry.name().lastIndexOf('/') + 1);
                            if (name.startsWith(databaseName)) {
                                Files.copy(is, databaseTmpFile, StandardCopyOption.REPLACE_EXISTING);
                            } else {
                                Files.copy(
                                    is,
                                    databaseTmpDirectory.resolve(databaseName + "_" + name),
                                    StandardCopyOption.REPLACE_EXISTING
                                );
                            }
                        }
                    }
                } else {
                    /*
                     * Given that this is not code that will be called extremely frequently, we copy the file to the
                     * expected location here in order to avoid making the rest of the code more complex to avoid this.
                     */
                    Files.copy(retrievedFile, databaseTmpFile, StandardCopyOption.REPLACE_EXISTING);
                }
                // finally, atomically move some-database.mmdb.tmp to some-database.mmdb
                logger.debug("moving database from [{}] to [{}]", databaseTmpFile, databaseFile);
                Files.move(databaseTmpFile, databaseFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                updateDatabase(projectId, databaseName, recordedMd5, databaseFile);
                Files.delete(retrievedFile);
            },
            failure -> {
                logger.error(() -> "failed to retrieve database [" + databaseName + "]", failure);
                try {
                    Files.deleteIfExists(databaseTmpFile);
                    Files.deleteIfExists(retrievedFile);
                } catch (IOException ioe) {
                    ioe.addSuppressed(failure);
                    logger.error("unable to delete tmp database file after failure", ioe);
                }
            }
        );
    }

    void updateDatabase(ProjectId projectId, String databaseFileName, String recordedMd5, Path file) {
        try {
            logger.debug("starting reload of changed database file [{}]", file);
            DatabaseReaderLazyLoader loader = new DatabaseReaderLazyLoader(cache, file, recordedMd5);
            DatabaseReaderLazyLoader existing = databases.computeIfAbsent(projectId, (k) -> new ConcurrentHashMap<>())
                .put(databaseFileName, loader);
            if (existing != null) {
                existing.shutdown();
            } else {
                // Loaded a database for the first time, so reload pipelines for which a database was not available:
                Predicate<GeoIpProcessor.DatabaseUnavailableProcessor> predicate = p -> databaseFileName.equals(p.getDatabaseName());
                var ids = ingestService.getPipelineWithProcessorType(
                    projectId,
                    GeoIpProcessor.DatabaseUnavailableProcessor.class,
                    predicate
                );
                if (ids.isEmpty() == false) {
                    logger.debug("pipelines [{}] found to reload", ids);
                    for (var id : ids) {
                        try {
                            ingestService.reloadPipeline(projectId, id);
                            logger.trace(
                                "successfully reloaded pipeline [{}] after downloading of database [{}] for the first time",
                                id,
                                databaseFileName
                            );
                        } catch (Exception e) {
                            logger.debug(
                                () -> format("failed to reload pipeline [%s] after downloading of database [%s]", id, databaseFileName),
                                e
                            );
                        }
                    }
                } else {
                    logger.debug("no pipelines found to reload");
                }
            }
            logger.info("successfully loaded database file [{}]", file.getFileName());
        } catch (Exception e) {
            logger.error(() -> "failed to update database [" + databaseFileName + "]", e);
        }
    }

    void removeStaleEntries(ProjectId projectId, Collection<String> staleEntries) {
        ConcurrentMap<String, DatabaseReaderLazyLoader> projectLoaders = databases.get(projectId);
        assert projectLoaders != null;
        for (String staleEntry : staleEntries) {
            try {
                logger.debug("database [{}] for project [{}] no longer exists, cleaning up...", staleEntry, projectId);
                DatabaseReaderLazyLoader existing = projectLoaders.remove(staleEntry);
                assert existing != null;
                existing.shutdown(true);
            } catch (Exception e) {
                logger.error(() -> "failed to clean database [" + staleEntry + "] for project [" + projectId + "]", e);
            }
        }
    }

    // This method issues search request to retrieves the database chunks from the .geoip_databases index and passes
    // them to the chunkConsumer (which appends the data to a tmp file). This method forks to the generic thread pool to do the search.
    void retrieveDatabase(
        ProjectId projectId,
        String databaseName,
        String expectedMd5,
        GeoIpTaskState.Metadata metadata,
        CheckedConsumer<byte[], IOException> chunkConsumer,
        CheckedRunnable<Exception> completedHandler,
        Consumer<Exception> failureHandler
    ) {
        // Search in the project specific .geoip_databases
        projectResolver.executeOnProject(projectId, () -> {
            // Need to run the search from a different thread, since this is executed from cluster state applier thread:
            genericExecutor.accept(() -> {
                MessageDigest md = MessageDigests.md5();
                int firstChunk = metadata.firstChunk();
                int lastChunk = metadata.lastChunk();
                try {
                    // TODO: invoke open point in time api when this api is moved from xpack core to server module.
                    // (so that we have a consistent view of the chunk documents while doing the lookups)
                    // (the chance that the documents change is rare, given the low frequency of the updates for these databases)
                    for (int chunk = firstChunk; chunk <= lastChunk; chunk++) {
                        SearchRequest searchRequest = new SearchRequest(GeoIpDownloader.DATABASES_INDEX);
                        String id = String.format(Locale.ROOT, "%s_%d_%d", databaseName, chunk, metadata.lastUpdate());
                        searchRequest.source().query(new TermQueryBuilder("_id", id));

                        // At most once a day a few searches may be executed to fetch the new files,
                        // so it is ok if this happens in a blocking manner on a thread from generic thread pool.
                        // This makes the code easier to understand and maintain.
                        // TODO: we should revisit if blocking the generic thread pool for search is still acceptable,
                        // since in multi-project mode each project will have its own geoip databases index search
                        SearchResponse searchResponse = client.search(searchRequest).actionGet();
                        try {
                            SearchHit[] hits = searchResponse.getHits().getHits();

                            if (searchResponse.getHits().getHits().length == 0) {
                                failureHandler.accept(new ResourceNotFoundException("chunk document with id [" + id + "] not found"));
                                return;
                            }
                            byte[] data = (byte[]) hits[0].getSourceAsMap().get("data");
                            md.update(data);
                            chunkConsumer.accept(data);
                        } finally {
                            searchResponse.decRef();
                        }
                    }
                    String actualMd5 = MessageDigests.toHexString(md.digest());
                    if (Objects.equals(expectedMd5, actualMd5)) {
                        completedHandler.run();
                    } else {
                        failureHandler.accept(
                            new RuntimeException("expected md5 hash [" + expectedMd5 + "], but got md5 hash [" + actualMd5 + "]")
                        );
                    }
                } catch (Exception e) {
                    failureHandler.accept(e);
                }
            });
        });
    }

    public Set<String> getAvailableDatabases(ProjectId projectId) {
        var loaders = databases.get(projectId);
        return loaders == null ? Set.of() : Set.copyOf(loaders.keySet());
    }

    public Set<String> getConfigDatabases() {
        return configDatabases.getConfigDatabases().keySet();
    }

    public Map<String, ConfigDatabaseDetail> getConfigDatabasesDetail() {
        Map<String, ConfigDatabaseDetail> allDatabases = new HashMap<>();
        for (Map.Entry<String, DatabaseReaderLazyLoader> entry : configDatabases.getConfigDatabases().entrySet()) {
            DatabaseReaderLazyLoader databaseReaderLazyLoader = entry.getValue();
            try {
                allDatabases.put(
                    entry.getKey(),
                    new ConfigDatabaseDetail(
                        entry.getKey(),
                        databaseReaderLazyLoader.getMd5(),
                        databaseReaderLazyLoader.getBuildDateMillis(),
                        databaseReaderLazyLoader.getDatabaseType()
                    )
                );
            } catch (FileNotFoundException e) {
                /*
                 * Since there is nothing to prevent a database from being deleted while this method is running, it is possible we get an
                 * exception here because the file no longer exists. We just log it and move on -- it's preferable to synchronization.
                 */
                logger.trace(Strings.format("Unable to get metadata for config database %s", entry.getKey()), e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return allDatabases;
    }

    public record ConfigDatabaseDetail(String name, @Nullable String md5, @Nullable Long buildDateInMillis, @Nullable String type) {}

    public Set<String> getFilesInTemp(ProjectId projectId) {
        try (Stream<Path> files = Files.list(getDatabaseTmpDirectory(projectId))) {
            return files.map(Path::getFileName).map(Path::toString).collect(Collectors.toSet());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public CacheStats getCacheStats() {
        return cache.getCacheStats();
    }

    private DatabaseReaderLazyLoader getProjectLazyLoader(ProjectId projectId, String databaseName) {
        return databases.computeIfAbsent(projectId, (k) -> new ConcurrentHashMap<>()).get(databaseName);
    }

    private Path getDatabaseTmpDirectory(ProjectId projectId) {
        Path path = projectResolver.supportsMultipleProjects() ? geoipTmpDirectory.resolve(projectId.toString()) : geoipTmpDirectory;
        try {
            if (Files.exists(path) == false) {
                Files.createDirectories(path);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to create geoip tmp directory for project [" + projectId + "]", e);
        }
        return path;
    }
}
