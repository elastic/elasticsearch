/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.ingest.geoip;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.OriginSettingClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.watcher.ResourceWatcherService;

import java.io.BufferedInputStream;
import java.io.Closeable;
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
import static org.elasticsearch.persistent.PersistentTasksCustomMetadata.getTaskWithId;

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
public final class DatabaseNodeService implements GeoIpDatabaseProvider, Closeable {

    private static final Logger LOGGER = LogManager.getLogger(DatabaseNodeService.class);

    private final Client client;
    private final GeoIpCache cache;
    private final Path geoipTmpBaseDirectory;
    private Path geoipTmpDirectory;
    private final ConfigDatabases configDatabases;
    private final Consumer<Runnable> genericExecutor;
    private final ClusterService clusterService;
    private IngestService ingestService;

    private final ConcurrentMap<String, DatabaseReaderLazyLoader> databases = new ConcurrentHashMap<>();

    DatabaseNodeService(
        Environment environment,
        Client client,
        GeoIpCache cache,
        Consumer<Runnable> genericExecutor,
        ClusterService clusterService
    ) {
        this(
            environment.tmpFile(),
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

    public void initialize(String nodeId, ResourceWatcherService resourceWatcher, IngestService ingestServiceArg) throws IOException {
        configDatabases.initialize(resourceWatcher);
        geoipTmpDirectory = geoipTmpBaseDirectory.resolve(nodeId);
        Files.walkFileTree(geoipTmpDirectory, new FileVisitor<>() {
            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                try {
                    LOGGER.info("deleting stale file [{}]", file);
                    Files.deleteIfExists(file);
                } catch (IOException e) {
                    LOGGER.warn("can't delete stale file [" + file + "]", e);
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException e) {
                if (e instanceof NoSuchFileException == false) {
                    LOGGER.warn("can't delete stale file [" + file + "]", e);
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
        LOGGER.debug("initialized database node service, using geoip-databases directory [{}]", geoipTmpDirectory);
        this.ingestService = ingestServiceArg;
        clusterService.addListener(event -> checkDatabases(event.state()));
    }

    @Override
    public Boolean isValid(String databaseFile) {
        ClusterState currentState = clusterService.state();
        assert currentState != null;

        PersistentTasksCustomMetadata.PersistentTask<?> task = getTaskWithId(currentState, GeoIpDownloader.GEOIP_DOWNLOADER);
        if (task == null || task.getState() == null) {
            return true;
        }
        GeoIpTaskState state = (GeoIpTaskState) task.getState();
        GeoIpTaskState.Metadata metadata = state.getDatabases().get(databaseFile);
        // we never remove metadata from cluster state, if metadata is null we deal with built-in database, which is always valid
        if (metadata == null) {
            return true;
        }

        boolean valid = metadata.isValid(currentState.metadata().settings());
        if (valid && metadata.isCloseToExpiration()) {
            HeaderWarning.addWarning(
                "database [{}] was not updated for over 25 days, geoip processor will stop working if there is no update for 30 days",
                databaseFile
            );
        }

        return valid;
    }

    // for testing only:
    DatabaseReaderLazyLoader getDatabaseReaderLazyLoader(String name) {
        // There is a need for reference counting in order to avoid using an instance
        // that gets closed while using it. (this can happen during a database update)
        while (true) {
            DatabaseReaderLazyLoader instance = databases.get(name);
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
    public GeoIpDatabase getDatabase(String name) {
        return getDatabaseReaderLazyLoader(name);
    }

    List<DatabaseReaderLazyLoader> getAllDatabases() {
        List<DatabaseReaderLazyLoader> all = new ArrayList<>(configDatabases.getConfigDatabases().values());
        this.databases.forEach((key, value) -> all.add(value));
        return all;
    }

    // for testing only:
    DatabaseReaderLazyLoader get(String key) {
        return databases.get(key);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(databases.values());
    }

    void checkDatabases(ClusterState state) {
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        DiscoveryNode localNode = state.nodes().getLocalNode();
        if (localNode.isIngestNode() == false) {
            LOGGER.trace("Not checking databases because local node is not ingest node");
            return;
        }

        PersistentTasksCustomMetadata persistentTasks = state.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        if (persistentTasks == null) {
            LOGGER.trace("Not checking databases because persistent tasks are null");
            return;
        }

        IndexAbstraction databasesAbstraction = state.getMetadata().getIndicesLookup().get(GeoIpDownloader.DATABASES_INDEX);
        if (databasesAbstraction == null) {
            LOGGER.trace("Not checking databases because geoip databases index does not exist");
            return;
        } else {
            // regardless of whether DATABASES_INDEX is an alias, resolve it to a concrete index
            Index databasesIndex = databasesAbstraction.getWriteIndex();
            IndexRoutingTable databasesIndexRT = state.getRoutingTable().index(databasesIndex);
            if (databasesIndexRT == null || databasesIndexRT.allPrimaryShardsActive() == false) {
                LOGGER.trace("Not checking databases because geoip databases index does not have all active primary shards");
                return;
            }
        }

        PersistentTasksCustomMetadata.PersistentTask<?> task = PersistentTasksCustomMetadata.getTaskWithId(
            state,
            GeoIpDownloader.GEOIP_DOWNLOADER
        );
        // Empty state will purge stale entries in databases map.
        GeoIpTaskState taskState = task == null || task.getState() == null ? GeoIpTaskState.EMPTY : (GeoIpTaskState) task.getState();

        taskState.getDatabases().entrySet().stream().filter(e -> e.getValue().isValid(state.getMetadata().settings())).forEach(e -> {
            String name = e.getKey();
            GeoIpTaskState.Metadata metadata = e.getValue();
            DatabaseReaderLazyLoader reference = databases.get(name);
            String remoteMd5 = metadata.md5();
            String localMd5 = reference != null ? reference.getMd5() : null;
            if (Objects.equals(localMd5, remoteMd5)) {
                LOGGER.debug("Current reference of [{}] is up to date [{}] with was recorded in CS [{}]", name, localMd5, remoteMd5);
                return;
            }

            try {
                retrieveAndUpdateDatabase(name, metadata);
            } catch (Exception ex) {
                LOGGER.error((Supplier<?>) () -> "attempt to download database [" + name + "] failed", ex);
            }
        });

        List<String> staleEntries = new ArrayList<>(databases.keySet());
        staleEntries.removeAll(
            taskState.getDatabases()
                .entrySet()
                .stream()
                .filter(e -> e.getValue().isValid(state.getMetadata().settings()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet())
        );
        removeStaleEntries(staleEntries);
    }

    void retrieveAndUpdateDatabase(String databaseName, GeoIpTaskState.Metadata metadata) throws IOException {
        LOGGER.trace("Retrieving database {}", databaseName);
        final String recordedMd5 = metadata.md5();

        // This acts as a lock, if this method for a specific db is executed later and downloaded for this db is still ongoing then
        // FileAlreadyExistsException is thrown and this method silently returns.
        // (this method is never invoked concurrently and is invoked by a cluster state applier thread)
        final Path databaseTmpGzFile;
        try {
            databaseTmpGzFile = Files.createFile(geoipTmpDirectory.resolve(databaseName + ".tmp.gz"));
        } catch (FileAlreadyExistsException e) {
            LOGGER.debug("database update [{}] already in progress, skipping...", databaseName);
            return;
        }

        // 2 types of threads:
        // 1) The thread that checks whether database should be retrieved / updated and creates (^) tmp file (cluster state applied thread)
        // 2) the thread that retrieves the db file from the .geoip_databases index, updates the databases map and then removes the tmp file
        // Thread 2 may have updated the databases map after thread 1 detects that there is no entry (or md5 mismatch) for a database.
        // If thread 2 then also removes the tmp file before thread 1 attempts to create it then we're about to retrieve the same database
        // twice. This check is here to avoid this:
        DatabaseReaderLazyLoader lazyLoader = databases.get(databaseName);
        if (lazyLoader != null && recordedMd5.equals(lazyLoader.getMd5())) {
            LOGGER.debug("deleting tmp file because database [{}] has already been updated.", databaseName);
            Files.delete(databaseTmpGzFile);
            return;
        }

        final Path databaseTmpFile = Files.createFile(geoipTmpDirectory.resolve(databaseName + ".tmp"));
        LOGGER.debug("retrieve geoip database [{}] from [{}] to [{}]", databaseName, GeoIpDownloader.DATABASES_INDEX, databaseTmpGzFile);
        retrieveDatabase(
            databaseName,
            recordedMd5,
            metadata,
            bytes -> Files.write(databaseTmpGzFile, bytes, StandardOpenOption.APPEND),
            () -> {
                LOGGER.debug("decompressing [{}]", databaseTmpGzFile.getFileName());

                Path databaseFile = geoipTmpDirectory.resolve(databaseName);
                // tarball contains <database_name>.mmdb, LICENSE.txt, COPYRIGHTS.txt and optional README.txt files.
                // we store mmdb file as is and prepend database name to all other entries to avoid conflicts
                try (
                    TarInputStream is = new TarInputStream(
                        new GZIPInputStream(new BufferedInputStream(Files.newInputStream(databaseTmpGzFile)), 8192)
                    )
                ) {
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
                            Files.copy(is, geoipTmpDirectory.resolve(databaseName + "_" + name), StandardCopyOption.REPLACE_EXISTING);
                        }
                    }
                }

                LOGGER.debug("moving database from [{}] to [{}]", databaseTmpFile, databaseFile);
                Files.move(databaseTmpFile, databaseFile, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                updateDatabase(databaseName, recordedMd5, databaseFile);
                Files.delete(databaseTmpGzFile);
            },
            failure -> {
                LOGGER.error((Supplier<?>) () -> "failed to retrieve database [" + databaseName + "]", failure);
                try {
                    Files.deleteIfExists(databaseTmpFile);
                    Files.deleteIfExists(databaseTmpGzFile);
                } catch (IOException ioe) {
                    ioe.addSuppressed(failure);
                    LOGGER.error("Unable to delete tmp database file after failure", ioe);
                }
            }
        );
    }

    void updateDatabase(String databaseFileName, String recordedMd5, Path file) {
        try {
            LOGGER.debug("starting reload of changed geoip database file [{}]", file);
            DatabaseReaderLazyLoader loader = new DatabaseReaderLazyLoader(cache, file, recordedMd5);
            DatabaseReaderLazyLoader existing = databases.put(databaseFileName, loader);
            if (existing != null) {
                existing.close();
            } else {
                // Loaded a database for the first time, so reload pipelines for which a database was not available:
                Predicate<GeoIpProcessor.DatabaseUnavailableProcessor> predicate = p -> databaseFileName.equals(p.getDatabaseName());
                var ids = ingestService.getPipelineWithProcessorType(GeoIpProcessor.DatabaseUnavailableProcessor.class, predicate);
                if (ids.isEmpty() == false) {
                    LOGGER.debug("pipelines [{}] found to reload", ids);
                    for (var id : ids) {
                        try {
                            ingestService.reloadPipeline(id);
                            LOGGER.trace(
                                "successfully reloaded pipeline [{}] after downloading of database [{}] for the first time",
                                id,
                                databaseFileName
                            );
                        } catch (Exception e) {
                            LOGGER.debug(
                                () -> format("failed to reload pipeline [%s] after downloading of database [%s]", id, databaseFileName),
                                e
                            );
                        }
                    }
                } else {
                    LOGGER.debug("no pipelines found to reload");
                }
            }
            LOGGER.info("successfully loaded geoip database file [{}]", file.getFileName());
        } catch (Exception e) {
            LOGGER.error((Supplier<?>) () -> "failed to update database [" + databaseFileName + "]", e);
        }
    }

    void removeStaleEntries(Collection<String> staleEntries) {
        for (String staleEntry : staleEntries) {
            try {
                LOGGER.debug("database [{}] no longer exists, cleaning up...", staleEntry);
                DatabaseReaderLazyLoader existing = databases.remove(staleEntry);
                assert existing != null;
                existing.close(true);
            } catch (Exception e) {
                LOGGER.error((Supplier<?>) () -> "failed to clean database [" + staleEntry + "]", e);
            }
        }
    }

    void retrieveDatabase(
        String databaseName,
        String expectedMd5,
        GeoIpTaskState.Metadata metadata,
        CheckedConsumer<byte[], IOException> chunkConsumer,
        CheckedRunnable<Exception> completedHandler,
        Consumer<Exception> failureHandler
    ) {
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
                    SearchResponse searchResponse = client.search(searchRequest).actionGet();
                    SearchHit[] hits = searchResponse.getHits().getHits();

                    if (searchResponse.getHits().getHits().length == 0) {
                        failureHandler.accept(new ResourceNotFoundException("chunk document with id [" + id + "] not found"));
                        return;
                    }
                    byte[] data = (byte[]) hits[0].getSourceAsMap().get("data");
                    md.update(data);
                    chunkConsumer.accept(data);
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
    }

    public Set<String> getAvailableDatabases() {
        return Set.copyOf(databases.keySet());
    }

    public Set<String> getConfigDatabases() {
        return configDatabases.getConfigDatabases().keySet();
    }

    public Set<String> getFilesInTemp() {
        try (Stream<Path> files = Files.list(geoipTmpDirectory)) {
            return files.map(Path::getFileName).map(Path::toString).collect(Collectors.toSet());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
