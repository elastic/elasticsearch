/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.env;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Strings;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.elasticsearch.Build;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Predicates;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.gateway.CorruptStateException;
import org.elasticsearch.gateway.MetadataStateFormat;
import org.elasticsearch.gateway.PersistedClusterStateService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.monitor.fs.FsInfo;
import org.elasticsearch.monitor.fs.FsProbe;
import org.elasticsearch.monitor.jvm.HotThreads;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.core.Strings.format;

/**
 * A component that holds all data paths for a single node.
 */
public final class NodeEnvironment implements Closeable {
    public static class DataPath {
        /* ${data.paths} */
        public final Path path;
        /* ${data.paths}/indices */
        public final Path indicesPath;
        /** Cached FileStore from path */
        public final FileStore fileStore;

        public final int majorDeviceNumber;
        public final int minorDeviceNumber;

        public DataPath(Path path) throws IOException {
            this.path = path;
            this.indicesPath = path.resolve(INDICES_FOLDER);
            this.fileStore = Environment.getFileStore(path);
            if (fileStore.supportsFileAttributeView("lucene")) {
                this.majorDeviceNumber = (int) fileStore.getAttribute("lucene:major_device_number");
                this.minorDeviceNumber = (int) fileStore.getAttribute("lucene:minor_device_number");
            } else {
                this.majorDeviceNumber = -1;
                this.minorDeviceNumber = -1;
            }
        }

        /**
         * Resolves the given shards directory against this DataPath
         * ${data.paths}/indices/{index.uuid}/{shard.id}
         */
        public Path resolve(ShardId shardId) {
            return resolve(shardId.getIndex()).resolve(Integer.toString(shardId.id()));
        }

        /**
         * Resolves index directory against this DataPath
         * ${data.paths}/indices/{index.uuid}
         */
        public Path resolve(Index index) {
            return resolve(index.getUUID());
        }

        Path resolve(String uuid) {
            return indicesPath.resolve(uuid);
        }

        @Override
        public String toString() {
            return "DataPath{"
                + "path="
                + path
                + ", indicesPath="
                + indicesPath
                + ", fileStore="
                + fileStore
                + ", majorDeviceNumber="
                + majorDeviceNumber
                + ", minorDeviceNumber="
                + minorDeviceNumber
                + '}';
        }

    }

    private final Logger logger = LogManager.getLogger(NodeEnvironment.class);
    private final DataPath[] dataPaths;
    private final Path sharedDataPath;
    private final Lock[] locks;

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Map<ShardId, InternalShardLock> shardLocks = new HashMap<>();

    private final NodeMetadata nodeMetadata;

    /**
     * Seed for determining a persisted unique uuid of this node. If the node has already a persisted uuid on disk,
     * this seed will be ignored and the uuid from disk will be reused.
     */
    public static final Setting<Long> NODE_ID_SEED_SETTING = Setting.longSetting("node.id.seed", 0L, Long.MIN_VALUE, Property.NodeScope);

    /**
     * If true the [verbose] SegmentInfos.infoStream logging is sent to System.out.
     */
    public static final Setting<Boolean> ENABLE_LUCENE_SEGMENT_INFOS_TRACE_SETTING = Setting.boolSetting(
        "node.enable_lucene_segment_infos_trace",
        false,
        Property.NodeScope
    );

    public static final String INDICES_FOLDER = "indices";
    public static final String NODE_LOCK_FILENAME = "node.lock";

    /**
     * Searchable snapshot's Lucene index directory.
     */
    private static final String SNAPSHOT_CACHE_FOLDER = "snapshot_cache";

    /**
     * Searchable snapshot's shared cache file
     */
    static final String SEARCHABLE_SHARED_CACHE_FILE = "shared_snapshot_cache";

    public static final class NodeLock implements Releasable {

        private final Lock[] locks;
        private final DataPath[] dataPaths;

        public NodeLock(final Logger logger, final Environment environment, final CheckedFunction<Path, Boolean, IOException> pathFunction)
            throws IOException {
            this(logger, environment, pathFunction, Function.identity());
        }

        /**
         * Tries to acquire a node lock for a node id, throws {@code IOException} if it is unable to acquire it
         * @param pathFunction function to check node path before attempt of acquiring a node lock
         */
        public NodeLock(
            final Logger logger,
            final Environment environment,
            final CheckedFunction<Path, Boolean, IOException> pathFunction,
            final Function<Path, Path> subPathMapping
        ) throws IOException {
            dataPaths = new DataPath[environment.dataDirs().length];
            locks = new Lock[dataPaths.length];
            try {
                final Path[] dataPaths = environment.dataDirs();
                for (int dirIndex = 0; dirIndex < dataPaths.length; dirIndex++) {
                    Path dataDir = dataPaths[dirIndex];
                    Path dir = subPathMapping.apply(dataDir);
                    if (pathFunction.apply(dir) == false) {
                        continue;
                    }
                    try (Directory luceneDir = FSDirectory.open(dir, NativeFSLockFactory.INSTANCE)) {
                        logger.trace("obtaining node lock on {} ...", dir.toAbsolutePath());
                        locks[dirIndex] = luceneDir.obtainLock(NODE_LOCK_FILENAME);
                        this.dataPaths[dirIndex] = new DataPath(dir);
                    } catch (IOException e) {
                        logger.trace(() -> format("failed to obtain node lock on %s", dir.toAbsolutePath()), e);
                        // release all the ones that were obtained up until now
                        throw (e instanceof LockObtainFailedException
                            ? e
                            : new IOException("failed to obtain lock on " + dir.toAbsolutePath(), e));
                    }
                }
            } catch (IOException e) {
                close();
                throw e;
            }
        }

        public DataPath[] getDataPaths() {
            return dataPaths;
        }

        @Override
        public void close() {
            for (int i = 0; i < locks.length; i++) {
                if (locks[i] != null) {
                    IOUtils.closeWhileHandlingException(locks[i]);
                }
                locks[i] = null;
            }
        }
    }

    /**
     * Setup the environment.
     * @param settings settings from elasticsearch.yml
     * @param environment global environment
     */
    public NodeEnvironment(Settings settings, Environment environment) throws IOException {
        boolean success = false;

        try {
            sharedDataPath = environment.sharedDataDir();

            for (Path path : environment.dataDirs()) {
                if (Files.exists(path)) {
                    // Call to toRealPath required to resolve symlinks.
                    // We let it fall through to create directories to ensure the symlink
                    // isn't a file instead of a directory.
                    path = path.toRealPath();
                }
                Files.createDirectories(path);
            }

            final NodeLock nodeLock;
            try {
                nodeLock = new NodeLock(logger, environment, dir -> true);
            } catch (IOException e) {
                final String message = String.format(
                    Locale.ROOT,
                    "failed to obtain node locks, tried %s;"
                        + " maybe these locations are not writable or multiple nodes were started on the same data path?",
                    Arrays.toString(environment.dataDirs())
                );
                throw new IllegalStateException(message, e);
            }

            this.locks = nodeLock.locks;
            this.dataPaths = nodeLock.dataPaths;

            logger.debug("using node location {}", Arrays.toString(dataPaths));

            maybeLogPathDetails();
            maybeLogHeapDetails();

            applySegmentInfosTrace(settings);
            assertCanWrite();

            ensureAtomicMoveSupported(dataPaths);

            if (upgradeLegacyNodeFolders(logger, settings, environment, nodeLock)) {
                assertCanWrite();
            }

            // versions 7.x and earlier put their data under ${path.data}/nodes/; leave a file at that location to prevent downgrades
            for (Path dataPath : environment.dataDirs()) {
                final Path legacyNodesPath = dataPath.resolve("nodes");
                if (Files.isRegularFile(legacyNodesPath) == false) {
                    final String content = "written by Elasticsearch "
                        + Build.current().version()
                        + " to prevent a downgrade to a version prior to v8.0.0 which would result in data loss";
                    Files.writeString(legacyNodesPath, content);
                    IOUtils.fsync(legacyNodesPath, false);
                    IOUtils.fsync(dataPath, true);
                }
            }

            if (DiscoveryNode.canContainData(settings) == false) {
                if (DiscoveryNode.isMasterNode(settings) == false) {
                    ensureNoIndexMetadata(dataPaths);
                }

                ensureNoShardData(dataPaths);
            }

            this.nodeMetadata = loadNodeMetadata(settings, logger, dataPaths);

            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    /**
     * Upgrades all data paths that have been written to by an older ES version to the 8.0+ compatible folder layout,
     * removing the "nodes/${lockId}" folder prefix
     */
    private static boolean upgradeLegacyNodeFolders(Logger logger, Settings settings, Environment environment, NodeLock nodeLock)
        throws IOException {
        boolean upgradeNeeded = false;

        // check if we can do an auto-upgrade
        for (Path path : environment.dataDirs()) {
            final Path nodesFolderPath = path.resolve("nodes");
            if (Files.isDirectory(nodesFolderPath)) {
                final List<Integer> nodeLockIds = new ArrayList<>();

                try (DirectoryStream<Path> stream = Files.newDirectoryStream(nodesFolderPath)) {
                    for (Path nodeLockIdPath : stream) {
                        String fileName = nodeLockIdPath.getFileName().toString();
                        if (Files.isDirectory(nodeLockIdPath) && fileName.chars().allMatch(Character::isDigit)) {
                            int nodeLockId = Integer.parseInt(fileName);
                            nodeLockIds.add(nodeLockId);
                        } else if (FileSystemUtils.isDesktopServicesStore(nodeLockIdPath) == false) {
                            throw new IllegalStateException(
                                "unexpected file/folder encountered during data folder upgrade: " + nodeLockIdPath
                            );
                        }
                    }
                }

                if (nodeLockIds.isEmpty() == false) {
                    upgradeNeeded = true;

                    if (nodeLockIds.equals(Arrays.asList(0)) == false) {
                        throw new IllegalStateException(
                            "data path "
                                + nodesFolderPath
                                + " cannot be upgraded automatically because it "
                                + "contains data from nodes with ordinals "
                                + nodeLockIds
                                + ", due to previous use of the now obsolete "
                                + "[node.max_local_storage_nodes] setting. Please check the breaking changes docs for the current version "
                                + "of Elasticsearch to find an upgrade path"
                        );
                    }
                }
            }
        }

        if (upgradeNeeded == false) {
            logger.trace("data folder upgrade not required");
            return false;
        }

        logger.info("upgrading legacy data folders: {}", Arrays.toString(environment.dataDirs()));

        // acquire locks on legacy path for duration of upgrade (to ensure there is no older ES version running on this path)
        final NodeLock legacyNodeLock;
        try {
            legacyNodeLock = new NodeLock(logger, environment, dir -> true, path -> path.resolve("nodes").resolve("0"));
        } catch (IOException e) {
            final String message = String.format(
                Locale.ROOT,
                "failed to obtain legacy node locks, tried %s;"
                    + " maybe these locations are not writable or multiple nodes were started on the same data path?",
                Arrays.toString(environment.dataDirs())
            );
            throw new IllegalStateException(message, e);
        }

        // move contents from legacy path to new path
        assert nodeLock.getDataPaths().length == legacyNodeLock.getDataPaths().length;
        try {
            // first check if we are upgrading from an index compatible version
            checkForIndexCompatibility(logger, legacyNodeLock.getDataPaths());

            final List<CheckedRunnable<IOException>> upgradeActions = new ArrayList<>();
            for (int i = 0; i < legacyNodeLock.getDataPaths().length; i++) {
                final DataPath legacyDataPath = legacyNodeLock.getDataPaths()[i];
                final DataPath dataPath = nodeLock.getDataPaths()[i];

                // determine folders to move and check that there are no extra files/folders
                final Set<String> folderNames = new HashSet<>();
                final Set<String> expectedFolderNames = new HashSet<>(
                    Arrays.asList(

                        // node state directory, containing MetadataStateFormat-based node metadata as well as cluster state
                        MetadataStateFormat.STATE_DIR_NAME,

                        // indices
                        INDICES_FOLDER,

                        // searchable snapshot cache Lucene index
                        SNAPSHOT_CACHE_FOLDER
                    )
                );

                final Set<String> ignoredFileNames = new HashSet<>(
                    Arrays.asList(
                        NODE_LOCK_FILENAME,
                        TEMP_FILE_NAME,
                        TEMP_FILE_NAME + ".tmp",
                        TEMP_FILE_NAME + ".final",
                        SEARCHABLE_SHARED_CACHE_FILE
                    )
                );

                try (DirectoryStream<Path> stream = Files.newDirectoryStream(legacyDataPath.path)) {
                    for (Path subFolderPath : stream) {
                        final String fileName = subFolderPath.getFileName().toString();
                        if (FileSystemUtils.isDesktopServicesStore(subFolderPath)) {
                            // ignore
                        } else if (FileSystemUtils.isAccessibleDirectory(subFolderPath, logger)) {
                            if (expectedFolderNames.contains(fileName) == false) {
                                throw new IllegalStateException(
                                    "unexpected folder encountered during data folder upgrade: " + subFolderPath
                                );
                            }
                            final Path targetSubFolderPath = dataPath.path.resolve(fileName);
                            if (Files.exists(targetSubFolderPath)) {
                                throw new IllegalStateException(
                                    "target folder already exists during data folder upgrade: " + targetSubFolderPath
                                );
                            }
                            folderNames.add(fileName);
                        } else if (ignoredFileNames.contains(fileName) == false) {
                            throw new IllegalStateException(
                                "unexpected file/folder encountered during data folder upgrade: " + subFolderPath
                            );
                        }
                    }
                }

                assert Sets.difference(folderNames, expectedFolderNames).isEmpty()
                    : "expected indices and/or state dir folder but was " + folderNames;

                upgradeActions.add(() -> {
                    for (String folderName : folderNames) {
                        final Path sourceSubFolderPath = legacyDataPath.path.resolve(folderName);
                        final Path targetSubFolderPath = dataPath.path.resolve(folderName);
                        Files.move(sourceSubFolderPath, targetSubFolderPath, StandardCopyOption.ATOMIC_MOVE);
                        logger.info("data folder upgrade: moved from [{}] to [{}]", sourceSubFolderPath, targetSubFolderPath);
                    }
                    IOUtils.fsync(dataPath.path, true);
                });
            }
            // now do the actual upgrade
            for (CheckedRunnable<IOException> upgradeAction : upgradeActions) {
                upgradeAction.run();
            }

        } finally {
            legacyNodeLock.close();
        }

        // upgrade successfully completed, remove legacy nodes folders
        IOUtils.rm(Stream.of(environment.dataDirs()).map(path -> path.resolve("nodes")).toArray(Path[]::new));

        return true;
    }

    /**
     * Checks to see if we can upgrade to this version based on the existing index state. Upgrading
     * from older versions can cause irreversible changes if allowed.
     */
    static void checkForIndexCompatibility(Logger logger, DataPath... dataPaths) throws IOException {
        final Path[] paths = Arrays.stream(dataPaths).map(np -> np.path).toArray(Path[]::new);
        NodeMetadata metadata = PersistedClusterStateService.nodeMetadata(paths);

        // We are upgrading the cluster, but we didn't find any previous metadata. Corrupted state or incompatible version.
        if (metadata == null) {
            throw new CorruptStateException(
                "Format version is not supported. Upgrading to ["
                    + Build.current().version()
                    + "] is only supported from version ["
                    + Build.current().minWireCompatVersion()
                    + "]."
            );
        }

        metadata.verifyUpgradeToCurrentVersion();

        logger.info("oldest index version recorded in NodeMetadata {}", metadata.oldestIndexVersion());

        if (metadata.oldestIndexVersion().before(IndexVersions.MINIMUM_COMPATIBLE)) {
            BuildVersion bestDowngradeVersion = getBestDowngradeVersion(metadata.previousNodeVersion());
            throw new IllegalStateException(
                "Cannot start this node because it holds metadata for indices with version ["
                    + metadata.oldestIndexVersion().toReleaseVersion()
                    + "] with which this node of version ["
                    + Build.current().version()
                    + "] is incompatible. Revert this node to version ["
                    + bestDowngradeVersion
                    + "] and delete any indices with versions earlier than ["
                    + IndexVersions.MINIMUM_COMPATIBLE.toReleaseVersion()
                    + "] before upgrading to version ["
                    + Build.current().version()
                    + "]. If all such indices have already been deleted, revert this node to version ["
                    + bestDowngradeVersion
                    + "] and wait for it to join the cluster to clean up any older indices from its metadata."
            );
        }
    }

    private void maybeLogPathDetails() throws IOException {

        // We do some I/O in here, so skip this if DEBUG/INFO are not enabled:
        if (logger.isDebugEnabled()) {
            // Log one line per path.data:
            StringBuilder sb = new StringBuilder();
            for (DataPath dataPath : dataPaths) {
                sb.append('\n').append(" -> ").append(dataPath.path.toAbsolutePath());

                FsInfo.Path fsPath = FsProbe.getFSInfo(dataPath);
                sb.append(", free_space [")
                    .append(fsPath.getFree())
                    .append("], usable_space [")
                    .append(fsPath.getAvailable())
                    .append("], total_space [")
                    .append(fsPath.getTotal())
                    .append("], mount [")
                    .append(fsPath.getMount())
                    .append("], type [")
                    .append(fsPath.getType())
                    .append(']');
            }
            logger.debug("node data locations details:{}", sb);
        } else if (logger.isInfoEnabled()) {
            FsInfo.Path totFSPath = new FsInfo.Path();
            Set<String> allTypes = new HashSet<>();
            Set<String> allMounts = new HashSet<>();
            for (DataPath dataPath : dataPaths) {
                FsInfo.Path fsPath = FsProbe.getFSInfo(dataPath);
                String mount = fsPath.getMount();
                if (allMounts.contains(mount) == false) {
                    allMounts.add(mount);
                    String type = fsPath.getType();
                    if (type != null) {
                        allTypes.add(type);
                    }
                    totFSPath.add(fsPath);
                }
            }

            // Just log a 1-line summary:
            logger.info(
                "using [{}] data paths, mounts [{}], net usable_space [{}], net total_space [{}], types [{}]",
                dataPaths.length,
                allMounts,
                totFSPath.getAvailable(),
                totFSPath.getTotal(),
                toString(allTypes)
            );
        }
    }

    private void maybeLogHeapDetails() {
        JvmInfo jvmInfo = JvmInfo.jvmInfo();
        ByteSizeValue maxHeapSize = jvmInfo.getMem().getHeapMax();
        String useCompressedOops = jvmInfo.useCompressedOops();
        logger.info("heap size [{}], compressed ordinary object pointers [{}]", maxHeapSize, useCompressedOops);
    }

    /**
     * scans the node paths and loads existing metadata file. If not found a new meta data will be generated
     */
    private static NodeMetadata loadNodeMetadata(Settings settings, Logger logger, DataPath... dataPaths) throws IOException {
        final Path[] paths = Arrays.stream(dataPaths).map(np -> np.path).toArray(Path[]::new);
        NodeMetadata metadata = PersistedClusterStateService.nodeMetadata(paths);
        if (metadata == null) {
            // load legacy metadata
            final Set<String> nodeIds = new HashSet<>();
            for (final Path path : paths) {
                final NodeMetadata oldStyleMetadata = NodeMetadata.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, path);
                if (oldStyleMetadata != null) {
                    nodeIds.add(oldStyleMetadata.nodeId());
                }
            }
            if (nodeIds.size() > 1) {
                throw new IllegalStateException("data paths " + Arrays.toString(paths) + " belong to multiple nodes with IDs " + nodeIds);
            }
            // load legacy metadata
            final NodeMetadata legacyMetadata = NodeMetadata.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, paths);
            if (legacyMetadata == null) {
                assert nodeIds.isEmpty() : nodeIds;
                // If we couldn't find legacy metadata, we set the latest index version to this version. This happens
                // when we are starting a new node and there are no indices to worry about.
                metadata = new NodeMetadata(generateNodeId(settings), BuildVersion.current(), IndexVersion.current());
            } else {
                assert nodeIds.equals(Collections.singleton(legacyMetadata.nodeId())) : nodeIds + " doesn't match " + legacyMetadata;
                metadata = legacyMetadata;
            }
        }

        metadata = metadata.upgradeToCurrentVersion();
        assert metadata.nodeVersion().equals(BuildVersion.current()) : metadata.nodeVersion() + " != " + Build.current();

        return metadata;
    }

    public static String generateNodeId(Settings settings) {
        Random random = Randomness.get(settings, NODE_ID_SEED_SETTING);
        return UUIDs.randomBase64UUID(random);
    }

    @SuppressForbidden(reason = "System.out.*")
    static void applySegmentInfosTrace(Settings settings) {
        if (ENABLE_LUCENE_SEGMENT_INFOS_TRACE_SETTING.get(settings)) {
            SegmentInfos.setInfoStream(System.out);
        }
    }

    private static String toString(Collection<String> items) {
        StringBuilder b = new StringBuilder();
        for (String item : items) {
            if (b.length() > 0) {
                b.append(", ");
            }
            b.append(item);
        }
        return b.toString();
    }

    /**
     * Deletes a shard data directory iff the shards locks were successfully acquired.
     *
     * @param shardId the id of the shard to delete to delete
     * @throws IOException if an IOException occurs
     */
    public void deleteShardDirectorySafe(ShardId shardId, IndexSettings indexSettings, Consumer<Path[]> listener) throws IOException,
        ShardLockObtainFailedException {
        final Path[] paths = availableShardPaths(shardId);
        logger.trace("deleting shard {} directory, paths: [{}]", shardId, paths);
        try (ShardLock lock = shardLock(shardId, "shard deletion under lock")) {
            deleteShardDirectoryUnderLock(lock, indexSettings, listener);
        }
    }

    /**
     * Acquires, then releases, all {@code write.lock} files in the given
     * shard paths. The "write.lock" file is assumed to be under the shard
     * path's "index" directory as used by Elasticsearch.
     *
     * @throws LockObtainFailedException if any of the locks could not be acquired
     */
    public static void acquireFSLockForPaths(IndexSettings indexSettings, Path... shardPaths) throws IOException {
        Lock[] locks = new Lock[shardPaths.length];
        Directory[] dirs = new Directory[shardPaths.length];
        try {
            for (int i = 0; i < shardPaths.length; i++) {
                // resolve the directory the shard actually lives in
                Path p = shardPaths[i].resolve("index");
                // open a directory (will be immediately closed) on the shard's location
                dirs[i] = new NIOFSDirectory(p, indexSettings.getValue(FsDirectoryFactory.INDEX_LOCK_FACTOR_SETTING));
                // create a lock for the "write.lock" file
                try {
                    locks[i] = dirs[i].obtainLock(IndexWriter.WRITE_LOCK_NAME);
                } catch (IOException ex) {
                    throw new LockObtainFailedException("unable to acquire " + IndexWriter.WRITE_LOCK_NAME + " for " + p, ex);
                }
            }
        } finally {
            IOUtils.closeWhileHandlingException(locks);
            IOUtils.closeWhileHandlingException(dirs);
        }
    }

    /**
     * Deletes a shard data directory. Note: this method assumes that the shard
     * lock is acquired. This method will also attempt to acquire the write
     * locks for the shard's paths before deleting the data, but this is best
     * effort, as the lock is released before the deletion happens in order to
     * allow the folder to be deleted
     *
     * @param lock the shards lock
     * @throws IOException if an IOException occurs
     * @throws ElasticsearchException if the write.lock is not acquirable
     */
    public void deleteShardDirectoryUnderLock(ShardLock lock, IndexSettings indexSettings, Consumer<Path[]> listener) throws IOException {
        final ShardId shardId = lock.getShardId();
        assert isShardLocked(shardId) : "shard " + shardId + " is not locked";
        final Path[] paths = availableShardPaths(shardId);
        logger.trace("acquiring locks for {}, paths: [{}]", shardId, paths);
        acquireFSLockForPaths(indexSettings, paths);
        listener.accept(paths);
        IOUtils.rm(paths);
        if (indexSettings.hasCustomDataPath()) {
            Path customLocation = resolveCustomLocation(indexSettings.customDataPath(), shardId);
            logger.trace("acquiring lock for {}, custom path: [{}]", shardId, customLocation);
            acquireFSLockForPaths(indexSettings, customLocation);
            logger.trace("deleting custom shard {} directory [{}]", shardId, customLocation);
            listener.accept(new Path[] { customLocation });
            IOUtils.rm(customLocation);
        }
        logger.trace("deleted shard {} directory, paths: [{}]", shardId, paths);
        assert assertPathsDoNotExist(paths);
    }

    private static boolean assertPathsDoNotExist(final Path[] paths) {
        Set<Path> existingPaths = Stream.of(paths).filter(FileSystemUtils::exists).filter(leftOver -> {
            // Relaxed assertion for the special case where only the empty state directory exists after deleting
            // the shard directory because it was created again as a result of a metadata read action concurrently.
            try (DirectoryStream<Path> children = Files.newDirectoryStream(leftOver)) {
                Iterator<Path> iter = children.iterator();
                if (iter.hasNext() == false) {
                    return true;
                }
                Path maybeState = iter.next();
                if (iter.hasNext() || maybeState.equals(leftOver.resolve(MetadataStateFormat.STATE_DIR_NAME)) == false) {
                    return true;
                }
                try (DirectoryStream<Path> stateChildren = Files.newDirectoryStream(maybeState)) {
                    return stateChildren.iterator().hasNext();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).collect(Collectors.toSet());
        assert existingPaths.size() == 0 : "Paths exist that should have been deleted: " + existingPaths;
        return existingPaths.size() == 0;
    }

    private boolean isShardLocked(ShardId id) {
        try {
            shardLock(id, "checking if shard is locked").close();
            return false;
        } catch (ShardLockObtainFailedException ex) {
            return true;
        }
    }

    /**
     * Deletes an indexes data directory recursively iff all of the indexes
     * shards locks were successfully acquired. If any of the indexes shard directories can't be locked
     * non of the shards will be deleted
     *
     * @param index the index to delete
     * @param lockTimeoutMS how long to wait for acquiring the indices shard locks
     * @param indexSettings settings for the index being deleted
     * @throws IOException if any of the shards data directories can't be locked or deleted
     */
    public void deleteIndexDirectorySafe(Index index, long lockTimeoutMS, IndexSettings indexSettings, Consumer<Path[]> listener)
        throws IOException, ShardLockObtainFailedException {
        final List<ShardLock> locks = lockAllForIndex(index, indexSettings, "deleting index directory", lockTimeoutMS);
        try {
            deleteIndexDirectoryUnderLock(index, indexSettings, listener);
        } finally {
            IOUtils.closeWhileHandlingException(locks);
        }
    }

    /**
     * Deletes an indexes data directory recursively.
     * Note: this method assumes that the shard lock is acquired
     *
     * @param index the index to delete
     * @param indexSettings settings for the index being deleted
     */
    public void deleteIndexDirectoryUnderLock(Index index, IndexSettings indexSettings, Consumer<Path[]> listener) throws IOException {
        final Path[] indexPaths = indexPaths(index);
        logger.trace("deleting index {} directory, paths({}): [{}]", index, indexPaths.length, indexPaths);
        listener.accept(indexPaths);
        IOUtils.rm(indexPaths);
        if (indexSettings.hasCustomDataPath()) {
            Path customLocation = resolveIndexCustomLocation(indexSettings.customDataPath(), index.getUUID());
            logger.trace("deleting custom index {} directory [{}]", index, customLocation);
            listener.accept(new Path[] { customLocation });
            IOUtils.rm(customLocation);
        }
    }

    /**
     * Tries to lock all local shards for the given index. If any of the shard locks can't be acquired
     * a {@link ShardLockObtainFailedException} is thrown and all previously acquired locks are released.
     *
     * @param index the index to lock shards for
     * @param lockTimeoutMS how long to wait for acquiring the indices shard locks
     * @return the {@link ShardLock} instances for this index.
     */
    public List<ShardLock> lockAllForIndex(
        final Index index,
        final IndexSettings settings,
        final String lockDetails,
        final long lockTimeoutMS
    ) throws ShardLockObtainFailedException {
        final int numShards = settings.getNumberOfShards();
        if (numShards <= 0) {
            throw new IllegalArgumentException("settings must contain a non-null > 0 number of shards");
        }
        logger.trace("locking all shards for index {} - [{}]", index, numShards);
        List<ShardLock> allLocks = new ArrayList<>(numShards);
        boolean success = false;
        long startTimeNS = System.nanoTime();
        try {
            for (int i = 0; i < numShards; i++) {
                long timeoutLeftMS = Math.max(0, lockTimeoutMS - TimeValue.nsecToMSec((System.nanoTime() - startTimeNS)));
                allLocks.add(shardLock(new ShardId(index, i), lockDetails, timeoutLeftMS));
            }
            success = true;
        } finally {
            if (success == false) {
                logger.trace("unable to lock all shards for index {}", index);
                IOUtils.closeWhileHandlingException(allLocks);
            }
        }
        return allLocks;
    }

    /**
     * Tries to lock the given shards ID. A shard lock is required to perform any kind of
     * write operation on a shards data directory like deleting files, creating a new index writer
     * or recover from a different shard instance into it. If the shard lock can not be acquired
     * a {@link ShardLockObtainFailedException} is thrown.
     *
     * Note: this method will return immediately if the lock can't be acquired.
     *
     * @param id the shard ID to lock
     * @param details information about why the shard is being locked
     * @return the shard lock. Call {@link ShardLock#close()} to release the lock
     */
    public ShardLock shardLock(ShardId id, final String details) throws ShardLockObtainFailedException {
        return shardLock(id, details, 0);
    }

    /**
     * Tries to lock the given shards ID. A shard lock is required to perform any kind of
     * write operation on a shards data directory like deleting files, creating a new index writer
     * or recover from a different shard instance into it. If the shard lock can not be acquired
     * a {@link ShardLockObtainFailedException} is thrown
     * @param shardId the shard ID to lock
     * @param details information about why the shard is being locked
     * @param lockTimeoutMS the lock timeout in milliseconds
     * @return the shard lock. Call {@link ShardLock#close()} to release the lock
     */
    public ShardLock shardLock(final ShardId shardId, final String details, final long lockTimeoutMS)
        throws ShardLockObtainFailedException {
        logger.trace("acquiring node shardlock on [{}], timeout [{}], details [{}]", shardId, lockTimeoutMS, details);
        final InternalShardLock shardLock;
        final boolean acquired;
        synchronized (shardLocks) {
            final InternalShardLock found = shardLocks.get(shardId);
            if (found != null) {
                shardLock = found;
                shardLock.incWaitCount();
                acquired = false;
            } else {
                shardLock = new InternalShardLock(shardId, details);
                shardLocks.put(shardId, shardLock);
                acquired = true;
            }
        }
        if (acquired == false) {
            boolean success = false;
            try {
                shardLock.acquire(lockTimeoutMS, details);
                success = true;
            } finally {
                if (success == false) {
                    shardLock.decWaitCount();
                }
            }
        }
        logger.trace("successfully acquired shardlock for [{}]", shardId);
        return new ShardLock(shardId) { // new instance prevents double closing
            @Override
            protected void closeInternal() {
                shardLock.release();
                logger.trace("released shard lock for [{}]", shardId);
            }

            @Override
            public void setDetails(String details) {
                shardLock.setDetails(details);
            }
        };
    }

    /**
     * A functional interface that people can use to reference {@link #shardLock(ShardId, String, long)}
     */
    @FunctionalInterface
    public interface ShardLocker {
        ShardLock lock(ShardId shardId, String lockDetails, long lockTimeoutMS) throws ShardLockObtainFailedException;
    }

    /**
     * Returns all currently lock shards.
     *
     * Note: the shard ids return do not contain a valid Index UUID
     */
    public Set<ShardId> lockedShards() {
        synchronized (shardLocks) {
            return Set.copyOf(shardLocks.keySet());
        }
    }

    // throttle the hot-threads calls: no more than one per minute
    private final Semaphore shardLockHotThreadsPermit = new Semaphore(1);
    private long nextShardLockHotThreadsNanos = Long.MIN_VALUE;

    private void maybeLogThreadDump(ShardId shardId, String message) {
        if (logger.isDebugEnabled() == false) {
            return;
        }

        final var prefix = format("hot threads while failing to obtain shard lock for %s: %s", shardId, message);
        if (shardLockHotThreadsPermit.tryAcquire()) {
            try {
                final var now = System.nanoTime();
                if (now <= nextShardLockHotThreadsNanos) {
                    return;
                }
                nextShardLockHotThreadsNanos = now + TimeUnit.SECONDS.toNanos(60);
                HotThreads.logLocalHotThreads(logger, Level.DEBUG, prefix, ReferenceDocs.SHARD_LOCK_TROUBLESHOOTING);
            } catch (Exception e) {
                logger.error(format("could not obtain %s", prefix), e);
            } finally {
                shardLockHotThreadsPermit.release();
            }
        }
    }

    private final class InternalShardLock {
        /*
         * This class holds a mutex for exclusive access and timeout / wait semantics
         * and a reference count to cleanup the shard lock instance form the internal data
         * structure if nobody is waiting for it. the wait count is guarded by the same lock
         * that is used to mutate the map holding the shard locks to ensure exclusive access
         */
        private final Semaphore mutex = new Semaphore(1);
        private int waitCount = 1; // guarded by shardLocks
        private final ShardId shardId;
        private volatile Tuple<Long, String> lockDetails;

        InternalShardLock(final ShardId shardId, final String details) {
            this.shardId = shardId;
            mutex.acquireUninterruptibly();
            lockDetails = Tuple.tuple(System.nanoTime(), details);
        }

        private void release() {
            mutex.release();
            decWaitCount();
        }

        void incWaitCount() {
            synchronized (shardLocks) {
                assert waitCount > 0 : "waitCount is " + waitCount + " but should be > 0";
                waitCount++;
            }
        }

        private void decWaitCount() {
            synchronized (shardLocks) {
                assert waitCount > 0 : "waitCount is " + waitCount + " but should be > 0";
                --waitCount;
                logger.trace("shard lock wait count for {} is now [{}]", shardId, waitCount);
                if (waitCount == 0) {
                    logger.trace("last shard lock wait decremented, removing lock for {}", shardId);
                    InternalShardLock remove = shardLocks.remove(shardId);
                    assert remove != null : "Removed lock was null";
                }
            }
        }

        void acquire(long timeoutInMillis, final String details) throws ShardLockObtainFailedException {
            try {
                if (mutex.tryAcquire(timeoutInMillis, TimeUnit.MILLISECONDS)) {
                    setDetails(details);
                } else {
                    final Tuple<Long, String> lockDetails = this.lockDetails; // single volatile read
                    final var message = format(
                        "obtaining shard lock for [%s] timed out after [%dms], lock already held for [%s] with age [%dms]",
                        details,
                        timeoutInMillis,
                        lockDetails.v2(),
                        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lockDetails.v1())
                    );
                    maybeLogThreadDump(shardId, message);
                    throw new ShardLockObtainFailedException(shardId, message);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ShardLockObtainFailedException(shardId, "thread interrupted while trying to obtain shard lock", e);
            }
        }

        public void setDetails(String details) {
            lockDetails = Tuple.tuple(System.nanoTime(), details);
        }
    }

    public boolean hasNodeFile() {
        return dataPaths != null && locks != null;
    }

    /**
     * Returns an array of all of the nodes data locations.
     * @throws IllegalStateException if the node is not configured to store local locations
     */
    public Path[] nodeDataPaths() {
        assertEnvIsLocked();
        Path[] paths = new Path[dataPaths.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = dataPaths[i].path;
        }
        return paths;
    }

    /**
     * Returns shared data path for this node environment
     */
    public Path sharedDataPath() {
        return sharedDataPath;
    }

    /**
     * returns the unique uuid describing this node. The uuid is persistent in the data folder of this node
     * and remains across restarts.
     **/
    public String nodeId() {
        // we currently only return the ID and hide the underlying nodeMetadata implementation in order to avoid
        // confusion with other "metadata" like node settings found in elasticsearch.yml. In future
        // we can encapsulate both (and more) in one NodeMetadata (or NodeSettings) object ala IndexSettings
        return nodeMetadata.nodeId();
    }

    /**
     * Returns the loaded NodeMetadata for this node
     */
    public NodeMetadata nodeMetadata() {
        return nodeMetadata;
    }

    /**
     * Returns an array of all of the {@link DataPath}s.
     */
    public DataPath[] dataPaths() {
        assertEnvIsLocked();
        if (dataPaths == null || locks == null) {
            throw new IllegalStateException("node is not configured to store local location");
        }
        return dataPaths;
    }

    /**
     * Returns all index paths.
     */
    public Path[] indexPaths(Index index) {
        assertEnvIsLocked();
        Path[] indexPaths = new Path[dataPaths.length];
        for (int i = 0; i < dataPaths.length; i++) {
            indexPaths[i] = dataPaths[i].resolve(index);
        }
        return indexPaths;
    }

    /**
     * Returns all shard paths excluding custom shard path. Note: Shards are only allocated on one of the
     * returned paths. The returned array may contain paths to non-existing directories.
     *
     * @see IndexSettings#hasCustomDataPath()
     * @see #resolveCustomLocation(String, ShardId)
     *
     */
    public Path[] availableShardPaths(ShardId shardId) {
        assertEnvIsLocked();
        final DataPath[] dataPaths = dataPaths();
        final Path[] shardLocations = new Path[dataPaths.length];
        for (int i = 0; i < dataPaths.length; i++) {
            shardLocations[i] = dataPaths[i].resolve(shardId);
        }
        return shardLocations;
    }

    /**
     * Returns all folder names in ${data.paths}/indices folder
     */
    public Set<String> availableIndexFolders() throws IOException {
        return availableIndexFolders(Predicates.never());
    }

    /**
     * Returns folder names in ${data.paths}/indices folder that don't match the given predicate.
     * @param excludeIndexPathIdsPredicate folder names to exclude
     */
    public Set<String> availableIndexFolders(Predicate<String> excludeIndexPathIdsPredicate) throws IOException {
        if (dataPaths == null || locks == null) {
            throw new IllegalStateException("node is not configured to store local location");
        }
        assertEnvIsLocked();
        Set<String> indexFolders = new HashSet<>();
        for (DataPath dataPath : dataPaths) {
            indexFolders.addAll(availableIndexFoldersForPath(dataPath, excludeIndexPathIdsPredicate));
        }
        return indexFolders;

    }

    /**
     * Return all directory names in the indices directory for the given node path.
     *
     * @param dataPath the path
     * @return all directories that could be indices for the given node path.
     * @throws IOException if an I/O exception occurs traversing the filesystem
     */
    public Set<String> availableIndexFoldersForPath(final DataPath dataPath) throws IOException {
        return availableIndexFoldersForPath(dataPath, Predicates.never());
    }

    /**
     * Return directory names in the indices directory for the given node path that don't match the given predicate.
     *
     * @param dataPath the path
     * @param excludeIndexPathIdsPredicate folder names to exclude
     * @return all directories that could be indices for the given node path.
     * @throws IOException if an I/O exception occurs traversing the filesystem
     */
    public Set<String> availableIndexFoldersForPath(final DataPath dataPath, Predicate<String> excludeIndexPathIdsPredicate)
        throws IOException {
        if (dataPaths == null || locks == null) {
            throw new IllegalStateException("node is not configured to store local location");
        }
        assertEnvIsLocked();
        final Set<String> indexFolders = new HashSet<>();
        Path indicesLocation = dataPath.indicesPath;
        if (Files.isDirectory(indicesLocation)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(indicesLocation)) {
                for (Path index : stream) {
                    final String fileName = index.getFileName().toString();
                    if (excludeIndexPathIdsPredicate.test(fileName) == false && Files.isDirectory(index)) {
                        indexFolders.add(fileName);
                    }
                }
            }
        }
        return indexFolders;
    }

    /**
     * Resolves all existing paths to <code>indexFolderName</code> in ${data.paths}/indices
     */
    public Path[] resolveIndexFolder(String indexFolderName) {
        if (dataPaths == null || locks == null) {
            throw new IllegalStateException("node is not configured to store local location");
        }
        assertEnvIsLocked();
        List<Path> paths = new ArrayList<>(dataPaths.length);
        for (DataPath dataPath : dataPaths) {
            Path indexFolder = dataPath.indicesPath.resolve(indexFolderName);
            if (Files.exists(indexFolder)) {
                paths.add(indexFolder);
            }
        }
        return paths.toArray(Path[]::new);
    }

    /**
     * Tries to find all allocated shards for the given index
     * on the current node. NOTE: This methods is prone to race-conditions on the filesystem layer since it might not
     * see directories created concurrently or while it's traversing.
     * @param index the index to filter shards
     * @return a set of shard IDs
     * @throws IOException if an IOException occurs
     */
    public Set<ShardId> findAllShardIds(final Index index) throws IOException {
        assert index != null;
        if (dataPaths == null || locks == null) {
            throw new IllegalStateException("node is not configured to store local location");
        }
        assertEnvIsLocked();
        final Set<ShardId> shardIds = new HashSet<>();
        final String indexUniquePathId = index.getUUID();
        for (final DataPath dataPath : dataPaths) {
            shardIds.addAll(findAllShardsForIndex(dataPath.indicesPath.resolve(indexUniquePathId), index));
        }
        return shardIds;
    }

    /**
     * Find all the shards for this index, returning a map of the {@code DataPath} to the number of shards on that path
     * @param index the index by which to filter shards
     * @return a map of DataPath to count of the shards for the index on that path
     * @throws IOException if an IOException occurs
     */
    public Map<DataPath, Long> shardCountPerPath(final Index index) throws IOException {
        assert index != null;
        if (dataPaths == null || locks == null) {
            throw new IllegalStateException("node is not configured to store local location");
        }
        assertEnvIsLocked();
        final Map<DataPath, Long> shardCountPerPath = new HashMap<>();
        final String indexUniquePathId = index.getUUID();
        for (final DataPath dataPath : dataPaths) {
            Path indexLocation = dataPath.indicesPath.resolve(indexUniquePathId);
            if (Files.isDirectory(indexLocation)) {
                shardCountPerPath.put(dataPath, (long) findAllShardsForIndex(indexLocation, index).size());
            }
        }
        return shardCountPerPath;
    }

    private static Set<ShardId> findAllShardsForIndex(Path indexPath, Index index) throws IOException {
        assert indexPath.getFileName().toString().equals(index.getUUID());
        Set<ShardId> shardIds = new HashSet<>();
        if (Files.isDirectory(indexPath)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(indexPath)) {
                for (Path shardPath : stream) {
                    String fileName = shardPath.getFileName().toString();
                    if (Files.isDirectory(shardPath) && fileName.chars().allMatch(Character::isDigit)) {
                        int shardId = Integer.parseInt(fileName);
                        ShardId id = new ShardId(index, shardId);
                        shardIds.add(id);
                    }
                }
            }
        }
        return shardIds;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true) && locks != null) {
            synchronized (locks) {
                for (Lock lock : locks) {
                    try {
                        logger.trace("releasing lock [{}]", lock);
                        lock.close();
                    } catch (IOException e) {
                        logger.trace(() -> "failed to release lock [" + lock + "]", e);
                    }
                }
            }
        }
    }

    private void assertEnvIsLocked() {
        if (closed.get() == false && locks != null) {
            synchronized (locks) {
                if (closed.get()) return; // raced with close() - we lost
                for (Lock lock : locks) {
                    try {
                        lock.ensureValid();
                    } catch (IOException e) {
                        logger.warn("lock assertion failed", e);
                        throw new IllegalStateException("environment is not locked", e);
                    }
                }
            }
        }
    }

    /**
     * This method tries to write an empty file and moves it using an atomic move operation.
     * This method throws an {@link IllegalStateException} if this operation is
     * not supported by the filesystem. This test is executed on each of the data directories.
     * This method cleans up all files even in the case of an error.
     */
    private static void ensureAtomicMoveSupported(final DataPath[] dataPaths) throws IOException {
        for (DataPath dataPath : dataPaths) {
            assert Files.isDirectory(dataPath.path) : dataPath.path + " is not a directory";
            final Path src = dataPath.path.resolve(TEMP_FILE_NAME + ".tmp");
            final Path target = dataPath.path.resolve(TEMP_FILE_NAME + ".final");
            try {
                Files.deleteIfExists(src);
                Files.createFile(src);
                Files.move(src, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException ex) {
                throw new IllegalStateException(
                    "atomic_move is not supported by the filesystem on path ["
                        + dataPath.path
                        + "] atomic_move is required for elasticsearch to work correctly.",
                    ex
                );
            } finally {
                try {
                    Files.deleteIfExists(src);
                } finally {
                    Files.deleteIfExists(target);
                }
            }
        }
    }

    private static void ensureNoShardData(final DataPath[] dataPaths) throws IOException {
        List<Path> shardDataPaths = collectShardDataPaths(dataPaths);
        if (shardDataPaths.isEmpty() == false) {
            final String message = String.format(
                Locale.ROOT,
                "node does not have the %s role but has shard data: %s. Use 'elasticsearch-node repurpose' tool to clean up",
                DiscoveryNodeRole.DATA_ROLE.roleName(),
                shardDataPaths
            );
            throw new IllegalStateException(message);
        }
    }

    private static void ensureNoIndexMetadata(final DataPath[] dataPaths) throws IOException {
        List<Path> indexMetadataPaths = collectIndexMetadataPaths(dataPaths);
        if (indexMetadataPaths.isEmpty() == false) {
            final String message = String.format(
                Locale.ROOT,
                "node does not have the %s and %s roles but has index metadata: %s. Use 'elasticsearch-node repurpose' tool to clean up",
                DiscoveryNodeRole.DATA_ROLE.roleName(),
                DiscoveryNodeRole.MASTER_ROLE.roleName(),
                indexMetadataPaths
            );
            throw new IllegalStateException(message);
        }
    }

    /**
     * Collect the paths containing shard data in the indicated node paths. The returned paths will point to the shard data folder.
     */
    static List<Path> collectShardDataPaths(DataPath[] dataPaths) throws IOException {
        return collectIndexSubPaths(dataPaths, NodeEnvironment::isShardPath);
    }

    /**
     * Collect the paths containing index meta data in the indicated node paths. The returned paths will point to the
     * {@link MetadataStateFormat#STATE_DIR_NAME} folder
     */
    static List<Path> collectIndexMetadataPaths(DataPath[] dataPaths) throws IOException {
        return collectIndexSubPaths(dataPaths, NodeEnvironment::isIndexMetadataPath);
    }

    private static List<Path> collectIndexSubPaths(DataPath[] dataPaths, Predicate<Path> subPathPredicate) throws IOException {
        List<Path> indexSubPaths = new ArrayList<>();
        for (DataPath dataPath : dataPaths) {
            Path indicesPath = dataPath.indicesPath;
            if (Files.isDirectory(indicesPath)) {
                try (DirectoryStream<Path> indexStream = Files.newDirectoryStream(indicesPath)) {
                    for (Path indexPath : indexStream) {
                        if (Files.isDirectory(indexPath)) {
                            try (Stream<Path> shardStream = Files.list(indexPath)) {
                                shardStream.filter(subPathPredicate).map(Path::toAbsolutePath).forEach(indexSubPaths::add);
                            }
                        }
                    }
                }
            }
        }

        return indexSubPaths;
    }

    private static boolean isShardPath(Path path) {
        return Files.isDirectory(path) && path.getFileName().toString().chars().allMatch(Character::isDigit);
    }

    private static boolean isIndexMetadataPath(Path path) {
        return Files.isDirectory(path) && path.getFileName().toString().equals(MetadataStateFormat.STATE_DIR_NAME);
    }

    /**
     * Resolve the custom path for a index's shard.
     */
    public static Path resolveBaseCustomLocation(String customDataPath, Path sharedDataPath) {
        if (Strings.isNotEmpty(customDataPath)) {
            // This assert is because this should be caught by MetadataCreateIndexService
            assert sharedDataPath != null;
            return sharedDataPath.resolve(customDataPath).resolve("0");
        } else {
            throw new IllegalArgumentException("no custom " + IndexMetadata.SETTING_DATA_PATH + " setting available");
        }
    }

    /**
     * Resolve the custom path for a index's shard.
     * Uses the {@code IndexMetadata.SETTING_DATA_PATH} setting to determine
     * the root path for the index.
     *
     * @param customDataPath the custom data path
     */
    private Path resolveIndexCustomLocation(String customDataPath, String indexUUID) {
        return resolveIndexCustomLocation(customDataPath, indexUUID, sharedDataPath);
    }

    private static Path resolveIndexCustomLocation(String customDataPath, String indexUUID, Path sharedDataPath) {
        return resolveBaseCustomLocation(customDataPath, sharedDataPath).resolve(indexUUID);
    }

    /**
     * Resolve the custom path for a index's shard.
     * Uses the {@code IndexMetadata.SETTING_DATA_PATH} setting to determine
     * the root path for the index.
     *
     * @param customDataPath the custom data path
     * @param shardId shard to resolve the path to
     */
    public Path resolveCustomLocation(String customDataPath, final ShardId shardId) {
        return resolveCustomLocation(customDataPath, shardId, sharedDataPath);
    }

    public static Path resolveCustomLocation(String customDataPath, final ShardId shardId, Path sharedDataPath) {
        return resolveIndexCustomLocation(customDataPath, shardId.getIndex().getUUID(), sharedDataPath).resolve(
            Integer.toString(shardId.id())
        );
    }

    /**
     * Returns the {@code DataPath.path} for this shard.
     */
    public static Path shardStatePathToDataPath(Path shardPath) {
        int count = shardPath.getNameCount();

        // Sanity check:
        assert Integer.parseInt(shardPath.getName(count - 1).toString()) >= 0;
        assert "indices".equals(shardPath.getName(count - 3).toString());

        return shardPath.getParent().getParent().getParent();
    }

    /**
     * This is a best effort to ensure that we actually have write permissions to write in all our data directories.
     * This prevents disasters if nodes are started under the wrong username etc.
     */
    private void assertCanWrite() throws IOException {
        for (Path path : nodeDataPaths()) { // check node-paths are writable
            tryWriteTempFile(path);
        }
        for (String indexFolderName : this.availableIndexFolders()) {
            for (Path indexPath : this.resolveIndexFolder(indexFolderName)) { // check index paths are writable
                Path indexStatePath = indexPath.resolve(MetadataStateFormat.STATE_DIR_NAME);
                tryWriteTempFile(indexStatePath);
                tryWriteTempFile(indexPath);
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(indexPath)) {
                    for (Path shardPath : stream) {
                        String fileName = shardPath.getFileName().toString();
                        if (Files.isDirectory(shardPath) && fileName.chars().allMatch(Character::isDigit)) {
                            Path indexDir = shardPath.resolve(ShardPath.INDEX_FOLDER_NAME);
                            Path statePath = shardPath.resolve(MetadataStateFormat.STATE_DIR_NAME);
                            Path translogDir = shardPath.resolve(ShardPath.TRANSLOG_FOLDER_NAME);
                            tryWriteTempFile(indexDir);
                            tryWriteTempFile(translogDir);
                            tryWriteTempFile(statePath);
                            tryWriteTempFile(shardPath);
                        }
                    }
                }
            }
        }
    }

    // package private for testing
    static final String TEMP_FILE_NAME = ".es_temp_file";

    private static void tryWriteTempFile(Path path) throws IOException {
        if (Files.exists(path)) {
            Path resolve = path.resolve(TEMP_FILE_NAME);
            try {
                // delete any lingering file from a previous failure
                Files.deleteIfExists(resolve);
                Files.createFile(resolve);
                Files.delete(resolve);
            } catch (IOException ex) {
                throw new IOException("failed to test writes in data directory [" + path + "] write permission is required", ex);
            }
        }
    }

    /**
     * Get a useful version string to direct a user's downgrade operation
     * <p>
     * Assuming that the index was compatible with {@code previousNodeVersion},
     * the user should downgrade to that {@code previousNodeVersion},
     * unless it's prior to the minimum compatible version,
     * in which case the user should downgrade to that instead.
     * (If the index version is so old that the minimum compatible version is incompatible with the index,
     * then the cluster was already borked before the node upgrade began,
     * and we can't probably help them without more info than we have here.)
     *
     * @return Version to downgrade to
     */
    // visible for testing
    static BuildVersion getBestDowngradeVersion(BuildVersion previousNodeVersion) {
        if (previousNodeVersion.onOrAfterMinimumCompatible()) {
            return previousNodeVersion;
        }
        return BuildVersion.current().minimumCompatibilityVersion();
    }

}
