/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RateLimiter;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardRestoreFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshots;
import org.elasticsearch.index.snapshots.blobstore.RateLimitingInputStream;
import org.elasticsearch.index.snapshots.blobstore.SlicedInputStream;
import org.elasticsearch.index.snapshots.blobstore.SnapshotFiles;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.GetSnapshotInfoContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryCleanupResult;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryData.SnapshotDetails;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.RepositoryOperation;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.RepositoryVerificationException;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.ShardSnapshotResult;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.AbortedSnapshotException;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import java.util.stream.Stream;

import static org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo.canonicalName;

/**
 * BlobStore - based implementation of Snapshot Repository
 * <p>
 * This repository works with any {@link BlobStore} implementation. The blobStore could be (and preferred) lazy initialized in
 * {@link #createBlobStore()}.
 * </p>
 * For in depth documentation on how exactly implementations of this class interact with the snapshot functionality please refer to the
 * documentation of the package {@link org.elasticsearch.repositories.blobstore}.
 */
public abstract class BlobStoreRepository extends AbstractLifecycleComponent implements Repository {
    private static final Logger logger = LogManager.getLogger(BlobStoreRepository.class);

    protected volatile RepositoryMetadata metadata;

    protected final ThreadPool threadPool;

    public static final String SNAPSHOT_PREFIX = "snap-";

    public static final String INDEX_FILE_PREFIX = "index-";

    public static final String INDEX_LATEST_BLOB = "index.latest";

    private static final String TESTS_FILE = "tests-";

    public static final String METADATA_PREFIX = "meta-";

    public static final String METADATA_NAME_FORMAT = METADATA_PREFIX + "%s.dat";

    public static final String SNAPSHOT_NAME_FORMAT = SNAPSHOT_PREFIX + "%s.dat";

    private static final String SNAPSHOT_INDEX_PREFIX = "index-";

    private static final String SNAPSHOT_INDEX_NAME_FORMAT = SNAPSHOT_INDEX_PREFIX + "%s";

    public static final String UPLOADED_DATA_BLOB_PREFIX = "__";

    // Expose a copy of URLRepository#TYPE here too, for a better error message until https://github.com/elastic/elasticsearch/issues/68918
    // is resolved.
    public static final String URL_REPOSITORY_TYPE = "url";

    /**
     * All {@link BlobStoreRepository} implementations can be made read-only by setting this key to {@code true} in their settings.
     */
    public static final String READONLY_SETTING_KEY = "readonly";

    /**
     * Prefix used for the identifiers of data blobs that were not actually written to the repository physically because their contents are
     * already stored in the metadata referencing them, i.e. in {@link BlobStoreIndexShardSnapshot} and
     * {@link BlobStoreIndexShardSnapshots}. This is the case for files for which {@link StoreFileMetadata#hashEqualsContents()} is
     * {@code true}.
     */
    private static final String VIRTUAL_DATA_BLOB_PREFIX = "v__";

    /**
     * When set to true metadata files are stored in compressed format. This setting doesn’t affect index
     * files that are already compressed by default. Changing the setting does not invalidate existing files since reads
     * do not observe the setting, instead they examine the file to see if it is compressed or not.
     */
    public static final Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("compress", true, Setting.Property.NodeScope);

    /**
     * Setting to disable caching of the latest repository data.
     */
    public static final Setting<Boolean> CACHE_REPOSITORY_DATA = Setting.boolSetting(
        "cache_repository_data",
        true,
        Setting.Property.Deprecated
    );

    /**
     * Size hint for the IO buffer size to use when reading from and writing to the repository.
     */
    public static final Setting<ByteSizeValue> BUFFER_SIZE_SETTING = Setting.byteSizeSetting(
        "io_buffer_size",
        ByteSizeValue.parseBytesSizeValue("128kb", "io_buffer_size"),
        ByteSizeValue.parseBytesSizeValue("8kb", "buffer_size"),
        ByteSizeValue.parseBytesSizeValue("16mb", "io_buffer_size"),
        Setting.Property.NodeScope
    );

    /**
     * Setting to disable writing the {@code index.latest} blob which enables the contents of this repository to be used with a
     * url-repository.
     */
    public static final Setting<Boolean> SUPPORT_URL_REPO = Setting.boolSetting("support_url_repo", true, Setting.Property.NodeScope);

    /**
     * Setting that defines the maximum number of snapshots to which the repository may grow. Trying to create a snapshot into the
     * repository that would move it above this size will throw an exception.
     */
    public static final Setting<Integer> MAX_SNAPSHOTS_SETTING = Setting.intSetting(
        "max_number_of_snapshots",
        Integer.MAX_VALUE,
        1,
        Setting.Property.NodeScope
    );

    protected final boolean supportURLRepo;

    private final boolean compress;

    private final boolean cacheRepositoryData;

    private volatile RateLimiter snapshotRateLimiter;

    private volatile RateLimiter restoreRateLimiter;

    private final CounterMetric snapshotRateLimitingTimeInNanos = new CounterMetric();

    private final CounterMetric restoreRateLimitingTimeInNanos = new CounterMetric();

    public static final ChecksumBlobStoreFormat<Metadata> GLOBAL_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "metadata",
        METADATA_NAME_FORMAT,
        (repoName, parser) -> Metadata.fromXContent(parser)
    );

    public static final ChecksumBlobStoreFormat<IndexMetadata> INDEX_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "index-metadata",
        METADATA_NAME_FORMAT,
        (repoName, parser) -> IndexMetadata.fromXContent(parser)
    );

    private static final String SNAPSHOT_CODEC = "snapshot";

    public static final ChecksumBlobStoreFormat<SnapshotInfo> SNAPSHOT_FORMAT = new ChecksumBlobStoreFormat<>(
        SNAPSHOT_CODEC,
        SNAPSHOT_NAME_FORMAT,
        SnapshotInfo::fromXContentInternal
    );

    public static final ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshot> INDEX_SHARD_SNAPSHOT_FORMAT = new ChecksumBlobStoreFormat<>(
        SNAPSHOT_CODEC,
        SNAPSHOT_NAME_FORMAT,
        (repoName, parser) -> BlobStoreIndexShardSnapshot.fromXContent(parser)
    );

    public static final ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshots> INDEX_SHARD_SNAPSHOTS_FORMAT = new ChecksumBlobStoreFormat<>(
        "snapshots",
        SNAPSHOT_INDEX_NAME_FORMAT,
        (repoName, parser) -> BlobStoreIndexShardSnapshots.fromXContent(parser)
    );

    public static final Setting<ByteSizeValue> MAX_SNAPSHOT_BYTES_PER_SEC = Setting.byteSizeSetting(
        "max_snapshot_bytes_per_sec",
        new ByteSizeValue(40, ByteSizeUnit.MB),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<ByteSizeValue> MAX_RESTORE_BYTES_PER_SEC = Setting.byteSizeSetting(
        "max_restore_bytes_per_sec",
        ByteSizeValue.ZERO,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Repository settings that can be updated dynamically without having to create a new repository.
     */
    private static final Set<String> DYNAMIC_SETTING_NAMES = Set.of(
        MAX_SNAPSHOT_BYTES_PER_SEC.getKey(),
        MAX_RESTORE_BYTES_PER_SEC.getKey()
    );

    private final boolean readOnly;

    private final Object lock = new Object();

    private final SetOnce<BlobContainer> blobContainer = new SetOnce<>();

    private final SetOnce<BlobStore> blobStore = new SetOnce<>();

    private final BlobPath basePath;

    private final ClusterService clusterService;

    private final RecoverySettings recoverySettings;

    private final NamedXContentRegistry namedXContentRegistry;

    protected final BigArrays bigArrays;

    /**
     * Flag that is set to {@code true} if this instance is started with {@link #metadata} that has a higher value for
     * {@link RepositoryMetadata#pendingGeneration()} than for {@link RepositoryMetadata#generation()} indicating a full cluster restart
     * potentially accounting for the the last {@code index-N} write in the cluster state.
     * Note: While it is true that this value could also be set to {@code true} for an instance on a node that is just joining the cluster
     * during a new {@code index-N} write, this does not present a problem. The node will still load the correct {@link RepositoryData} in
     * all cases and simply do a redundant listing of the repository contents if it tries to load {@link RepositoryData} and falls back
     * to {@link #latestIndexBlobId()} to validate the value of {@link RepositoryMetadata#generation()}.
     */
    private boolean uncleanStart;

    /**
     * This flag indicates that the repository can not exclusively rely on the value stored in {@link #latestKnownRepoGen} to determine the
     * latest repository generation but must inspect its physical contents as well via {@link #latestIndexBlobId()}.
     * This flag is set in the following situations:
     * <ul>
     *     <li>All repositories that are read-only, i.e. for which {@link #isReadOnly()} returns {@code true} because there are no
     *     guarantees that another cluster is not writing to the repository at the same time</li>
     *     <li>The value of {@link RepositoryMetadata#generation()} for this repository is {@link RepositoryData#UNKNOWN_REPO_GEN}
     *     indicating that no consistent repository generation is tracked in the cluster state yet.</li>
     *     <li>The {@link #uncleanStart} flag is set to {@code true}</li>
     * </ul>
     */
    private volatile boolean bestEffortConsistency;

    /**
     * IO buffer size hint for reading and writing to the underlying blob store.
     */
    protected final int bufferSize;

    /**
     * Maximum number of snapshots that this repository can hold.
     */
    private final int maxSnapshotCount;

    /**
     * Constructs new BlobStoreRepository
     * @param metadata   The metadata for this repository including name and settings
     * @param clusterService ClusterService
     */
    protected BlobStoreRepository(
        final RepositoryMetadata metadata,
        final NamedXContentRegistry namedXContentRegistry,
        final ClusterService clusterService,
        final BigArrays bigArrays,
        final RecoverySettings recoverySettings,
        final BlobPath basePath
    ) {
        this.metadata = metadata;
        this.threadPool = clusterService.getClusterApplierService().threadPool();
        this.clusterService = clusterService;
        this.bigArrays = bigArrays;
        this.recoverySettings = recoverySettings;
        this.compress = COMPRESS_SETTING.get(metadata.settings());
        this.supportURLRepo = SUPPORT_URL_REPO.get(metadata.settings());
        snapshotRateLimiter = getRateLimiter(metadata.settings(), MAX_SNAPSHOT_BYTES_PER_SEC);
        restoreRateLimiter = getRateLimiter(metadata.settings(), MAX_RESTORE_BYTES_PER_SEC);
        readOnly = metadata.settings().getAsBoolean(READONLY_SETTING_KEY, false);
        cacheRepositoryData = CACHE_REPOSITORY_DATA.get(metadata.settings());
        bufferSize = Math.toIntExact(BUFFER_SIZE_SETTING.get(metadata.settings()).getBytes());
        this.namedXContentRegistry = namedXContentRegistry;
        this.basePath = basePath;
        this.maxSnapshotCount = MAX_SNAPSHOTS_SETTING.get(metadata.settings());
    }

    @Override
    protected void doStart() {
        uncleanStart = metadata.pendingGeneration() > RepositoryData.EMPTY_REPO_GEN
            && metadata.generation() != metadata.pendingGeneration();
        ByteSizeValue chunkSize = chunkSize();
        if (chunkSize != null && chunkSize.getBytes() <= 0) {
            throw new IllegalArgumentException("the chunk size cannot be negative: [" + chunkSize + "]");
        }
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {
        BlobStore store;
        // to close blobStore if blobStore initialization is started during close
        synchronized (lock) {
            store = blobStore.get();
        }
        if (store != null) {
            try {
                store.close();
            } catch (Exception t) {
                logger.warn("cannot close blob store", t);
            }
        }
    }

    @Override
    public void executeConsistentStateUpdate(
        Function<RepositoryData, ClusterStateUpdateTask> createUpdateTask,
        String source,
        Consumer<Exception> onFailure
    ) {
        final RepositoryMetadata repositoryMetadataStart = metadata;
        getRepositoryData(ActionListener.wrap(repositoryData -> {
            final ClusterStateUpdateTask updateTask = createUpdateTask.apply(repositoryData);
            clusterService.submitStateUpdateTask(source, new ClusterStateUpdateTask(updateTask.priority(), updateTask.timeout()) {

                private boolean executedTask = false;

                @Override
                public ClusterState execute(ClusterState currentState) throws Exception {
                    // Comparing the full metadata here on purpose instead of simply comparing the safe generation.
                    // If the safe generation has changed, then we have to reload repository data and start over.
                    // If the pending generation has changed we are in the midst of a write operation and might pick up the
                    // updated repository data and state on the retry. We don't want to wait for the write to finish though
                    // because it could fail for any number of reasons so we just retry instead of waiting on the cluster state
                    // to change in any form.
                    if (repositoryMetadataStart.equals(getRepoMetadata(currentState))) {
                        executedTask = true;
                        return updateTask.execute(currentState);
                    }
                    return currentState;
                }

                @Override
                public void onFailure(String source, Exception e) {
                    if (executedTask) {
                        updateTask.onFailure(source, e);
                    } else {
                        onFailure.accept(e);
                    }
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    if (executedTask) {
                        updateTask.clusterStateProcessed(source, oldState, newState);
                    } else {
                        executeConsistentStateUpdate(createUpdateTask, source, onFailure);
                    }
                }
            });
        }, onFailure));
    }

    @Override
    public void cloneShardSnapshot(
        SnapshotId source,
        SnapshotId target,
        RepositoryShardId shardId,
        @Nullable ShardGeneration shardGeneration,
        ActionListener<ShardSnapshotResult> listener
    ) {
        if (isReadOnly()) {
            listener.onFailure(new RepositoryException(metadata.name(), "cannot clone shard snapshot on a readonly repository"));
            return;
        }
        final IndexId index = shardId.index();
        final int shardNum = shardId.shardId();
        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        executor.execute(ActionRunnable.supply(listener, () -> {
            final long startTime = threadPool.absoluteTimeInMillis();
            final BlobContainer shardContainer = shardContainer(index, shardNum);
            final BlobStoreIndexShardSnapshots existingSnapshots;
            final ShardGeneration newGen;
            final ShardGeneration existingShardGen;
            if (shardGeneration == null) {
                Tuple<BlobStoreIndexShardSnapshots, Long> tuple = buildBlobStoreIndexShardSnapshots(
                    shardContainer.listBlobsByPrefix(INDEX_FILE_PREFIX).keySet(),
                    shardContainer
                );
                existingShardGen = new ShardGeneration(tuple.v2());
                newGen = new ShardGeneration(tuple.v2() + 1);
                existingSnapshots = tuple.v1();
            } else {
                newGen = ShardGeneration.newGeneration();
                existingSnapshots = buildBlobStoreIndexShardSnapshots(Collections.emptySet(), shardContainer, shardGeneration).v1();
                existingShardGen = shardGeneration;
            }
            SnapshotFiles existingTargetFiles = null;
            SnapshotFiles sourceFiles = null;
            for (SnapshotFiles existingSnapshot : existingSnapshots) {
                final String snapshotName = existingSnapshot.snapshot();
                if (snapshotName.equals(target.getName())) {
                    existingTargetFiles = existingSnapshot;
                } else if (snapshotName.equals(source.getName())) {
                    sourceFiles = existingSnapshot;
                }
                if (sourceFiles != null && existingTargetFiles != null) {
                    break;
                }
            }
            if (sourceFiles == null) {
                throw new RepositoryException(
                    metadata.name(),
                    "Can't create clone of ["
                        + shardId
                        + "] for snapshot ["
                        + target
                        + "]. The source snapshot ["
                        + source
                        + "] was not found in the shard metadata."
                );
            }
            if (existingTargetFiles != null) {
                if (existingTargetFiles.isSame(sourceFiles)) {
                    return new ShardSnapshotResult(
                        existingShardGen,
                        ByteSizeValue.ofBytes(existingTargetFiles.totalSize()),
                        getSegmentInfoFileCount(existingTargetFiles.indexFiles())
                    );
                }
                throw new RepositoryException(
                    metadata.name(),
                    "Can't create clone of ["
                        + shardId
                        + "] for snapshot ["
                        + target
                        + "]. A snapshot by that name already exists for this shard."
                );
            }
            final BlobStoreIndexShardSnapshot sourceMeta = loadShardSnapshot(shardContainer, source);
            logger.trace("[{}] [{}] writing shard snapshot file for clone", shardId, target);
            INDEX_SHARD_SNAPSHOT_FORMAT.write(
                sourceMeta.asClone(target.getName(), startTime, threadPool.absoluteTimeInMillis() - startTime),
                shardContainer,
                target.getUUID(),
                compress
            );
            INDEX_SHARD_SNAPSHOTS_FORMAT.write(
                existingSnapshots.withClone(source.getName(), target.getName()),
                shardContainer,
                newGen.toBlobNamePart(),
                compress
            );
            return new ShardSnapshotResult(
                newGen,
                ByteSizeValue.ofBytes(sourceMeta.totalSize()),
                getSegmentInfoFileCount(sourceMeta.indexFiles())
            );
        }));
    }

    private static int getSegmentInfoFileCount(List<BlobStoreIndexShardSnapshot.FileInfo> indexFiles) {
        // noinspection ConstantConditions
        return Math.toIntExact(Math.min(Integer.MAX_VALUE, indexFiles.stream().filter(fi -> fi.physicalName().endsWith(".si")).count()));
    }

    @Override
    public boolean canUpdateInPlace(Settings updatedSettings, Set<String> ignoredSettings) {
        final Settings current = metadata.settings();
        if (current.equals(updatedSettings)) {
            return true;
        }
        final Set<String> changedSettingNames = new HashSet<>(current.keySet());
        changedSettingNames.addAll(updatedSettings.keySet());
        changedSettingNames.removeAll(ignoredSettings);
        changedSettingNames.removeIf(setting -> Objects.equals(current.get(setting), updatedSettings.get(setting)));
        changedSettingNames.removeAll(DYNAMIC_SETTING_NAMES);
        return changedSettingNames.isEmpty();
    }

    // Inspects all cluster state elements that contain a hint about what the current repository generation is and updates
    // #latestKnownRepoGen if a newer than currently known generation is found
    @Override
    public void updateState(ClusterState state) {
        final Settings previousSettings = metadata.settings();
        metadata = getRepoMetadata(state);
        final Settings updatedSettings = metadata.settings();
        if (updatedSettings.equals(previousSettings) == false) {
            snapshotRateLimiter = getRateLimiter(metadata.settings(), MAX_SNAPSHOT_BYTES_PER_SEC);
            restoreRateLimiter = getRateLimiter(metadata.settings(), MAX_RESTORE_BYTES_PER_SEC);
        }

        uncleanStart = uncleanStart && metadata.generation() != metadata.pendingGeneration();
        final boolean wasBestEffortConsistency = bestEffortConsistency;
        bestEffortConsistency = uncleanStart || isReadOnly() || metadata.generation() == RepositoryData.UNKNOWN_REPO_GEN;
        if (isReadOnly()) {
            // No need to waste cycles, no operations can run against a read-only repository
            return;
        }
        if (bestEffortConsistency) {
            final SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY);
            long bestGenerationFromCS = bestGeneration(snapshotsInProgress.entries());
            // Don't use generation from the delete task if we already found a generation for an in progress snapshot.
            // In this case, the generation points at the generation the repo will be in after the snapshot finishes so it may not yet
            // exist
            if (bestGenerationFromCS == RepositoryData.EMPTY_REPO_GEN) {
                bestGenerationFromCS = bestGeneration(
                    state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY).getEntries()
                );
            }
            if (bestGenerationFromCS == RepositoryData.EMPTY_REPO_GEN) {
                bestGenerationFromCS = bestGeneration(
                    state.custom(RepositoryCleanupInProgress.TYPE, RepositoryCleanupInProgress.EMPTY).entries()
                );
            }
            final long finalBestGen = Math.max(bestGenerationFromCS, metadata.generation());
            latestKnownRepoGen.updateAndGet(known -> Math.max(known, finalBestGen));
        } else {
            final long previousBest = latestKnownRepoGen.getAndSet(metadata.generation());
            if (previousBest != metadata.generation()) {
                assert wasBestEffortConsistency
                    || metadata.generation() == RepositoryData.CORRUPTED_REPO_GEN
                    || previousBest < metadata.generation()
                    : "Illegal move from repository generation [" + previousBest + "] to generation [" + metadata.generation() + "]";
                logger.debug("Updated repository generation from [{}] to [{}]", previousBest, metadata.generation());
            }
        }
    }

    private long bestGeneration(Collection<? extends RepositoryOperation> operations) {
        final String repoName = metadata.name();
        return operations.stream()
            .filter(e -> e.repository().equals(repoName))
            .mapToLong(RepositoryOperation::repositoryStateId)
            .max()
            .orElse(RepositoryData.EMPTY_REPO_GEN);
    }

    public ThreadPool threadPool() {
        return threadPool;
    }

    // package private, only use for testing
    BlobContainer getBlobContainer() {
        return blobContainer.get();
    }

    // for test purposes only
    protected BlobStore getBlobStore() {
        return blobStore.get();
    }

    /**
     * maintains single lazy instance of {@link BlobContainer}
     */
    protected BlobContainer blobContainer() {
        assertSnapshotOrGenericThread();

        if (lifecycle.started() == false) {
            throw notStartedException();
        }

        BlobContainer blobContainer = this.blobContainer.get();
        if (blobContainer == null) {
            synchronized (lock) {
                blobContainer = this.blobContainer.get();
                if (blobContainer == null) {
                    blobContainer = blobStore().blobContainer(basePath());
                    this.blobContainer.set(blobContainer);
                }
            }
        }

        return blobContainer;
    }

    /**
     * Maintains single lazy instance of {@link BlobStore}.
     * Public for testing.
     */
    public BlobStore blobStore() {
        assertSnapshotOrGenericThread();

        BlobStore store = blobStore.get();
        if (store == null) {
            synchronized (lock) {
                store = blobStore.get();
                if (store == null) {
                    if (lifecycle.started() == false) {
                        throw notStartedException();
                    }
                    try {
                        store = createBlobStore();
                    } catch (RepositoryException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new RepositoryException(metadata.name(), "cannot create blob store", e);
                    }
                    blobStore.set(store);
                }
            }
        }
        return store;
    }

    /**
     * Creates new BlobStore to read and write data.
     */
    protected abstract BlobStore createBlobStore() throws Exception;

    /**
     * Returns base path of the repository
     * Public for testing.
     */
    public BlobPath basePath() {
        return basePath;
    }

    /**
     * Returns true if metadata and snapshot files should be compressed
     *
     * @return true if compression is needed
     */
    protected final boolean isCompress() {
        return compress;
    }

    /**
     * Returns data file chunk size.
     * <p>
     * This method should return null if no chunking is needed.
     *
     * @return chunk size
     */
    protected ByteSizeValue chunkSize() {
        return null;
    }

    @Override
    public RepositoryMetadata getMetadata() {
        return metadata;
    }

    @Override
    public RepositoryStats stats() {
        final BlobStore store = blobStore.get();
        if (store == null) {
            return RepositoryStats.EMPTY_STATS;
        }
        return new RepositoryStats(store.stats());
    }

    @Override
    public void deleteSnapshots(
        Collection<SnapshotId> snapshotIds,
        long repositoryStateId,
        Version repositoryMetaVersion,
        ActionListener<RepositoryData> listener
    ) {
        if (isReadOnly()) {
            listener.onFailure(new RepositoryException(metadata.name(), "cannot delete snapshot from a readonly repository"));
        } else {
            threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    final Map<String, BlobMetadata> rootBlobs = blobContainer().listBlobs();
                    final RepositoryData repositoryData = safeRepositoryData(repositoryStateId, rootBlobs);
                    // Cache the indices that were found before writing out the new index-N blob so that a stuck master will never
                    // delete an index that was created by another master node after writing this index-N blob.
                    final Map<String, BlobContainer> foundIndices = blobStore().blobContainer(indicesPath()).children();
                    doDeleteShardSnapshots(
                        snapshotIds,
                        repositoryStateId,
                        foundIndices,
                        rootBlobs,
                        repositoryData,
                        repositoryMetaVersion,
                        listener
                    );
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(new RepositoryException(metadata.name(), "failed to delete snapshots " + snapshotIds, e));
                }
            });
        }
    }

    /**
     * Loads {@link RepositoryData} ensuring that it is consistent with the given {@code rootBlobs} as well of the assumed generation.
     *
     * @param repositoryStateId Expected repository generation
     * @param rootBlobs         Blobs at the repository root
     * @return RepositoryData
     */
    private RepositoryData safeRepositoryData(long repositoryStateId, Map<String, BlobMetadata> rootBlobs) {
        final long generation = latestGeneration(rootBlobs.keySet());
        final long genToLoad;
        final RepositoryData cached;
        if (bestEffortConsistency) {
            genToLoad = latestKnownRepoGen.updateAndGet(known -> Math.max(known, repositoryStateId));
            cached = null;
        } else {
            genToLoad = latestKnownRepoGen.get();
            cached = latestKnownRepositoryData.get();
        }
        if (genToLoad > generation) {
            // It's always a possibility to not see the latest index-N in the listing here on an eventually consistent blob store, just
            // debug log it. Any blobs leaked as a result of an inconsistent listing here will be cleaned up in a subsequent cleanup or
            // snapshot delete run anyway.
            logger.debug(
                "Determined repository's generation from its contents to ["
                    + generation
                    + "] but "
                    + "current generation is at least ["
                    + genToLoad
                    + "]"
            );
        }
        if (genToLoad != repositoryStateId) {
            throw new RepositoryException(
                metadata.name(),
                "concurrent modification of the index-N file, expected current generation ["
                    + repositoryStateId
                    + "], actual current generation ["
                    + genToLoad
                    + "]"
            );
        }
        if (cached != null && cached.getGenId() == genToLoad) {
            return cached;
        }
        return getRepositoryData(genToLoad);
    }

    /**
     * After updating the {@link RepositoryData} each of the shards directories is individually first moved to the next shard generation
     * and then has all now unreferenced blobs in it deleted.
     *
     * @param snapshotIds       SnapshotIds to delete
     * @param repositoryStateId Expected repository state id
     * @param foundIndices      All indices folders found in the repository before executing any writes to the repository during this
     *                          delete operation
     * @param rootBlobs         All blobs found at the root of the repository before executing any writes to the repository during this
     *                          delete operation
     * @param repositoryData    RepositoryData found the in the repository before executing this delete
     * @param listener          Listener to invoke once finished
     */
    private void doDeleteShardSnapshots(
        Collection<SnapshotId> snapshotIds,
        long repositoryStateId,
        Map<String, BlobContainer> foundIndices,
        Map<String, BlobMetadata> rootBlobs,
        RepositoryData repositoryData,
        Version repoMetaVersion,
        ActionListener<RepositoryData> listener
    ) {

        if (SnapshotsService.useShardGenerations(repoMetaVersion)) {
            // First write the new shard state metadata (with the removed snapshot) and compute deletion targets
            final StepListener<Collection<ShardSnapshotMetaDeleteResult>> writeShardMetaDataAndComputeDeletesStep = new StepListener<>();
            writeUpdatedShardMetaDataAndComputeDeletes(snapshotIds, repositoryData, true, writeShardMetaDataAndComputeDeletesStep);
            // Once we have put the new shard-level metadata into place, we can update the repository metadata as follows:
            // 1. Remove the snapshots from the list of existing snapshots
            // 2. Update the index shard generations of all updated shard folders
            //
            // Note: If we fail updating any of the individual shard paths, none of them are changed since the newly created
            // index-${gen_uuid} will not be referenced by the existing RepositoryData and new RepositoryData is only
            // written if all shard paths have been successfully updated.
            final StepListener<RepositoryData> writeUpdatedRepoDataStep = new StepListener<>();
            writeShardMetaDataAndComputeDeletesStep.whenComplete(deleteResults -> {
                final ShardGenerations.Builder builder = ShardGenerations.builder();
                for (ShardSnapshotMetaDeleteResult newGen : deleteResults) {
                    builder.put(newGen.indexId, newGen.shardId, newGen.newGeneration);
                }
                final RepositoryData updatedRepoData = repositoryData.removeSnapshots(snapshotIds, builder.build());
                writeIndexGen(
                    updatedRepoData,
                    repositoryStateId,
                    repoMetaVersion,
                    Function.identity(),
                    ActionListener.wrap(writeUpdatedRepoDataStep::onResponse, listener::onFailure)
                );
            }, listener::onFailure);
            // Once we have updated the repository, run the clean-ups
            writeUpdatedRepoDataStep.whenComplete(updatedRepoData -> {
                // Run unreferenced blobs cleanup in parallel to shard-level snapshot deletion
                final ActionListener<Void> afterCleanupsListener = new GroupedActionListener<>(
                    ActionListener.wrap(() -> listener.onResponse(updatedRepoData)),
                    2
                );
                cleanupUnlinkedRootAndIndicesBlobs(snapshotIds, foundIndices, rootBlobs, updatedRepoData, afterCleanupsListener);
                asyncCleanupUnlinkedShardLevelBlobs(
                    repositoryData,
                    snapshotIds,
                    writeShardMetaDataAndComputeDeletesStep.result(),
                    afterCleanupsListener
                );
            }, listener::onFailure);
        } else {
            // Write the new repository data first (with the removed snapshot), using no shard generations
            final RepositoryData updatedRepoData = repositoryData.removeSnapshots(snapshotIds, ShardGenerations.EMPTY);
            writeIndexGen(updatedRepoData, repositoryStateId, repoMetaVersion, Function.identity(), ActionListener.wrap(newRepoData -> {
                // Run unreferenced blobs cleanup in parallel to shard-level snapshot deletion
                final ActionListener<Void> afterCleanupsListener = new GroupedActionListener<>(
                    ActionListener.wrap(() -> listener.onResponse(newRepoData)),
                    2
                );
                cleanupUnlinkedRootAndIndicesBlobs(snapshotIds, foundIndices, rootBlobs, newRepoData, afterCleanupsListener);
                final StepListener<Collection<ShardSnapshotMetaDeleteResult>> writeMetaAndComputeDeletesStep = new StepListener<>();
                writeUpdatedShardMetaDataAndComputeDeletes(snapshotIds, repositoryData, false, writeMetaAndComputeDeletesStep);
                writeMetaAndComputeDeletesStep.whenComplete(
                    deleteResults -> asyncCleanupUnlinkedShardLevelBlobs(repositoryData, snapshotIds, deleteResults, afterCleanupsListener),
                    afterCleanupsListener::onFailure
                );
            }, listener::onFailure));
        }
    }

    private void cleanupUnlinkedRootAndIndicesBlobs(
        Collection<SnapshotId> deletedSnapshots,
        Map<String, BlobContainer> foundIndices,
        Map<String, BlobMetadata> rootBlobs,
        RepositoryData updatedRepoData,
        ActionListener<Void> listener
    ) {
        cleanupStaleBlobs(deletedSnapshots, foundIndices, rootBlobs, updatedRepoData, listener.map(ignored -> null));
    }

    private void asyncCleanupUnlinkedShardLevelBlobs(
        RepositoryData oldRepositoryData,
        Collection<SnapshotId> snapshotIds,
        Collection<ShardSnapshotMetaDeleteResult> deleteResults,
        ActionListener<Void> listener
    ) {
        final Iterator<String> filesToDelete = resolveFilesToDelete(oldRepositoryData, snapshotIds, deleteResults);
        if (filesToDelete.hasNext() == false) {
            listener.onResponse(null);
            return;
        }
        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.wrap(listener, l -> {
            try {
                deleteFromContainer(blobContainer(), filesToDelete);
                l.onResponse(null);
            } catch (Exception e) {
                logger.warn(() -> new ParameterizedMessage("{} Failed to delete some blobs during snapshot delete", snapshotIds), e);
                throw e;
            }
        }));
    }

    // updates the shard state metadata for shards of a snapshot that is to be deleted. Also computes the files to be cleaned up.
    private void writeUpdatedShardMetaDataAndComputeDeletes(
        Collection<SnapshotId> snapshotIds,
        RepositoryData oldRepositoryData,
        boolean useUUIDs,
        ActionListener<Collection<ShardSnapshotMetaDeleteResult>> onAllShardsCompleted
    ) {

        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        final List<IndexId> indices = oldRepositoryData.indicesToUpdateAfterRemovingSnapshot(snapshotIds);

        if (indices.isEmpty()) {
            onAllShardsCompleted.onResponse(Collections.emptyList());
            return;
        }

        // Listener that flattens out the delete results for each index
        final ActionListener<Collection<ShardSnapshotMetaDeleteResult>> deleteIndexMetadataListener = new GroupedActionListener<>(
            onAllShardsCompleted.map(res -> res.stream().flatMap(Collection::stream).collect(Collectors.toList())),
            indices.size()
        );

        for (IndexId indexId : indices) {
            final Set<SnapshotId> snapshotsWithIndex = Set.copyOf(oldRepositoryData.getSnapshots(indexId));
            final Set<SnapshotId> survivingSnapshots = snapshotsWithIndex.stream()
                .filter(id -> snapshotIds.contains(id) == false)
                .collect(Collectors.toSet());
            final StepListener<Collection<Integer>> shardCountListener = new StepListener<>();
            final Collection<String> indexMetaGenerations = snapshotIds.stream()
                .filter(snapshotsWithIndex::contains)
                .map(id -> oldRepositoryData.indexMetaDataGenerations().indexMetaBlobId(id, indexId))
                .collect(Collectors.toSet());
            final ActionListener<Integer> allShardCountsListener = new GroupedActionListener<>(
                shardCountListener,
                indexMetaGenerations.size()
            );
            final BlobContainer indexContainer = indexContainer(indexId);
            for (String indexMetaGeneration : indexMetaGenerations) {
                executor.execute(ActionRunnable.supply(allShardCountsListener, () -> {
                    try {
                        return INDEX_METADATA_FORMAT.read(metadata.name(), indexContainer, indexMetaGeneration, namedXContentRegistry)
                            .getNumberOfShards();
                    } catch (Exception ex) {
                        logger.warn(
                            () -> new ParameterizedMessage(
                                "[{}] [{}] failed to read metadata for index",
                                indexMetaGeneration,
                                indexId.getName()
                            ),
                            ex
                        );
                        // Just invoke the listener without any shard generations to count it down, this index will be cleaned up
                        // by the stale data cleanup in the end.
                        // TODO: Getting here means repository corruption. We should find a way of dealing with this instead of just
                        // ignoring it and letting the cleanup deal with it.
                        return null;
                    }
                }));
            }
            shardCountListener.whenComplete(counts -> {
                final int shardCount = counts.stream().mapToInt(i -> i).max().orElse(0);
                if (shardCount == 0) {
                    deleteIndexMetadataListener.onResponse(null);
                    return;
                }
                // Listener for collecting the results of removing the snapshot from each shard's metadata in the current index
                final ActionListener<ShardSnapshotMetaDeleteResult> allShardsListener = new GroupedActionListener<>(
                    deleteIndexMetadataListener,
                    shardCount
                );
                for (int shardId = 0; shardId < shardCount; shardId++) {
                    final int finalShardId = shardId;
                    executor.execute(new AbstractRunnable() {
                        @Override
                        protected void doRun() throws Exception {
                            final BlobContainer shardContainer = shardContainer(indexId, finalShardId);
                            final Set<String> blobs = shardContainer.listBlobs().keySet();
                            final BlobStoreIndexShardSnapshots blobStoreIndexShardSnapshots;
                            final long newGen;
                            if (useUUIDs) {
                                newGen = -1L;
                                blobStoreIndexShardSnapshots = buildBlobStoreIndexShardSnapshots(
                                    blobs,
                                    shardContainer,
                                    oldRepositoryData.shardGenerations().getShardGen(indexId, finalShardId)
                                ).v1();
                            } else {
                                Tuple<BlobStoreIndexShardSnapshots, Long> tuple = buildBlobStoreIndexShardSnapshots(blobs, shardContainer);
                                newGen = tuple.v2() + 1;
                                blobStoreIndexShardSnapshots = tuple.v1();
                            }
                            allShardsListener.onResponse(
                                deleteFromShardSnapshotMeta(
                                    survivingSnapshots,
                                    indexId,
                                    finalShardId,
                                    snapshotIds,
                                    shardContainer,
                                    blobs,
                                    blobStoreIndexShardSnapshots,
                                    newGen
                                )
                            );
                        }

                        @Override
                        public void onFailure(Exception ex) {
                            logger.warn(
                                () -> new ParameterizedMessage(
                                    "{} failed to delete shard data for shard [{}][{}]",
                                    snapshotIds,
                                    indexId.getName(),
                                    finalShardId
                                ),
                                ex
                            );
                            // Just passing null here to count down the listener instead of failing it, the stale data left behind
                            // here will be retried in the next delete or repository cleanup
                            allShardsListener.onResponse(null);
                        }
                    });
                }
            }, deleteIndexMetadataListener::onFailure);
        }
    }

    private Iterator<String> resolveFilesToDelete(
        RepositoryData oldRepositoryData,
        Collection<SnapshotId> snapshotIds,
        Collection<ShardSnapshotMetaDeleteResult> deleteResults
    ) {
        final String basePath = basePath().buildAsString();
        final int basePathLen = basePath.length();
        final Map<IndexId, Collection<String>> indexMetaGenerations = oldRepositoryData.indexMetaDataToRemoveAfterRemovingSnapshots(
            snapshotIds
        );
        return Stream.concat(deleteResults.stream().flatMap(shardResult -> {
            final String shardPath = shardContainer(shardResult.indexId, shardResult.shardId).path().buildAsString();
            return shardResult.blobsToDelete.stream().map(blob -> shardPath + blob);
        }), indexMetaGenerations.entrySet().stream().flatMap(entry -> {
            final String indexContainerPath = indexContainer(entry.getKey()).path().buildAsString();
            return entry.getValue().stream().map(id -> indexContainerPath + INDEX_METADATA_FORMAT.blobName(id));
        })).map(absolutePath -> {
            assert absolutePath.startsWith(basePath);
            return absolutePath.substring(basePathLen);
        }).iterator();
    }

    /**
     * Cleans up stale blobs directly under the repository root as well as all indices paths that aren't referenced by any existing
     * snapshots. This method is only to be called directly after a new {@link RepositoryData} was written to the repository and with
     * parameters {@code foundIndices}, {@code rootBlobs}
     *
     * @param deletedSnapshots if this method is called as part of a delete operation, the snapshot ids just deleted or empty if called as
     *                         part of a repository cleanup
     * @param foundIndices     all indices blob containers found in the repository before {@code newRepoData} was written
     * @param rootBlobs        all blobs found directly under the repository root
     * @param newRepoData      new repository data that was just written
     * @param listener         listener to invoke with the combined {@link DeleteResult} of all blobs removed in this operation
     */
    private void cleanupStaleBlobs(
        Collection<SnapshotId> deletedSnapshots,
        Map<String, BlobContainer> foundIndices,
        Map<String, BlobMetadata> rootBlobs,
        RepositoryData newRepoData,
        ActionListener<DeleteResult> listener
    ) {
        final GroupedActionListener<DeleteResult> groupedListener = new GroupedActionListener<>(ActionListener.wrap(deleteResults -> {
            DeleteResult deleteResult = DeleteResult.ZERO;
            for (DeleteResult result : deleteResults) {
                deleteResult = deleteResult.add(result);
            }
            listener.onResponse(deleteResult);
        }, listener::onFailure), 2);

        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        final List<String> staleRootBlobs = staleRootBlobs(newRepoData, rootBlobs.keySet());
        if (staleRootBlobs.isEmpty()) {
            groupedListener.onResponse(DeleteResult.ZERO);
        } else {
            executor.execute(ActionRunnable.supply(groupedListener, () -> {
                List<String> deletedBlobs = cleanupStaleRootFiles(newRepoData.getGenId() - 1, deletedSnapshots, staleRootBlobs);
                return new DeleteResult(deletedBlobs.size(), deletedBlobs.stream().mapToLong(name -> rootBlobs.get(name).length()).sum());
            }));
        }

        final Set<String> survivingIndexIds = newRepoData.getIndices().values().stream().map(IndexId::getId).collect(Collectors.toSet());
        if (foundIndices.keySet().equals(survivingIndexIds)) {
            groupedListener.onResponse(DeleteResult.ZERO);
        } else {
            executor.execute(ActionRunnable.supply(groupedListener, () -> cleanupStaleIndices(foundIndices, survivingIndexIds)));
        }
    }

    /**
     * Runs cleanup actions on the repository. Increments the repository state id by one before executing any modifications on the
     * repository.
     * TODO: Add shard level cleanups
     * TODO: Add unreferenced index metadata cleanup
     * <ul>
     *     <li>Deleting stale indices {@link #cleanupStaleIndices}</li>
     *     <li>Deleting unreferenced root level blobs {@link #cleanupStaleRootFiles}</li>
     * </ul>
     * @param repositoryStateId     Current repository state id
     * @param repositoryMetaVersion version of the updated repository metadata to write
     * @param listener              Listener to complete when done
     */
    public void cleanup(long repositoryStateId, Version repositoryMetaVersion, ActionListener<RepositoryCleanupResult> listener) {
        try {
            if (isReadOnly()) {
                throw new RepositoryException(metadata.name(), "cannot run cleanup on readonly repository");
            }
            Map<String, BlobMetadata> rootBlobs = blobContainer().listBlobs();
            final RepositoryData repositoryData = safeRepositoryData(repositoryStateId, rootBlobs);
            final Map<String, BlobContainer> foundIndices = blobStore().blobContainer(indicesPath()).children();
            final Set<String> survivingIndexIds = repositoryData.getIndices()
                .values()
                .stream()
                .map(IndexId::getId)
                .collect(Collectors.toSet());
            final List<String> staleRootBlobs = staleRootBlobs(repositoryData, rootBlobs.keySet());
            if (survivingIndexIds.equals(foundIndices.keySet()) && staleRootBlobs.isEmpty()) {
                // Nothing to clean up we return
                listener.onResponse(new RepositoryCleanupResult(DeleteResult.ZERO));
            } else {
                // write new index-N blob to ensure concurrent operations will fail
                writeIndexGen(
                    repositoryData,
                    repositoryStateId,
                    repositoryMetaVersion,
                    Function.identity(),
                    ActionListener.wrap(
                        v -> cleanupStaleBlobs(
                            Collections.emptyList(),
                            foundIndices,
                            rootBlobs,
                            repositoryData,
                            listener.map(RepositoryCleanupResult::new)
                        ),
                        listener::onFailure
                    )
                );
            }
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    // Finds all blobs directly under the repository root path that are not referenced by the current RepositoryData
    private static List<String> staleRootBlobs(RepositoryData repositoryData, Set<String> rootBlobNames) {
        final Set<String> allSnapshotIds = repositoryData.getSnapshotIds().stream().map(SnapshotId::getUUID).collect(Collectors.toSet());
        return rootBlobNames.stream().filter(blob -> {
            if (FsBlobContainer.isTempBlobName(blob)) {
                return true;
            }
            if (blob.endsWith(".dat")) {
                final String foundUUID;
                if (blob.startsWith(SNAPSHOT_PREFIX)) {
                    foundUUID = blob.substring(SNAPSHOT_PREFIX.length(), blob.length() - ".dat".length());
                    assert SNAPSHOT_FORMAT.blobName(foundUUID).equals(blob);
                } else if (blob.startsWith(METADATA_PREFIX)) {
                    foundUUID = blob.substring(METADATA_PREFIX.length(), blob.length() - ".dat".length());
                    assert GLOBAL_METADATA_FORMAT.blobName(foundUUID).equals(blob);
                } else {
                    return false;
                }
                return allSnapshotIds.contains(foundUUID) == false;
            } else if (blob.startsWith(INDEX_FILE_PREFIX)) {
                // TODO: Include the current generation here once we remove keeping index-(N-1) around from #writeIndexGen
                return repositoryData.getGenId() > Long.parseLong(blob.substring(INDEX_FILE_PREFIX.length()));
            }
            return false;
        }).collect(Collectors.toList());
    }

    private List<String> cleanupStaleRootFiles(
        long previousGeneration,
        Collection<SnapshotId> deletedSnapshots,
        List<String> blobsToDelete
    ) {
        if (blobsToDelete.isEmpty()) {
            return blobsToDelete;
        }
        try {
            if (logger.isInfoEnabled()) {
                // If we're running root level cleanup as part of a snapshot delete we should not log the snapshot- and global metadata
                // blobs associated with the just deleted snapshots as they are expected to exist and not stale. Otherwise every snapshot
                // delete would also log a confusing INFO message about "stale blobs".
                final Set<String> blobNamesToIgnore = deletedSnapshots.stream()
                    .flatMap(
                        snapshotId -> Stream.of(
                            GLOBAL_METADATA_FORMAT.blobName(snapshotId.getUUID()),
                            SNAPSHOT_FORMAT.blobName(snapshotId.getUUID()),
                            INDEX_FILE_PREFIX + previousGeneration
                        )
                    )
                    .collect(Collectors.toSet());
                final List<String> blobsToLog = blobsToDelete.stream()
                    .filter(b -> blobNamesToIgnore.contains(b) == false)
                    .collect(Collectors.toList());
                if (blobsToLog.isEmpty() == false) {
                    logger.info("[{}] Found stale root level blobs {}. Cleaning them up", metadata.name(), blobsToLog);
                }
            }
            deleteFromContainer(blobContainer(), blobsToDelete.iterator());
            return blobsToDelete;
        } catch (Exception e) {
            logger.warn(
                () -> new ParameterizedMessage(
                    "[{}] The following blobs are no longer part of any snapshot [{}] but failed to remove them",
                    metadata.name(),
                    blobsToDelete
                ),
                e
            );
        }
        return Collections.emptyList();
    }

    private DeleteResult cleanupStaleIndices(Map<String, BlobContainer> foundIndices, Set<String> survivingIndexIds) {
        DeleteResult deleteResult = DeleteResult.ZERO;
        for (Map.Entry<String, BlobContainer> indexEntry : foundIndices.entrySet()) {
            final String indexSnId = indexEntry.getKey();
            try {
                if (survivingIndexIds.contains(indexSnId) == false) {
                    logger.debug("[{}] Found stale index [{}]. Cleaning it up", metadata.name(), indexSnId);
                    deleteResult = deleteResult.add(indexEntry.getValue().delete());
                    logger.debug("[{}] Cleaned up stale index [{}]", metadata.name(), indexSnId);
                }
            } catch (Exception e) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "[{}] index {} is no longer part of any snapshot in the repository, " + "but failed to clean up its index folder",
                        metadata.name(),
                        indexSnId
                    ),
                    e
                );
            }
        }
        return deleteResult;
    }

    @Override
    public void finalizeSnapshot(
        final ShardGenerations shardGenerations,
        final long repositoryStateId,
        final Metadata clusterMetadata,
        SnapshotInfo snapshotInfo,
        Version repositoryMetaVersion,
        Function<ClusterState, ClusterState> stateTransformer,
        final ActionListener<RepositoryData> listener
    ) {
        assert repositoryStateId > RepositoryData.UNKNOWN_REPO_GEN
            : "Must finalize based on a valid repository generation but received [" + repositoryStateId + "]";
        final Collection<IndexId> indices = shardGenerations.indices();
        final SnapshotId snapshotId = snapshotInfo.snapshotId();
        // Once we are done writing the updated index-N blob we remove the now unreferenced index-${uuid} blobs in each shard
        // directory if all nodes are at least at version SnapshotsService#SHARD_GEN_IN_REPO_DATA_VERSION
        // If there are older version nodes in the cluster, we don't need to run this cleanup as it will have already happened
        // when writing the index-${N} to each shard directory.
        final boolean writeShardGens = SnapshotsService.useShardGenerations(repositoryMetaVersion);
        final Consumer<Exception> onUpdateFailure = e -> listener.onFailure(
            new SnapshotException(metadata.name(), snapshotId, "failed to update snapshot in repository", e)
        );

        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);

        final boolean writeIndexGens = SnapshotsService.useIndexGenerations(repositoryMetaVersion);

        final StepListener<RepositoryData> repoDataListener = new StepListener<>();
        getRepositoryData(repoDataListener);
        repoDataListener.whenComplete(existingRepositoryData -> {
            final int existingSnapshotCount = existingRepositoryData.getSnapshotIds().size();
            if (existingSnapshotCount >= maxSnapshotCount) {
                listener.onFailure(
                    new RepositoryException(
                        metadata.name(),
                        "Cannot add another snapshot to this repository as it "
                            + "already contains ["
                            + existingSnapshotCount
                            + "] snapshots and is configured to hold up to ["
                            + maxSnapshotCount
                            + "] snapshots only."
                    )
                );
                return;
            }

            final Map<IndexId, String> indexMetas;
            final Map<String, String> indexMetaIdentifiers;
            if (writeIndexGens) {
                indexMetaIdentifiers = ConcurrentCollections.newConcurrentMap();
                indexMetas = ConcurrentCollections.newConcurrentMap();
            } else {
                indexMetas = null;
                indexMetaIdentifiers = null;
            }

            final ActionListener<Void> allMetaListener = new GroupedActionListener<>(ActionListener.wrap(v -> {
                final SnapshotDetails snapshotDetails = new SnapshotDetails(
                    snapshotInfo.state(),
                    Version.CURRENT,
                    snapshotInfo.startTime(),
                    snapshotInfo.endTime()
                );
                final RepositoryData updatedRepositoryData = existingRepositoryData.addSnapshot(
                    snapshotId,
                    snapshotDetails,
                    shardGenerations,
                    indexMetas,
                    indexMetaIdentifiers
                );
                writeIndexGen(
                    updatedRepositoryData,
                    repositoryStateId,
                    repositoryMetaVersion,
                    stateTransformer,
                    ActionListener.wrap(newRepoData -> {
                        if (writeShardGens) {
                            cleanupOldShardGens(existingRepositoryData, updatedRepositoryData);
                        }
                        listener.onResponse(newRepoData);
                    }, onUpdateFailure)
                );
            }, onUpdateFailure), 2 + indices.size());

            // We ignore all FileAlreadyExistsException when writing metadata since otherwise a master failover while in this method will
            // mean that no snap-${uuid}.dat blob is ever written for this snapshot. This is safe because any updated version of the
            // index or global metadata will be compatible with the segments written in this snapshot as well.
            // Failing on an already existing index-${repoGeneration} below ensures that the index.latest blob is not updated in a way
            // that decrements the generation it points at

            // Write Global MetaData
            executor.execute(
                ActionRunnable.run(
                    allMetaListener,
                    () -> GLOBAL_METADATA_FORMAT.write(clusterMetadata, blobContainer(), snapshotId.getUUID(), compress)
                )
            );

            // write the index metadata for each index in the snapshot
            for (IndexId index : indices) {
                executor.execute(ActionRunnable.run(allMetaListener, () -> {
                    final IndexMetadata indexMetaData = clusterMetadata.index(index.getName());
                    if (writeIndexGens) {
                        final String identifiers = IndexMetaDataGenerations.buildUniqueIdentifier(indexMetaData);
                        String metaUUID = existingRepositoryData.indexMetaDataGenerations().getIndexMetaBlobId(identifiers);
                        if (metaUUID == null) {
                            // We don't yet have this version of the metadata so we write it
                            metaUUID = UUIDs.base64UUID();
                            INDEX_METADATA_FORMAT.write(indexMetaData, indexContainer(index), metaUUID, compress);
                            indexMetaIdentifiers.put(identifiers, metaUUID);
                        }
                        indexMetas.put(index, identifiers);
                    } else {
                        INDEX_METADATA_FORMAT.write(
                            clusterMetadata.index(index.getName()),
                            indexContainer(index),
                            snapshotId.getUUID(),
                            compress
                        );
                    }
                }));
            }
            executor.execute(
                ActionRunnable.run(
                    allMetaListener,
                    () -> SNAPSHOT_FORMAT.write(snapshotInfo, blobContainer(), snapshotId.getUUID(), compress)
                )
            );
        }, onUpdateFailure);
    }

    // Delete all old shard gen blobs that aren't referenced any longer as a result from moving to updated repository data
    private void cleanupOldShardGens(RepositoryData existingRepositoryData, RepositoryData updatedRepositoryData) {
        final List<String> toDelete = new ArrayList<>();
        final int prefixPathLen = basePath().buildAsString().length();
        updatedRepositoryData.shardGenerations()
            .obsoleteShardGenerations(existingRepositoryData.shardGenerations())
            .forEach(
                (indexId, gens) -> gens.forEach(
                    (shardId, oldGen) -> toDelete.add(
                        shardContainer(indexId, shardId).path().buildAsString().substring(prefixPathLen) + INDEX_FILE_PREFIX + oldGen
                    )
                )
            );
        try {
            deleteFromContainer(blobContainer(), toDelete.iterator());
        } catch (Exception e) {
            logger.warn("Failed to clean up old shard generation blobs", e);
        }
    }

    @Override
    public void getSnapshotInfo(GetSnapshotInfoContext context) {
        // put snapshot info downloads into a task queue instead of pushing them all into the queue to not completely monopolize the
        // snapshot meta pool for a single request
        final int workers = Math.min(threadPool.info(ThreadPool.Names.SNAPSHOT_META).getMax(), context.snapshotIds().size());
        final BlockingQueue<SnapshotId> queue = new LinkedBlockingQueue<>(context.snapshotIds());
        for (int i = 0; i < workers; i++) {
            getOneSnapshotInfo(queue, context);
        }
    }

    /**
     * Tries to poll a {@link SnapshotId} to load {@link SnapshotInfo} for from the given {@code queue}.
     */
    private void getOneSnapshotInfo(BlockingQueue<SnapshotId> queue, GetSnapshotInfoContext context) {
        final SnapshotId snapshotId = queue.poll();
        if (snapshotId == null) {
            return;
        }
        threadPool.executor(ThreadPool.Names.SNAPSHOT_META).execute(() -> {
            if (context.done()) {
                return;
            }
            if (context.isCancelled()) {
                queue.clear();
                context.onFailure(new TaskCancelledException("task cancelled"));
                return;
            }
            Exception failure = null;
            SnapshotInfo snapshotInfo = null;
            try {
                snapshotInfo = SNAPSHOT_FORMAT.read(metadata.name(), blobContainer(), snapshotId.getUUID(), namedXContentRegistry);
            } catch (NoSuchFileException ex) {
                failure = new SnapshotMissingException(metadata.name(), snapshotId, ex);
            } catch (IOException | NotXContentException ex) {
                failure = new SnapshotException(metadata.name(), snapshotId, "failed to get snapshot info" + snapshotId, ex);
            } catch (Exception e) {
                failure = e instanceof SnapshotException
                    ? e
                    : new SnapshotException(metadata.name(), snapshotId, "Snapshot could not be read", e);
            }
            if (failure != null) {
                if (context.abortOnFailure()) {
                    queue.clear();
                }
                context.onFailure(failure);
            } else {
                assert snapshotInfo != null;
                context.onResponse(snapshotInfo);
            }
            getOneSnapshotInfo(queue, context);
        });
    }

    @Override
    public Metadata getSnapshotGlobalMetadata(final SnapshotId snapshotId) {
        try {
            return GLOBAL_METADATA_FORMAT.read(metadata.name(), blobContainer(), snapshotId.getUUID(), namedXContentRegistry);
        } catch (NoSuchFileException ex) {
            throw new SnapshotMissingException(metadata.name(), snapshotId, ex);
        } catch (IOException ex) {
            throw new SnapshotException(metadata.name(), snapshotId, "failed to read global metadata", ex);
        }
    }

    @Override
    public IndexMetadata getSnapshotIndexMetaData(RepositoryData repositoryData, SnapshotId snapshotId, IndexId index) throws IOException {
        try {
            return INDEX_METADATA_FORMAT.read(
                metadata.name(),
                indexContainer(index),
                repositoryData.indexMetaDataGenerations().indexMetaBlobId(snapshotId, index),
                namedXContentRegistry
            );
        } catch (NoSuchFileException e) {
            throw new SnapshotMissingException(metadata.name(), snapshotId, e);
        }
    }

    private void deleteFromContainer(BlobContainer container, Iterator<String> blobs) throws IOException {
        final Iterator<String> wrappedIterator;
        if (logger.isTraceEnabled()) {
            wrappedIterator = new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return blobs.hasNext();
                }

                @Override
                public String next() {
                    final String blobName = blobs.next();
                    logger.trace("[{}] Deleting [{}] from [{}]", metadata.name(), blobName, container.path());
                    return blobName;
                }
            };
        } else {
            wrappedIterator = blobs;
        }
        container.deleteBlobsIgnoringIfNotExists(wrappedIterator);
    }

    private BlobPath indicesPath() {
        return basePath().add("indices");
    }

    private BlobContainer indexContainer(IndexId indexId) {
        return blobStore().blobContainer(indicesPath().add(indexId.getId()));
    }

    private BlobContainer shardContainer(IndexId indexId, ShardId shardId) {
        return shardContainer(indexId, shardId.getId());
    }

    public BlobContainer shardContainer(IndexId indexId, int shardId) {
        return blobStore().blobContainer(indicesPath().add(indexId.getId()).add(Integer.toString(shardId)));
    }

    /**
     * Configures RateLimiter based on repository and global settings
     *
     * @param repositorySettings repository settings
     * @param setting            setting to use to configure rate limiter
     * @return rate limiter or null of no throttling is needed
     */
    private static RateLimiter getRateLimiter(Settings repositorySettings, Setting<ByteSizeValue> setting) {
        ByteSizeValue maxSnapshotBytesPerSec = setting.get(repositorySettings);
        if (maxSnapshotBytesPerSec.getBytes() <= 0) {
            return null;
        } else {
            return new RateLimiter.SimpleRateLimiter(maxSnapshotBytesPerSec.getMbFrac());
        }
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        return snapshotRateLimitingTimeInNanos.count();
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        return restoreRateLimitingTimeInNanos.count();
    }

    protected void assertSnapshotOrGenericThread() {
        assert Thread.currentThread().getName().contains('[' + ThreadPool.Names.SNAPSHOT + ']')
            || Thread.currentThread().getName().contains('[' + ThreadPool.Names.SNAPSHOT_META + ']')
            || Thread.currentThread().getName().contains('[' + ThreadPool.Names.GENERIC + ']')
            : "Expected current thread [" + Thread.currentThread() + "] to be the snapshot or generic thread.";
    }

    @Override
    public String startVerification() {
        try {
            if (isReadOnly()) {
                // It's readonly - so there is not much we can do here to verify it apart from reading the blob store metadata
                latestIndexBlobId();
                return "read-only";
            } else {
                String seed = UUIDs.randomBase64UUID();
                byte[] testBytes = Strings.toUTF8Bytes(seed);
                BlobContainer testContainer = blobStore().blobContainer(basePath().add(testBlobPrefix(seed)));
                testContainer.writeBlobAtomic("master.dat", new BytesArray(testBytes), true);
                return seed;
            }
        } catch (Exception exp) {
            throw new RepositoryVerificationException(metadata.name(), "path " + basePath() + " is not accessible on master node", exp);
        }
    }

    @Override
    public void endVerification(String seed) {
        if (isReadOnly() == false) {
            try {
                final String testPrefix = testBlobPrefix(seed);
                blobStore().blobContainer(basePath().add(testPrefix)).delete();
            } catch (Exception exp) {
                throw new RepositoryVerificationException(metadata.name(), "cannot delete test data at " + basePath(), exp);
            }
        }
    }

    // Tracks the latest known repository generation in a best-effort way to detect inconsistent listing of root level index-N blobs
    // and concurrent modifications.
    private final AtomicLong latestKnownRepoGen = new AtomicLong(RepositoryData.UNKNOWN_REPO_GEN);

    // Best effort cache of the latest known repository data
    private final AtomicReference<RepositoryData> latestKnownRepositoryData = new AtomicReference<>(RepositoryData.EMPTY);

    @Override
    public void getRepositoryData(ActionListener<RepositoryData> listener) {
        // RepositoryData is the responsibility of the elected master: we shouldn't be loading it on other nodes as we don't have good
        // consistency guarantees there, but electedness is too ephemeral to assert. We can say for sure that this node should be
        // master-eligible, which is almost as strong since all other snapshot-related activity happens on data nodes whether they be
        // master-eligible or not.
        assert clusterService.localNode().isMasterNode() : "should only load repository data on master nodes";

        if (lifecycle.started() == false) {
            listener.onFailure(notStartedException());
            return;
        }

        if (latestKnownRepoGen.get() == RepositoryData.CORRUPTED_REPO_GEN) {
            listener.onFailure(corruptedStateException(null, null));
            return;
        }
        final RepositoryData cached = latestKnownRepositoryData.get();
        // Fast path loading repository data directly from cache if we're in fully consistent mode and the cache matches up with
        // the latest known repository generation
        if (bestEffortConsistency == false && cached.getGenId() == latestKnownRepoGen.get()) {
            repoDataDeduplicator.executeOnce(metadata, listener, (metadata, l) -> l.onResponse(cached));
            return;
        }
        if (metadata.generation() == RepositoryData.UNKNOWN_REPO_GEN && isReadOnly() == false) {
            logger.debug(
                "[{}] loading repository metadata for the first time, trying to determine correct generation and to store "
                    + "it in the cluster state",
                metadata.name()
            );
            initializeRepoGenerationTracking(listener);
        } else {
            logger.trace(
                "[{}] loading un-cached repository data with best known repository generation [{}]",
                metadata.name(),
                latestKnownRepoGen
            );
            // Don't deduplicate repo data loading if we don't have strong consistency guarantees between the repo and the cluster state
            // Also, if we are not caching repository data (for tests) we assume that the contents of the repository data at a given
            // generation may change
            final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT_META);
            if (bestEffortConsistency || cacheRepositoryData == false) {
                executor.execute(ActionRunnable.wrap(listener, this::doGetRepositoryData));
            } else {
                repoDataDeduplicator.executeOnce(
                    metadata,
                    listener,
                    (metadata, l) -> executor.execute(ActionRunnable.wrap(l, this::doGetRepositoryData))
                );
            }
        }
    }

    private RepositoryException notStartedException() {
        return new RepositoryException(metadata.name(), "repository is not in started state");
    }

    // Listener used to ensure that repository data is only initialized once in the cluster state by #initializeRepoGenerationTracking
    private ListenableActionFuture<RepositoryData> repoDataInitialized;

    /**
     * Method used to set the current repository generation in the cluster state's {@link RepositoryMetadata} to the latest generation that
     * can be physically found in the repository before passing the latest {@link RepositoryData} to the given listener.
     * This ensures that operations using {@link #executeConsistentStateUpdate} right after mounting a fresh repository will have a
     * consistent view of the {@link RepositoryData} before any data has been written to the repository.
     *
     * @param listener listener to resolve with new repository data
     */
    private void initializeRepoGenerationTracking(ActionListener<RepositoryData> listener) {
        synchronized (this) {
            if (repoDataInitialized == null) {
                // double check the generation since we checked it outside the mutex in the caller and it could have changed by a
                // concurrent initialization of the repo metadata and just load repository normally in case we already finished the
                // initialization
                if (metadata.generation() != RepositoryData.UNKNOWN_REPO_GEN) {
                    getRepositoryData(listener);
                    return;
                }
                logger.trace("[{}] initializing repository generation in cluster state", metadata.name());
                repoDataInitialized = new ListenableActionFuture<>();
                repoDataInitialized.addListener(listener);
                final Consumer<Exception> onFailure = e -> {
                    logger.warn(
                        new ParameterizedMessage(
                            "[{}] Exception when initializing repository generation in cluster state",
                            metadata.name()
                        ),
                        e
                    );
                    final ActionListener<RepositoryData> existingListener;
                    synchronized (BlobStoreRepository.this) {
                        existingListener = repoDataInitialized;
                        repoDataInitialized = null;
                    }
                    existingListener.onFailure(e);
                };
                threadPool.generic()
                    .execute(
                        ActionRunnable.wrap(
                            ActionListener.wrap(
                                repoData -> clusterService.submitStateUpdateTask(
                                    "set initial safe repository generation [" + metadata.name() + "][" + repoData.getGenId() + "]",
                                    new ClusterStateUpdateTask() {
                                        @Override
                                        public ClusterState execute(ClusterState currentState) {
                                            RepositoryMetadata metadata = getRepoMetadata(currentState);
                                            // No update to the repository generation should have occurred concurrently in general except
                                            // for
                                            // extreme corner cases like failing over to an older version master node and back to the
                                            // current
                                            // node concurrently
                                            if (metadata.generation() != RepositoryData.UNKNOWN_REPO_GEN) {
                                                throw new RepositoryException(
                                                    metadata.name(),
                                                    "Found unexpected initialized repo metadata [" + metadata + "]"
                                                );
                                            }
                                            return ClusterState.builder(currentState)
                                                .metadata(
                                                    Metadata.builder(currentState.getMetadata())
                                                        .putCustom(
                                                            RepositoriesMetadata.TYPE,
                                                            currentState.metadata()
                                                                .<RepositoriesMetadata>custom(RepositoriesMetadata.TYPE)
                                                                .withUpdatedGeneration(
                                                                    metadata.name(),
                                                                    repoData.getGenId(),
                                                                    repoData.getGenId()
                                                                )
                                                        )
                                                )
                                                .build();
                                        }

                                        @Override
                                        public void onFailure(String source, Exception e) {
                                            onFailure.accept(e);
                                        }

                                        @Override
                                        public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                                            logger.trace(
                                                "[{}] initialized repository generation in cluster state to [{}]",
                                                metadata.name(),
                                                repoData.getGenId()
                                            );
                                            // Resolve listeners on generic pool since some callbacks for repository data do additional IO
                                            threadPool.generic().execute(() -> {
                                                final ActionListener<RepositoryData> existingListener;
                                                synchronized (BlobStoreRepository.this) {
                                                    existingListener = repoDataInitialized;
                                                    repoDataInitialized = null;
                                                }
                                                existingListener.onResponse(repoData);
                                                logger.trace(
                                                    "[{}] called listeners after initializing repository to generation [{}]",
                                                    metadata.name(),
                                                    repoData.getGenId()
                                                );
                                            });
                                        }
                                    }
                                ),
                                onFailure
                            ),
                            this::doGetRepositoryData
                        )
                    );
            } else {
                logger.trace(
                    "[{}] waiting for existing initialization of repository metadata generation in cluster state",
                    metadata.name()
                );
                repoDataInitialized.addListener(listener);
            }
        }
    }

    /**
     * {@link RepositoryData} loading deduplicator. This may only be used with consistent generation repositories, meaning
     * {@link #bestEffortConsistency} must be {@code false}, in which case we can assume that the {@link RepositoryData} loaded is
     * unique for a given value of {@link #metadata} at any point in time.
     */
    private final ResultDeduplicator<RepositoryMetadata, RepositoryData> repoDataDeduplicator = new ResultDeduplicator<>();

    private void doGetRepositoryData(ActionListener<RepositoryData> listener) {
        // Retry loading RepositoryData in a loop in case we run into concurrent modifications of the repository.
        // Keep track of the most recent generation we failed to load so we can break out of the loop if we fail to load the same
        // generation repeatedly.

        long lastFailedGeneration = RepositoryData.UNKNOWN_REPO_GEN;
        while (true) {
            final long genToLoad;
            if (bestEffortConsistency) {
                // We're only using #latestKnownRepoGen as a hint in this mode and listing repo contents as a secondary way of trying
                // to find a higher generation
                final long generation;
                try {
                    generation = latestIndexBlobId();
                } catch (Exception e) {
                    listener.onFailure(
                        new RepositoryException(metadata.name(), "Could not determine repository generation from root blobs", e)
                    );
                    return;
                }
                genToLoad = latestKnownRepoGen.updateAndGet(known -> Math.max(known, generation));
                if (genToLoad > generation) {
                    logger.info(
                        "Determined repository generation [{}] from repository contents but correct generation must be at " + "least [{}]",
                        generation,
                        genToLoad
                    );
                }
            } else {
                // We only rely on the generation tracked in #latestKnownRepoGen which is exclusively updated from the cluster state
                genToLoad = latestKnownRepoGen.get();
            }
            try {
                final RepositoryData cached = latestKnownRepositoryData.get();
                // Caching is not used with #bestEffortConsistency see docs on #cacheRepositoryData for details
                if (bestEffortConsistency == false && cached.getGenId() == genToLoad) {
                    listener.onResponse(cached);
                } else {
                    final RepositoryData loaded = getRepositoryData(genToLoad);
                    if (cached == null || cached.getGenId() < genToLoad) {
                        // We can cache in the most recent version here without regard to the actual repository metadata version since
                        // we're only caching the information that we just wrote and thus won't accidentally cache any information that
                        // isn't safe
                        cacheRepositoryData(loaded, Version.CURRENT);
                    }
                    if (loaded.getUuid().equals(metadata.uuid())) {
                        listener.onResponse(loaded);
                    } else {
                        // someone switched the repo contents out from under us
                        RepositoriesService.updateRepositoryUuidInMetadata(
                            clusterService,
                            metadata.name(),
                            loaded,
                            new ThreadedActionListener<>(logger, threadPool, ThreadPool.Names.GENERIC, listener.map(v -> loaded), false)
                        );
                    }
                }
                return;
            } catch (RepositoryException e) {
                // If the generation to load changed concurrently and we didn't just try loading the same generation before we retry
                if (genToLoad != latestKnownRepoGen.get() && genToLoad != lastFailedGeneration) {
                    lastFailedGeneration = genToLoad;
                    logger.warn(
                        "Failed to load repository data generation ["
                            + genToLoad
                            + "] because a concurrent operation moved the current generation to ["
                            + latestKnownRepoGen.get()
                            + "]",
                        e
                    );
                    continue;
                }
                if (bestEffortConsistency == false && ExceptionsHelper.unwrap(e, NoSuchFileException.class) != null) {
                    // We did not find the expected index-N even though the cluster state continues to point at the missing value
                    // of N so we mark this repository as corrupted.
                    Tuple<Long, String> previousWriterInformation = null;
                    try {
                        previousWriterInformation = readLastWriterInfo();
                    } catch (Exception ex) {
                        e.addSuppressed(ex);
                    }
                    final Tuple<Long, String> finalLastInfo = previousWriterInformation;
                    markRepoCorrupted(
                        genToLoad,
                        e,
                        ActionListener.wrap(v -> listener.onFailure(corruptedStateException(e, finalLastInfo)), listener::onFailure)
                    );
                } else {
                    listener.onFailure(e);
                }
                return;
            } catch (Exception e) {
                listener.onFailure(new RepositoryException(metadata.name(), "Unexpected exception when loading repository data", e));
                return;
            }
        }
    }

    /**
     * Cache repository data if repository data caching is enabled.
     *
     * @param repositoryData repository data to cache
     * @param version        repository metadata version used when writing the data to the repository
     */
    private void cacheRepositoryData(RepositoryData repositoryData, Version version) {
        if (cacheRepositoryData == false) {
            return;
        }
        final RepositoryData toCache;
        if (SnapshotsService.useShardGenerations(version)) {
            toCache = repositoryData;
        } else {
            // don't cache shard generations here as they may be unreliable
            toCache = repositoryData.withoutShardGenerations();
            assert repositoryData.indexMetaDataGenerations().equals(IndexMetaDataGenerations.EMPTY)
                : "repository data should not contain index generations at version ["
                    + version
                    + "] but saw ["
                    + repositoryData.indexMetaDataGenerations()
                    + "]";
        }
        assert toCache.getGenId() >= 0 : "No need to cache abstract generations but attempted to cache [" + toCache.getGenId() + "]";
        latestKnownRepositoryData.updateAndGet(known -> {
            if (known.getGenId() > toCache.getGenId()) {
                return known;
            }
            return toCache;
        });
    }

    private RepositoryException corruptedStateException(@Nullable Exception cause, @Nullable Tuple<Long, String> previousWriterInfo) {
        return new RepositoryException(
            metadata.name(),
            "Could not read repository data because the contents of the repository do not match its "
                + "expected state. This is likely the result of either concurrently modifying the contents of the "
                + "repository by a process other than this cluster or an issue with the repository's underlying storage. "
                + "The repository has been disabled to prevent corrupting its contents. To re-enable it "
                + "and continue using it please remove the repository from the cluster and add it again to make "
                + "the cluster recover the known state of the repository from its physical contents."
                + previousWriterMessage(previousWriterInfo),
            cause
        );
    }

    private String previousWriterMessage(@Nullable Tuple<Long, String> previousWriterInfo) {
        return previousWriterInfo == null
            ? ""
            : " The last cluster to write to this repository was ["
                + previousWriterInfo.v2()
                + "] at generation ["
                + previousWriterInfo.v1()
                + "].";
    }

    /**
     * Marks the repository as corrupted. This puts the repository in a state where its tracked value for
     * {@link RepositoryMetadata#pendingGeneration()} is unchanged while its value for {@link RepositoryMetadata#generation()} is set to
     * {@link RepositoryData#CORRUPTED_REPO_GEN}. In this state, the repository can not be used any longer and must be removed and
     * recreated after the problem that lead to it being marked as corrupted has been fixed.
     *
     * @param corruptedGeneration generation that failed to load because the index file was not found but that should have loaded
     * @param originalException   exception that lead to the failing to load the {@code index-N} blob
     * @param listener            listener to invoke once done
     */
    private void markRepoCorrupted(long corruptedGeneration, Exception originalException, ActionListener<Void> listener) {
        assert corruptedGeneration != RepositoryData.UNKNOWN_REPO_GEN;
        assert bestEffortConsistency == false;
        clusterService.submitStateUpdateTask(
            "mark repository corrupted [" + metadata.name() + "][" + corruptedGeneration + "]",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    final RepositoriesMetadata state = currentState.metadata().custom(RepositoriesMetadata.TYPE);
                    final RepositoryMetadata repoState = state.repository(metadata.name());
                    if (repoState.generation() != corruptedGeneration) {
                        throw new IllegalStateException(
                            "Tried to mark repo generation ["
                                + corruptedGeneration
                                + "] as corrupted but its state concurrently changed to ["
                                + repoState
                                + "]"
                        );
                    }
                    return ClusterState.builder(currentState)
                        .metadata(
                            Metadata.builder(currentState.metadata())
                                .putCustom(
                                    RepositoriesMetadata.TYPE,
                                    state.withUpdatedGeneration(
                                        metadata.name(),
                                        RepositoryData.CORRUPTED_REPO_GEN,
                                        repoState.pendingGeneration()
                                    )
                                )
                                .build()
                        )
                        .build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(
                        new RepositoryException(
                            metadata.name(),
                            "Failed marking repository state as corrupted",
                            ExceptionsHelper.useOrSuppress(e, originalException)
                        )
                    );
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    listener.onResponse(null);
                }
            }
        );
    }

    private RepositoryData getRepositoryData(long indexGen) {
        if (indexGen == RepositoryData.EMPTY_REPO_GEN) {
            return RepositoryData.EMPTY;
        }
        try {
            final String snapshotsIndexBlobName = INDEX_FILE_PREFIX + Long.toString(indexGen);

            // EMPTY is safe here because RepositoryData#fromXContent calls namedObject
            try (
                InputStream blob = blobContainer().readBlob(snapshotsIndexBlobName);
                XContentParser parser = XContentType.JSON.xContent()
                    .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, blob)
            ) {
                return RepositoryData.snapshotsFromXContent(parser, indexGen, true);
            }
        } catch (IOException ioe) {
            if (bestEffortConsistency) {
                // If we fail to load the generation we tracked in latestKnownRepoGen we reset it.
                // This is done as a fail-safe in case a user manually deletes the contents of the repository in which case subsequent
                // operations must start from the EMPTY_REPO_GEN again
                if (latestKnownRepoGen.compareAndSet(indexGen, RepositoryData.EMPTY_REPO_GEN)) {
                    logger.warn("Resetting repository generation tracker because we failed to read generation [" + indexGen + "]", ioe);
                }
            }
            throw new RepositoryException(metadata.name(), "could not read repository data from index blob", ioe);
        }
    }

    private static String testBlobPrefix(String seed) {
        return TESTS_FILE + seed;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    /**
     * Writing a new index generation is a three step process.
     * First, the {@link RepositoryMetadata} entry for this repository is set into a pending state by incrementing its
     * pending generation {@code P} while its safe generation {@code N} remains unchanged.
     * Second, the updated {@link RepositoryData} is written to generation {@code P + 1}.
     * Lastly, the {@link RepositoryMetadata} entry for this repository is updated to the new generation {@code P + 1} and thus
     * pending and safe generation are set to the same value marking the end of the update of the repository data.
     *
     * @param repositoryData RepositoryData to write
     * @param expectedGen    expected repository generation at the start of the operation
     * @param version        version of the repository metadata to write
     * @param stateFilter    filter for the last cluster state update executed by this method
     * @param listener       completion listener
     */
    protected void writeIndexGen(
        RepositoryData repositoryData,
        long expectedGen,
        Version version,
        Function<ClusterState, ClusterState> stateFilter,
        ActionListener<RepositoryData> listener
    ) {
        logger.trace("[{}] writing repository data on top of expected generation [{}]", metadata.name(), expectedGen);
        assert isReadOnly() == false; // can not write to a read only repository
        final long currentGen = repositoryData.getGenId();
        if (currentGen != expectedGen) {
            // the index file was updated by a concurrent operation, so we were operating on stale
            // repository data
            listener.onFailure(
                new RepositoryException(
                    metadata.name(),
                    "concurrent modification of the index-N file, expected current generation ["
                        + expectedGen
                        + "], actual current generation ["
                        + currentGen
                        + "]"
                )
            );
            return;
        }

        // Step 1: Set repository generation state to the next possible pending generation
        final StepListener<Long> setPendingStep = new StepListener<>();
        clusterService.submitStateUpdateTask(
            "set pending repository generation [" + metadata.name() + "][" + expectedGen + "]",
            new ClusterStateUpdateTask() {

                private long newGen;

                @Override
                public ClusterState execute(ClusterState currentState) {
                    final RepositoryMetadata meta = getRepoMetadata(currentState);
                    final String repoName = metadata.name();
                    final long genInState = meta.generation();
                    final boolean uninitializedMeta = meta.generation() == RepositoryData.UNKNOWN_REPO_GEN || bestEffortConsistency;
                    if (uninitializedMeta == false && meta.pendingGeneration() != genInState) {
                        logger.info(
                            "Trying to write new repository data over unfinished write, repo [{}] is at "
                                + "safe generation [{}] and pending generation [{}]",
                            meta.name(),
                            genInState,
                            meta.pendingGeneration()
                        );
                    }
                    assert expectedGen == RepositoryData.EMPTY_REPO_GEN || uninitializedMeta || expectedGen == meta.generation()
                        : "Expected non-empty generation [" + expectedGen + "] does not match generation tracked in [" + meta + "]";
                    // If we run into the empty repo generation for the expected gen, the repo is assumed to have been cleared of
                    // all contents by an external process so we reset the safe generation to the empty generation.
                    final long safeGeneration = expectedGen == RepositoryData.EMPTY_REPO_GEN
                        ? RepositoryData.EMPTY_REPO_GEN
                        : (uninitializedMeta ? expectedGen : genInState);
                    // Regardless of whether or not the safe generation has been reset, the pending generation always increments so that
                    // even if a repository has been manually cleared of all contents we will never reuse the same repository generation.
                    // This is motivated by the consistency behavior the S3 based blob repository implementation has to support which does
                    // not offer any consistency guarantees when it comes to overwriting the same blob name with different content.
                    final long nextPendingGen = metadata.pendingGeneration() + 1;
                    newGen = uninitializedMeta ? Math.max(expectedGen + 1, nextPendingGen) : nextPendingGen;
                    assert newGen > latestKnownRepoGen.get()
                        : "Attempted new generation ["
                            + newGen
                            + "] must be larger than latest known generation ["
                            + latestKnownRepoGen.get()
                            + "]";
                    return ClusterState.builder(currentState)
                        .metadata(
                            Metadata.builder(currentState.getMetadata())
                                .putCustom(
                                    RepositoriesMetadata.TYPE,
                                    currentState.metadata()
                                        .<RepositoriesMetadata>custom(RepositoriesMetadata.TYPE)
                                        .withUpdatedGeneration(repoName, safeGeneration, newGen)
                                )
                                .build()
                        )
                        .build();
                }

                @Override
                public void onFailure(String source, Exception e) {
                    listener.onFailure(
                        new RepositoryException(metadata.name(), "Failed to execute cluster state update [" + source + "]", e)
                    );
                }

                @Override
                public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                    logger.trace("[{}] successfully set pending repository generation to [{}]", metadata.name(), newGen);
                    setPendingStep.onResponse(newGen);
                }
            }
        );

        final StepListener<RepositoryData> filterRepositoryDataStep = new StepListener<>();

        // Step 2: Write new index-N blob to repository and update index.latest
        setPendingStep.whenComplete(newGen -> threadPool().executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.wrap(listener, l -> {
            // BwC logic: Load snapshot version information if any snapshot is missing details in RepositoryData so that the new
            // RepositoryData contains full details for every snapshot
            final List<SnapshotId> snapshotIdsWithMissingDetails = repositoryData.getSnapshotIds()
                .stream()
                .filter(repositoryData::hasMissingDetails)
                .collect(Collectors.toList());
            if (snapshotIdsWithMissingDetails.isEmpty() == false) {
                final Map<SnapshotId, SnapshotDetails> extraDetailsMap = new ConcurrentHashMap<>();
                getSnapshotInfo(
                    new GetSnapshotInfoContext(
                        snapshotIdsWithMissingDetails,
                        false,
                        () -> false,
                        (context, snapshotInfo) -> extraDetailsMap.put(
                            snapshotInfo.snapshotId(),
                            new SnapshotDetails(
                                snapshotInfo.state(),
                                snapshotInfo.version(),
                                snapshotInfo.startTime(),
                                snapshotInfo.endTime()
                            )
                        ),
                        ActionListener.runAfter(new ActionListener<>() {
                            @Override
                            public void onResponse(Void aVoid) {
                                logger.info(
                                    "Successfully loaded all snapshots' detailed information for {} from snapshot metadata",
                                    AllocationService.firstListElementsToCommaDelimitedString(
                                        snapshotIdsWithMissingDetails,
                                        SnapshotId::toString,
                                        logger.isDebugEnabled()
                                    )
                                );
                            }

                            @Override
                            public void onFailure(Exception e) {
                                logger.warn("Failure when trying to load missing details from snapshot metadata", e);
                            }
                        }, () -> filterRepositoryDataStep.onResponse(repositoryData.withExtraDetails(extraDetailsMap)))
                    )
                );
            } else {
                filterRepositoryDataStep.onResponse(repositoryData);
            }
        })), listener::onFailure);
        filterRepositoryDataStep.whenComplete(filteredRepositoryData -> {
            final long newGen = setPendingStep.result();
            final RepositoryData newRepositoryData = updateRepositoryData(filteredRepositoryData, version, newGen);
            if (latestKnownRepoGen.get() >= newGen) {
                throw new IllegalArgumentException(
                    "Tried writing generation ["
                        + newGen
                        + "] but repository is at least at generation ["
                        + latestKnownRepoGen.get()
                        + "] already"
                );
            }
            // write the index file
            if (ensureSafeGenerationExists(expectedGen, listener::onFailure) == false) {
                return;
            }
            final String indexBlob = INDEX_FILE_PREFIX + Long.toString(newGen);
            logger.debug("Repository [{}] writing new index generational blob [{}]", metadata.name(), indexBlob);
            writeAtomic(blobContainer(), indexBlob, out -> {
                try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder(Streams.noCloseStream(out))) {
                    newRepositoryData.snapshotsToXContent(xContentBuilder, version);
                }
            }, true);
            maybeWriteIndexLatest(newGen);

            // Step 3: Update CS to reflect new repository generation.
            clusterService.submitStateUpdateTask(
                "set safe repository generation [" + metadata.name() + "][" + newGen + "]",
                new ClusterStateUpdateTask() {
                    @Override
                    public ClusterState execute(ClusterState currentState) {
                        final RepositoryMetadata meta = getRepoMetadata(currentState);
                        if (meta.generation() != expectedGen) {
                            throw new IllegalStateException(
                                "Tried to update repo generation to [" + newGen + "] but saw unexpected generation in state [" + meta + "]"
                            );
                        }
                        if (meta.pendingGeneration() != newGen) {
                            throw new IllegalStateException(
                                "Tried to update from unexpected pending repo generation ["
                                    + meta.pendingGeneration()
                                    + "] after write to generation ["
                                    + newGen
                                    + "]"
                            );
                        }
                        final RepositoriesMetadata currentMetadata = currentState.metadata().custom(RepositoriesMetadata.TYPE);
                        final RepositoriesMetadata withGenerations = currentMetadata.withUpdatedGeneration(metadata.name(), newGen, newGen);
                        final RepositoriesMetadata withUuid = meta.uuid().equals(newRepositoryData.getUuid())
                            ? withGenerations
                            : withGenerations.withUuid(metadata.name(), newRepositoryData.getUuid());
                        final ClusterState newClusterState = stateFilter.apply(
                            ClusterState.builder(currentState)
                                .metadata(Metadata.builder(currentState.getMetadata()).putCustom(RepositoriesMetadata.TYPE, withUuid))
                                .build()
                        );
                        return updateRepositoryGenerationsIfNecessary(newClusterState, expectedGen, newGen);
                    }

                    @Override
                    public void onFailure(String source, Exception e) {
                        listener.onFailure(
                            new RepositoryException(metadata.name(), "Failed to execute cluster state update [" + source + "]", e)
                        );
                    }

                    @Override
                    public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                        logger.trace("[{}] successfully set safe repository generation to [{}]", metadata.name(), newGen);
                        cacheRepositoryData(newRepositoryData, version);
                        threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.supply(listener, () -> {
                            // Delete all now outdated index files up to 1000 blobs back from the new generation.
                            // If there are more than 1000 dangling index-N cleanup functionality on repo delete will take care of them.
                            // Deleting one older than the current expectedGen is done for BwC reasons as older versions used to keep
                            // two index-N blobs around.
                            try {
                                deleteFromContainer(
                                    blobContainer(),
                                    LongStream.range(Math.max(Math.max(expectedGen - 1, 0), newGen - 1000), newGen)
                                        .mapToObj(gen -> INDEX_FILE_PREFIX + gen)
                                        .iterator()
                                );
                            } catch (IOException e) {
                                logger.warn(
                                    () -> new ParameterizedMessage("Failed to clean up old index blobs from before [{}]", newGen),
                                    e
                                );
                            }
                            return newRepositoryData;
                        }));
                    }
                }
            );
        }, listener::onFailure);
    }

    private RepositoryData updateRepositoryData(RepositoryData repositoryData, Version repositoryMetaversion, long newGen) {
        if (SnapshotsService.includesUUIDs(repositoryMetaversion)) {
            final String clusterUUID = clusterService.state().metadata().clusterUUID();
            if (repositoryData.getClusterUUID().equals(clusterUUID) == false) {
                repositoryData = repositoryData.withClusterUuid(clusterUUID);
            }
        }
        return repositoryData.withGenId(newGen);
    }

    /**
     * Write {@code index.latest} blob to support using this repository as the basis of a url repository.
     *
     * @param newGen new repository generation
     */
    private void maybeWriteIndexLatest(long newGen) {
        if (supportURLRepo) {
            logger.debug("Repository [{}] updating index.latest with generation [{}]", metadata.name(), newGen);
            try {
                writeAtomic(blobContainer(), INDEX_LATEST_BLOB, out -> out.write(Numbers.longToBytes(newGen)), false);
            } catch (Exception e) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "Failed to write index.latest blob. If you do not intend to use this "
                            + "repository as the basis for a URL repository you may turn off attempting to write the index.latest blob by "
                            + "setting repository setting [{}] to [false]",
                        SUPPORT_URL_REPO.getKey()
                    ),
                    e
                );
            }
        }
    }

    /**
     * Ensures that {@link RepositoryData} for the given {@code safeGeneration} actually physically exists in the repository.
     * This method is used by {@link #writeIndexGen} to make sure that no writes are executed on top of a concurrently modified repository.
     * This check is necessary because {@link RepositoryData} is mostly read from the cached value in {@link #latestKnownRepositoryData}
     * which could be stale in the broken situation of a concurrent write to the repository.
     *
     * @param safeGeneration generation to verify existence for
     * @param onFailure      callback to invoke with failure in case the repository generation is not physically found in the repository
     */
    private boolean ensureSafeGenerationExists(long safeGeneration, Consumer<Exception> onFailure) throws IOException {
        logger.debug("Ensure generation [{}] that is the basis for this write exists in [{}]", safeGeneration, metadata.name());
        if (safeGeneration != RepositoryData.EMPTY_REPO_GEN && blobContainer().blobExists(INDEX_FILE_PREFIX + safeGeneration) == false) {
            Tuple<Long, String> previousWriterInfo = null;
            Exception readRepoDataEx = null;
            try {
                previousWriterInfo = readLastWriterInfo();
            } catch (Exception ex) {
                readRepoDataEx = ex;
            }
            final Exception exception = new RepositoryException(
                metadata.name(),
                "concurrent modification of the index-N file, expected current generation ["
                    + safeGeneration
                    + "] but it was not found in the repository."
                    + previousWriterMessage(previousWriterInfo)
            );
            if (readRepoDataEx != null) {
                exception.addSuppressed(readRepoDataEx);
            }
            markRepoCorrupted(safeGeneration, exception, new ActionListener<>() {
                @Override
                public void onResponse(Void aVoid) {
                    onFailure.accept(exception);
                }

                @Override
                public void onFailure(Exception e) {
                    onFailure.accept(e);
                }
            });
            return false;
        }
        return true;
    }

    /**
     * Tries to find the latest cluster UUID that wrote to this repository on a best effort basis by listing out repository root contents
     * to find the latest repository generation and then reading the cluster UUID of the last writer from the {@link RepositoryData} found
     * at this generation.
     *
     * @return tuple of repository generation and cluster UUID of the last cluster to write to this repository
     */
    private Tuple<Long, String> readLastWriterInfo() throws IOException {
        assert bestEffortConsistency == false : "This should only be used for adding information to errors in consistent mode";
        final long latestGeneration = latestIndexBlobId();
        final RepositoryData actualRepositoryData = getRepositoryData(latestGeneration);
        return Tuple.tuple(latestGeneration, actualRepositoryData.getClusterUUID());
    }

    /**
     * Updates the repository generation that running deletes and snapshot finalizations will be based on for this repository if any such
     * operations are found in the cluster state while setting the safe repository generation.
     *
     * @param state  cluster state to update
     * @param oldGen previous safe repository generation
     * @param newGen new safe repository generation
     * @return updated cluster state
     */
    private ClusterState updateRepositoryGenerationsIfNecessary(ClusterState state, long oldGen, long newGen) {
        final String repoName = metadata.name();
        final SnapshotsInProgress updatedSnapshotsInProgress;
        boolean changedSnapshots = false;
        final List<SnapshotsInProgress.Entry> snapshotEntries = new ArrayList<>();
        for (SnapshotsInProgress.Entry entry : state.custom(SnapshotsInProgress.TYPE, SnapshotsInProgress.EMPTY).entries()) {
            if (entry.repository().equals(repoName) && entry.repositoryStateId() == oldGen) {
                snapshotEntries.add(entry.withRepoGen(newGen));
                changedSnapshots = true;
            } else {
                snapshotEntries.add(entry);
            }
        }
        updatedSnapshotsInProgress = changedSnapshots ? SnapshotsInProgress.of(snapshotEntries) : null;
        final SnapshotDeletionsInProgress updatedDeletionsInProgress;
        boolean changedDeletions = false;
        final List<SnapshotDeletionsInProgress.Entry> deletionEntries = new ArrayList<>();
        for (SnapshotDeletionsInProgress.Entry entry : state.custom(SnapshotDeletionsInProgress.TYPE, SnapshotDeletionsInProgress.EMPTY)
            .getEntries()) {
            if (entry.repository().equals(repoName) && entry.repositoryStateId() == oldGen) {
                deletionEntries.add(entry.withRepoGen(newGen));
                changedDeletions = true;
            } else {
                deletionEntries.add(entry);
            }
        }
        updatedDeletionsInProgress = changedDeletions ? SnapshotDeletionsInProgress.of(deletionEntries) : null;
        return SnapshotsService.updateWithSnapshots(state, updatedSnapshotsInProgress, updatedDeletionsInProgress);
    }

    private RepositoryMetadata getRepoMetadata(ClusterState state) {
        final RepositoryMetadata repositoryMetadata = state.getMetadata()
            .<RepositoriesMetadata>custom(RepositoriesMetadata.TYPE)
            .repository(metadata.name());
        assert repositoryMetadata != null || lifecycle.stoppedOrClosed()
            : "did not find metadata for repo [" + metadata.name() + "] in state [" + lifecycleState() + "]";
        return repositoryMetadata;
    }

    /**
     * Get the latest snapshot index blob id.  Snapshot index blobs are named index-N, where N is
     * the next version number from when the index blob was written.  Each individual index-N blob is
     * only written once and never overwritten.  The highest numbered index-N blob is the latest one
     * that contains the current snapshots in the repository.
     *
     * Package private for testing
     */
    long latestIndexBlobId() throws IOException {
        try {
            // First, try listing all index-N blobs (there should only be two index-N blobs at any given
            // time in a repository if cleanup is happening properly) and pick the index-N blob with the
            // highest N value - this will be the latest index blob for the repository. Note, we do this
            // instead of directly reading the index.latest blob to get the current index-N blob because
            // index.latest is not written atomically and is not immutable - on every index-N change,
            // we first delete the old index.latest and then write the new one. If the repository is not
            // read-only, it is possible that we try deleting the index.latest blob while it is being read
            // by some other operation (such as the get snapshots operation). In some file systems, it is
            // illegal to delete a file while it is being read elsewhere (e.g. Windows). For read-only
            // repositories, we read for index.latest, both because listing blob prefixes is often unsupported
            // and because the index.latest blob will never be deleted and re-written.
            return listBlobsToGetLatestIndexId();
        } catch (UnsupportedOperationException e) {
            // If its a read-only repository, listing blobs by prefix may not be supported (e.g. a URL repository),
            // in this case, try reading the latest index generation from the index.latest blob
            try {
                return readSnapshotIndexLatestBlob();
            } catch (NoSuchFileException nsfe) {
                return RepositoryData.EMPTY_REPO_GEN;
            }
        }
    }

    // package private for testing
    long readSnapshotIndexLatestBlob() throws IOException {
        return Numbers.bytesToLong(Streams.readFully(blobContainer().readBlob(INDEX_LATEST_BLOB)).toBytesRef());
    }

    private long listBlobsToGetLatestIndexId() throws IOException {
        return latestGeneration(blobContainer().listBlobsByPrefix(INDEX_FILE_PREFIX).keySet());
    }

    private long latestGeneration(Collection<String> rootBlobs) {
        long latest = RepositoryData.EMPTY_REPO_GEN;
        for (String blobName : rootBlobs) {
            if (blobName.startsWith(INDEX_FILE_PREFIX) == false) {
                continue;
            }
            try {
                final long curr = Long.parseLong(blobName.substring(INDEX_FILE_PREFIX.length()));
                latest = Math.max(latest, curr);
            } catch (NumberFormatException nfe) {
                // the index- blob wasn't of the format index-N where N is a number,
                // no idea what this blob is but it doesn't belong in the repository!
                logger.warn("[{}] Unknown blob in the repository: {}", metadata.name(), blobName);
            }
        }
        return latest;
    }

    private void writeAtomic(
        BlobContainer container,
        final String blobName,
        CheckedConsumer<OutputStream, IOException> writer,
        boolean failIfAlreadyExists
    ) throws IOException {
        logger.trace(() -> new ParameterizedMessage("[{}] Writing [{}] to {} atomically", metadata.name(), blobName, container.path()));
        container.writeBlob(blobName, failIfAlreadyExists, true, writer);
    }

    @Override
    public void snapshotShard(SnapshotShardContext context) {
        if (isReadOnly()) {
            context.onFailure(new RepositoryException(metadata.name(), "cannot snapshot shard on a readonly repository"));
            return;
        }
        final Store store = context.store();
        final IndexCommit snapshotIndexCommit = context.indexCommit();
        final ShardId shardId = store.shardId();
        final SnapshotId snapshotId = context.snapshotId();
        final IndexShardSnapshotStatus snapshotStatus = context.status();
        final long startTime = threadPool.absoluteTimeInMillis();
        try {
            final ShardGeneration generation = snapshotStatus.generation();
            logger.debug("[{}] [{}] snapshot to [{}] [{}] ...", shardId, snapshotId, metadata.name(), generation);
            final BlobContainer shardContainer = shardContainer(context.indexId(), shardId);
            final Set<String> blobs;
            if (generation == null) {
                try {
                    blobs = shardContainer.listBlobsByPrefix(INDEX_FILE_PREFIX).keySet();
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId, "failed to list blobs", e);
                }
            } else {
                blobs = Collections.singleton(INDEX_FILE_PREFIX + generation);
            }

            Tuple<BlobStoreIndexShardSnapshots, ShardGeneration> tuple = buildBlobStoreIndexShardSnapshots(
                blobs,
                shardContainer,
                generation
            );
            BlobStoreIndexShardSnapshots snapshots = tuple.v1();
            ShardGeneration fileListGeneration = tuple.v2();

            if (snapshots.snapshots().stream().anyMatch(sf -> sf.snapshot().equals(snapshotId.getName()))) {
                throw new IndexShardSnapshotFailedException(
                    shardId,
                    "Duplicate snapshot name [" + snapshotId.getName() + "] detected, aborting"
                );
            }
            // First inspect all known SegmentInfos instances to see if we already have an equivalent commit in the repository
            final List<BlobStoreIndexShardSnapshot.FileInfo> filesFromSegmentInfos = Optional.ofNullable(context.stateIdentifier())
                .map(id -> {
                    for (SnapshotFiles snapshotFileSet : snapshots.snapshots()) {
                        if (id.equals(snapshotFileSet.shardStateIdentifier())) {
                            return snapshotFileSet.indexFiles();
                        }
                    }
                    return null;
                })
                .orElse(null);

            final List<BlobStoreIndexShardSnapshot.FileInfo> indexCommitPointFiles;
            int indexIncrementalFileCount = 0;
            int indexTotalNumberOfFiles = 0;
            long indexIncrementalSize = 0;
            long indexTotalFileSize = 0;
            final BlockingQueue<BlobStoreIndexShardSnapshot.FileInfo> filesToSnapshot = new LinkedBlockingQueue<>();
            // If we did not find a set of files that is equal to the current commit we determine the files to upload by comparing files
            // in the commit with files already in the repository
            if (filesFromSegmentInfos == null) {
                indexCommitPointFiles = new ArrayList<>();
                final Collection<String> fileNames;
                final Store.MetadataSnapshot metadataFromStore;
                try (Releasable ignored = incrementStoreRef(store, snapshotStatus, shardId)) {
                    // TODO apparently we don't use the MetadataSnapshot#.recoveryDiff(...) here but we should
                    try {
                        logger.trace("[{}] [{}] Loading store metadata using index commit [{}]", shardId, snapshotId, snapshotIndexCommit);
                        metadataFromStore = store.getMetadata(snapshotIndexCommit);
                        fileNames = snapshotIndexCommit.getFileNames();
                    } catch (IOException e) {
                        throw new IndexShardSnapshotFailedException(shardId, "Failed to get store file metadata", e);
                    }
                }
                for (String fileName : fileNames) {
                    if (snapshotStatus.isAborted()) {
                        logger.debug("[{}] [{}] Aborted on the file [{}], exiting", shardId, snapshotId, fileName);
                        throw new AbortedSnapshotException();
                    }

                    logger.trace("[{}] [{}] Processing [{}]", shardId, snapshotId, fileName);
                    final StoreFileMetadata md = metadataFromStore.get(fileName);
                    BlobStoreIndexShardSnapshot.FileInfo existingFileInfo = null;
                    List<BlobStoreIndexShardSnapshot.FileInfo> filesInfo = snapshots.findPhysicalIndexFiles(fileName);
                    if (filesInfo != null) {
                        for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : filesInfo) {
                            if (fileInfo.isSame(md)) {
                                // a commit point file with the same name, size and checksum was already copied to repository
                                // we will reuse it for this snapshot
                                existingFileInfo = fileInfo;
                                break;
                            }
                        }
                    }

                    // We can skip writing blobs where the metadata hash is equal to the blob's contents because we store the hash/contents
                    // directly in the shard level metadata in this case
                    final boolean needsWrite = md.hashEqualsContents() == false;
                    indexTotalFileSize += md.length();
                    indexTotalNumberOfFiles++;

                    if (existingFileInfo == null) {
                        indexIncrementalFileCount++;
                        indexIncrementalSize += md.length();
                        // create a new FileInfo
                        BlobStoreIndexShardSnapshot.FileInfo snapshotFileInfo = new BlobStoreIndexShardSnapshot.FileInfo(
                            (needsWrite ? UPLOADED_DATA_BLOB_PREFIX : VIRTUAL_DATA_BLOB_PREFIX) + UUIDs.randomBase64UUID(),
                            md,
                            chunkSize()
                        );
                        indexCommitPointFiles.add(snapshotFileInfo);
                        if (needsWrite) {
                            filesToSnapshot.add(snapshotFileInfo);
                        }
                        assert needsWrite || assertFileContentsMatchHash(snapshotStatus, snapshotFileInfo, store);
                    } else {
                        indexCommitPointFiles.add(existingFileInfo);
                    }
                }
            } else {
                for (BlobStoreIndexShardSnapshot.FileInfo fileInfo : filesFromSegmentInfos) {
                    indexTotalNumberOfFiles++;
                    indexTotalFileSize += fileInfo.length();
                }
                indexCommitPointFiles = filesFromSegmentInfos;
            }

            snapshotStatus.moveToStarted(
                startTime,
                indexIncrementalFileCount,
                indexTotalNumberOfFiles,
                indexIncrementalSize,
                indexTotalFileSize
            );

            final ShardGeneration indexGeneration;
            final boolean writeShardGens = SnapshotsService.useShardGenerations(context.getRepositoryMetaVersion());
            // build a new BlobStoreIndexShardSnapshot, that includes this one and all the saved ones
            List<SnapshotFiles> newSnapshotsList = new ArrayList<>();
            newSnapshotsList.add(new SnapshotFiles(snapshotId.getName(), indexCommitPointFiles, context.stateIdentifier()));
            for (SnapshotFiles point : snapshots) {
                newSnapshotsList.add(point);
            }
            final BlobStoreIndexShardSnapshots updatedBlobStoreIndexShardSnapshots = new BlobStoreIndexShardSnapshots(newSnapshotsList);
            final Runnable afterWriteSnapBlob;
            if (writeShardGens) {
                // When using shard generations we can safely write the index-${uuid} blob before writing out any of the actual data
                // for this shard since the uuid named blob will simply not be referenced in case of error and thus we will never
                // reference a generation that has not had all its files fully upload.
                indexGeneration = ShardGeneration.newGeneration();
                try {
                    INDEX_SHARD_SNAPSHOTS_FORMAT.write(
                        updatedBlobStoreIndexShardSnapshots,
                        shardContainer,
                        indexGeneration.toBlobNamePart(),
                        compress
                    );
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(
                        shardId,
                        "Failed to write shard level snapshot metadata for ["
                            + snapshotId
                            + "] to ["
                            + INDEX_SHARD_SNAPSHOTS_FORMAT.blobName(indexGeneration.toBlobNamePart())
                            + "]",
                        e
                    );
                }
                afterWriteSnapBlob = () -> {};
            } else {
                // When not using shard generations we can only write the index-${N} blob after all other work for this shard has
                // completed.
                // Also, in case of numeric shard generations the data node has to take care of deleting old shard generations.
                final long newGen = Long.parseLong(fileListGeneration.toBlobNamePart()) + 1;
                indexGeneration = new ShardGeneration(newGen);
                // Delete all previous index-N blobs
                final List<String> blobsToDelete = blobs.stream()
                    .filter(blob -> blob.startsWith(SNAPSHOT_INDEX_PREFIX))
                    .collect(Collectors.toList());
                assert blobsToDelete.stream()
                    .mapToLong(b -> Long.parseLong(b.replaceFirst(SNAPSHOT_INDEX_PREFIX, "")))
                    .max()
                    .orElse(-1L) < Long.parseLong(indexGeneration.toString())
                    : "Tried to delete an index-N blob newer than the current generation ["
                        + indexGeneration
                        + "] when deleting index-N blobs "
                        + blobsToDelete;
                afterWriteSnapBlob = () -> {
                    try {
                        writeShardIndexBlobAtomic(shardContainer, newGen, updatedBlobStoreIndexShardSnapshots);
                    } catch (IOException e) {
                        throw new IndexShardSnapshotFailedException(
                            shardId,
                            "Failed to finalize snapshot creation ["
                                + snapshotId
                                + "] with shard index ["
                                + INDEX_SHARD_SNAPSHOTS_FORMAT.blobName(indexGeneration.toBlobNamePart())
                                + "]",
                            e
                        );
                    }
                    try {
                        deleteFromContainer(shardContainer, blobsToDelete.iterator());
                    } catch (IOException e) {
                        logger.warn(
                            () -> new ParameterizedMessage(
                                "[{}][{}] failed to delete old index-N blobs during finalization",
                                snapshotId,
                                shardId
                            ),
                            e
                        );
                    }
                };
            }

            final StepListener<Collection<Void>> allFilesUploadedListener = new StepListener<>();
            allFilesUploadedListener.whenComplete(v -> {
                final IndexShardSnapshotStatus.Copy lastSnapshotStatus = snapshotStatus.moveToFinalize(snapshotIndexCommit.getGeneration());

                // now create and write the commit point
                logger.trace("[{}] [{}] writing shard snapshot file", shardId, snapshotId);
                final BlobStoreIndexShardSnapshot blobStoreIndexShardSnapshot = new BlobStoreIndexShardSnapshot(
                    snapshotId.getName(),
                    lastSnapshotStatus.getIndexVersion(),
                    indexCommitPointFiles,
                    lastSnapshotStatus.getStartTime(),
                    threadPool.absoluteTimeInMillis() - lastSnapshotStatus.getStartTime(),
                    lastSnapshotStatus.getIncrementalFileCount(),
                    lastSnapshotStatus.getIncrementalSize()
                );
                try {
                    final String snapshotUUID = snapshotId.getUUID();
                    INDEX_SHARD_SNAPSHOT_FORMAT.write(blobStoreIndexShardSnapshot, shardContainer, snapshotUUID, compress);
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId, "Failed to write commit point", e);
                }
                afterWriteSnapBlob.run();
                final ShardSnapshotResult shardSnapshotResult = new ShardSnapshotResult(
                    indexGeneration,
                    ByteSizeValue.ofBytes(blobStoreIndexShardSnapshot.totalSize()),
                    getSegmentInfoFileCount(blobStoreIndexShardSnapshot.indexFiles())
                );
                snapshotStatus.moveToDone(threadPool.absoluteTimeInMillis(), shardSnapshotResult);
                context.onResponse(shardSnapshotResult);
            }, context::onFailure);
            if (indexIncrementalFileCount == 0) {
                allFilesUploadedListener.onResponse(Collections.emptyList());
                return;
            }
            final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
            // Start as many workers as fit into the snapshot pool at once at the most
            final int workers = Math.min(threadPool.info(ThreadPool.Names.SNAPSHOT).getMax(), indexIncrementalFileCount);
            final ActionListener<Void> filesListener = fileQueueListener(filesToSnapshot, workers, allFilesUploadedListener);
            for (int i = 0; i < workers; ++i) {
                executeOneFileSnapshot(store, snapshotId, context.indexId(), snapshotStatus, filesToSnapshot, executor, filesListener);
            }
        } catch (Exception e) {
            context.onFailure(e);
        }
    }

    private void executeOneFileSnapshot(
        Store store,
        SnapshotId snapshotId,
        IndexId indexId,
        IndexShardSnapshotStatus snapshotStatus,
        BlockingQueue<BlobStoreIndexShardSnapshot.FileInfo> filesToSnapshot,
        Executor executor,
        ActionListener<Void> listener
    ) throws InterruptedException {
        final ShardId shardId = store.shardId();
        final BlobStoreIndexShardSnapshot.FileInfo snapshotFileInfo = filesToSnapshot.poll(0L, TimeUnit.MILLISECONDS);
        if (snapshotFileInfo == null) {
            listener.onResponse(null);
        } else {
            executor.execute(ActionRunnable.wrap(listener, l -> {
                try (Releasable ignored = incrementStoreRef(store, snapshotStatus, shardId)) {
                    snapshotFile(snapshotFileInfo, indexId, shardId, snapshotId, snapshotStatus, store);
                    executeOneFileSnapshot(store, snapshotId, indexId, snapshotStatus, filesToSnapshot, executor, l);
                }
            }));
        }
    }

    private static Releasable incrementStoreRef(Store store, IndexShardSnapshotStatus snapshotStatus, ShardId shardId) {
        if (store.tryIncRef() == false) {
            if (snapshotStatus.isAborted()) {
                throw new AbortedSnapshotException();
            } else {
                assert false : "Store should not be closed concurrently unless snapshot is aborted";
                throw new IndexShardSnapshotFailedException(shardId, "Store got closed concurrently");
            }
        }
        return store::decRef;
    }

    private static boolean assertFileContentsMatchHash(
        IndexShardSnapshotStatus snapshotStatus,
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        Store store
    ) {
        if (store.tryIncRef()) {
            try (IndexInput indexInput = store.openVerifyingInput(fileInfo.physicalName(), IOContext.READONCE, fileInfo.metadata())) {
                final byte[] tmp = new byte[Math.toIntExact(fileInfo.metadata().length())];
                indexInput.readBytes(tmp, 0, tmp.length);
                assert fileInfo.metadata().hash().bytesEquals(new BytesRef(tmp));
            } catch (IOException e) {
                throw new AssertionError(e);
            } finally {
                store.decRef();
            }
        } else {
            assert snapshotStatus.isAborted() : "if the store is already closed we must have been aborted";
        }
        return true;
    }

    @Override
    public void restoreShard(
        Store store,
        SnapshotId snapshotId,
        IndexId indexId,
        ShardId snapshotShardId,
        RecoveryState recoveryState,
        ActionListener<Void> listener
    ) {
        final ShardId shardId = store.shardId();
        final ActionListener<Void> restoreListener = listener.delegateResponse(
            (l, e) -> l.onFailure(new IndexShardRestoreFailedException(shardId, "failed to restore snapshot [" + snapshotId + "]", e))
        );
        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        final BlobContainer container = shardContainer(indexId, snapshotShardId);
        executor.execute(ActionRunnable.wrap(restoreListener, l -> {
            final BlobStoreIndexShardSnapshot snapshot = loadShardSnapshot(container, snapshotId);
            final SnapshotFiles snapshotFiles = new SnapshotFiles(snapshot.snapshot(), snapshot.indexFiles(), null);
            new FileRestoreContext(metadata.name(), shardId, snapshotId, recoveryState) {
                @Override
                protected void restoreFiles(
                    List<BlobStoreIndexShardSnapshot.FileInfo> filesToRecover,
                    Store store,
                    ActionListener<Void> listener
                ) {
                    if (filesToRecover.isEmpty()) {
                        listener.onResponse(null);
                    } else {
                        // Start as many workers as fit into the snapshot pool at once at the most
                        final int workers = Math.min(
                            threadPool.info(ThreadPool.Names.SNAPSHOT).getMax(),
                            snapshotFiles.indexFiles().size()
                        );
                        final BlockingQueue<BlobStoreIndexShardSnapshot.FileInfo> files = new LinkedBlockingQueue<>(filesToRecover);
                        final ActionListener<Void> allFilesListener = fileQueueListener(files, workers, listener.map(v -> null));
                        // restore the files from the snapshot to the Lucene store
                        for (int i = 0; i < workers; ++i) {
                            try {
                                executeOneFileRestore(files, allFilesListener);
                            } catch (Exception e) {
                                allFilesListener.onFailure(e);
                            }
                        }
                    }
                }

                private void executeOneFileRestore(
                    BlockingQueue<BlobStoreIndexShardSnapshot.FileInfo> files,
                    ActionListener<Void> allFilesListener
                ) throws InterruptedException {
                    final BlobStoreIndexShardSnapshot.FileInfo fileToRecover = files.poll(0L, TimeUnit.MILLISECONDS);
                    if (fileToRecover == null) {
                        allFilesListener.onResponse(null);
                    } else {
                        executor.execute(ActionRunnable.wrap(allFilesListener, filesListener -> {
                            store.incRef();
                            try {
                                restoreFile(fileToRecover, store);
                            } finally {
                                store.decRef();
                            }
                            executeOneFileRestore(files, filesListener);
                        }));
                    }
                }

                private void restoreFile(BlobStoreIndexShardSnapshot.FileInfo fileInfo, Store store) throws IOException {
                    ensureNotClosing(store);
                    logger.trace(() -> new ParameterizedMessage("[{}] restoring [{}] to [{}]", metadata.name(), fileInfo, store));
                    boolean success = false;
                    try (
                        IndexOutput indexOutput = store.createVerifyingOutput(
                            fileInfo.physicalName(),
                            fileInfo.metadata(),
                            IOContext.DEFAULT
                        )
                    ) {
                        if (fileInfo.name().startsWith(VIRTUAL_DATA_BLOB_PREFIX)) {
                            final BytesRef hash = fileInfo.metadata().hash();
                            indexOutput.writeBytes(hash.bytes, hash.offset, hash.length);
                            recoveryState.getIndex().addRecoveredBytesToFile(fileInfo.physicalName(), hash.length);
                        } else {
                            try (InputStream stream = maybeRateLimitRestores(new SlicedInputStream(fileInfo.numberOfParts()) {
                                @Override
                                protected InputStream openSlice(int slice) throws IOException {
                                    ensureNotClosing(store);
                                    return container.readBlob(fileInfo.partName(slice));
                                }
                            })) {
                                final byte[] buffer = new byte[Math.toIntExact(Math.min(bufferSize, fileInfo.length()))];
                                int length;
                                while ((length = stream.read(buffer)) > 0) {
                                    ensureNotClosing(store);
                                    indexOutput.writeBytes(buffer, 0, length);
                                    recoveryState.getIndex().addRecoveredBytesToFile(fileInfo.physicalName(), length);
                                }
                            }
                        }
                        Store.verify(indexOutput);
                        indexOutput.close();
                        store.directory().sync(Collections.singleton(fileInfo.physicalName()));
                        success = true;
                    } catch (CorruptIndexException | IndexFormatTooOldException | IndexFormatTooNewException ex) {
                        try {
                            store.markStoreCorrupted(ex);
                        } catch (IOException e) {
                            logger.warn("store cannot be marked as corrupted", e);
                        }
                        throw ex;
                    } finally {
                        if (success == false) {
                            store.deleteQuiet(fileInfo.physicalName());
                        }
                    }
                }

                void ensureNotClosing(final Store store) throws AlreadyClosedException {
                    assert store.refCount() > 0;
                    if (store.isClosing()) {
                        throw new AlreadyClosedException("store is closing");
                    }
                }

            }.restore(snapshotFiles, store, l);
        }));
    }

    private static ActionListener<Void> fileQueueListener(
        BlockingQueue<BlobStoreIndexShardSnapshot.FileInfo> files,
        int workers,
        ActionListener<Collection<Void>> listener
    ) {
        return new GroupedActionListener<>(listener, workers).delegateResponse((l, e) -> {
            files.clear(); // Stop uploading the remaining files if we run into any exception
            l.onFailure(e);
        });
    }

    private static InputStream maybeRateLimit(
        InputStream stream,
        Supplier<RateLimiter> rateLimiterSupplier,
        RateLimitingInputStream.Listener throttleListener
    ) {
        return new RateLimitingInputStream(stream, rateLimiterSupplier, throttleListener);
    }

    /**
     * Wrap the restore rate limiter (controlled by the repository setting `max_restore_bytes_per_sec` and the cluster setting
     * `indices.recovery.max_bytes_per_sec`) around the given stream. Any throttling is reported to the given listener and not otherwise
     * recorded in the value returned by {@link BlobStoreRepository#getRestoreThrottleTimeInNanos}.
     */
    public InputStream maybeRateLimitRestores(InputStream stream) {
        return maybeRateLimitRestores(stream, restoreRateLimitingTimeInNanos::inc);
    }

    /**
     * Wrap the restore rate limiter (controlled by the repository setting `max_restore_bytes_per_sec` and the cluster setting
     * `indices.recovery.max_bytes_per_sec`) around the given stream. Any throttling is recorded in the value returned by {@link
     * BlobStoreRepository#getRestoreThrottleTimeInNanos}.
     */
    public InputStream maybeRateLimitRestores(InputStream stream, RateLimitingInputStream.Listener throttleListener) {
        return maybeRateLimit(
            maybeRateLimit(stream, () -> restoreRateLimiter, throttleListener),
            recoverySettings::rateLimiter,
            throttleListener
        );
    }

    /**
     * Wrap the snapshot rate limiter (controlled by the repository setting `max_snapshot_bytes_per_sec`) around the given stream. Any
     * throttling is recorded in the value returned by {@link BlobStoreRepository#getSnapshotThrottleTimeInNanos()}.
     */
    public InputStream maybeRateLimitSnapshots(InputStream stream) {
        return maybeRateLimitSnapshots(stream, snapshotRateLimitingTimeInNanos::inc);
    }

    /**
     * Wrap the snapshot rate limiter (controlled by the repository setting `max_snapshot_bytes_per_sec`) around the given stream. Any
     * throttling is reported to the given listener and not otherwise recorded in the value returned by {@link
     * BlobStoreRepository#getSnapshotThrottleTimeInNanos()}.
     */
    public InputStream maybeRateLimitSnapshots(InputStream stream, RateLimitingInputStream.Listener throttleListener) {
        return maybeRateLimit(stream, () -> snapshotRateLimiter, throttleListener);
    }

    @Override
    public IndexShardSnapshotStatus getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
        BlobStoreIndexShardSnapshot snapshot = loadShardSnapshot(shardContainer(indexId, shardId), snapshotId);
        return IndexShardSnapshotStatus.newDone(
            snapshot.startTime(),
            snapshot.time(),
            snapshot.incrementalFileCount(),
            snapshot.totalFileCount(),
            snapshot.incrementalSize(),
            snapshot.totalSize(),
            null
        ); // Not adding a real generation here as it doesn't matter to callers
    }

    @Override
    public void verify(String seed, DiscoveryNode localNode) {
        assertSnapshotOrGenericThread();
        if (isReadOnly()) {
            try {
                latestIndexBlobId();
            } catch (Exception e) {
                throw new RepositoryVerificationException(
                    metadata.name(),
                    "path " + basePath() + " is not accessible on node " + localNode,
                    e
                );
            }
        } else {
            BlobContainer testBlobContainer = blobStore().blobContainer(basePath().add(testBlobPrefix(seed)));
            try {
                testBlobContainer.writeBlob("data-" + localNode.getId() + ".dat", new BytesArray(seed), true);
            } catch (Exception exp) {
                throw new RepositoryVerificationException(
                    metadata.name(),
                    "store location [" + blobStore() + "] is not accessible on the node [" + localNode + "]",
                    exp
                );
            }
            try (InputStream masterDat = testBlobContainer.readBlob("master.dat")) {
                final String seedRead = Streams.readFully(masterDat).utf8ToString();
                if (seedRead.equals(seed) == false) {
                    throw new RepositoryVerificationException(
                        metadata.name(),
                        "Seed read from master.dat was [" + seedRead + "] but expected seed [" + seed + "]"
                    );
                }
            } catch (NoSuchFileException e) {
                throw new RepositoryVerificationException(
                    metadata.name(),
                    "a file written by master to the store ["
                        + blobStore()
                        + "] cannot be accessed on the node ["
                        + localNode
                        + "]. "
                        + "This might indicate that the store ["
                        + blobStore()
                        + "] is not shared between this node and the master node or "
                        + "that permissions on the store don't allow reading files written by the master node",
                    e
                );
            } catch (Exception e) {
                throw new RepositoryVerificationException(metadata.name(), "Failed to verify repository", e);
            }
        }
    }

    @Override
    public String toString() {
        return "BlobStoreRepository[" + "[" + metadata.name() + "], [" + blobStore.get() + ']' + ']';
    }

    /**
     * Delete snapshot from shard level metadata.
     *
     * @param indexGeneration generation to write the new shard level level metadata to. If negative a uuid id shard generation should be
     *                        used
     */
    private ShardSnapshotMetaDeleteResult deleteFromShardSnapshotMeta(
        Set<SnapshotId> survivingSnapshots,
        IndexId indexId,
        int snapshotShardId,
        Collection<SnapshotId> snapshotIds,
        BlobContainer shardContainer,
        Set<String> blobs,
        BlobStoreIndexShardSnapshots snapshots,
        long indexGeneration
    ) {
        // Build a list of snapshots that should be preserved
        List<SnapshotFiles> newSnapshotsList = new ArrayList<>();
        final Set<String> survivingSnapshotNames = survivingSnapshots.stream().map(SnapshotId::getName).collect(Collectors.toSet());
        for (SnapshotFiles point : snapshots) {
            if (survivingSnapshotNames.contains(point.snapshot())) {
                newSnapshotsList.add(point);
            }
        }
        ShardGeneration writtenGeneration = null;
        try {
            if (newSnapshotsList.isEmpty()) {
                return new ShardSnapshotMetaDeleteResult(indexId, snapshotShardId, ShardGenerations.DELETED_SHARD_GEN, blobs);
            } else {
                final BlobStoreIndexShardSnapshots updatedSnapshots = new BlobStoreIndexShardSnapshots(newSnapshotsList);
                if (indexGeneration < 0L) {
                    writtenGeneration = ShardGeneration.newGeneration();
                    INDEX_SHARD_SNAPSHOTS_FORMAT.write(updatedSnapshots, shardContainer, writtenGeneration.toBlobNamePart(), compress);
                } else {
                    writtenGeneration = new ShardGeneration(indexGeneration);
                    writeShardIndexBlobAtomic(shardContainer, indexGeneration, updatedSnapshots);
                }
                final Set<String> survivingSnapshotUUIDs = survivingSnapshots.stream().map(SnapshotId::getUUID).collect(Collectors.toSet());
                return new ShardSnapshotMetaDeleteResult(
                    indexId,
                    snapshotShardId,
                    writtenGeneration,
                    unusedBlobs(blobs, survivingSnapshotUUIDs, updatedSnapshots)
                );
            }
        } catch (IOException e) {
            throw new RepositoryException(
                metadata.name(),
                "Failed to finalize snapshot deletion "
                    + snapshotIds
                    + " with shard index ["
                    + INDEX_SHARD_SNAPSHOTS_FORMAT.blobName(writtenGeneration.toBlobNamePart())
                    + "]",
                e
            );
        }
    }

    /**
     * Utility for atomically writing shard level metadata to a numeric shard generation. This is only required for writing
     * numeric shard generations where atomic writes with fail-if-already-exists checks are useful in preventing repository corruption.
     */
    private void writeShardIndexBlobAtomic(
        BlobContainer shardContainer,
        long indexGeneration,
        BlobStoreIndexShardSnapshots updatedSnapshots
    ) throws IOException {
        assert indexGeneration >= 0 : "Shard generation must not be negative but saw [" + indexGeneration + "]";
        logger.trace(
            () -> new ParameterizedMessage("[{}] Writing shard index [{}] to [{}]", metadata.name(), indexGeneration, shardContainer.path())
        );
        final String blobName = INDEX_SHARD_SNAPSHOTS_FORMAT.blobName(String.valueOf(indexGeneration));
        writeAtomic(
            shardContainer,
            blobName,
            out -> INDEX_SHARD_SNAPSHOTS_FORMAT.serialize(updatedSnapshots, blobName, compress, out),
            true
        );
    }

    // Unused blobs are all previous index-, data- and meta-blobs and that are not referenced by the new index- as well as all
    // temporary blobs
    private static List<String> unusedBlobs(
        Set<String> blobs,
        Set<String> survivingSnapshotUUIDs,
        BlobStoreIndexShardSnapshots updatedSnapshots
    ) {
        return blobs.stream()
            .filter(
                blob -> blob.startsWith(SNAPSHOT_INDEX_PREFIX)
                    || (blob.startsWith(SNAPSHOT_PREFIX)
                        && blob.endsWith(".dat")
                        && survivingSnapshotUUIDs.contains(
                            blob.substring(SNAPSHOT_PREFIX.length(), blob.length() - ".dat".length())
                        ) == false)
                    || (blob.startsWith(UPLOADED_DATA_BLOB_PREFIX) && updatedSnapshots.findNameFile(canonicalName(blob)) == null)
                    || FsBlobContainer.isTempBlobName(blob)
            )
            .collect(Collectors.toList());
    }

    /**
     * Loads information about shard snapshot
     */
    public BlobStoreIndexShardSnapshot loadShardSnapshot(BlobContainer shardContainer, SnapshotId snapshotId) {
        try {
            return INDEX_SHARD_SNAPSHOT_FORMAT.read(metadata.name(), shardContainer, snapshotId.getUUID(), namedXContentRegistry);
        } catch (NoSuchFileException ex) {
            throw new SnapshotMissingException(metadata.name(), snapshotId, ex);
        } catch (IOException ex) {
            throw new SnapshotException(
                metadata.name(),
                snapshotId,
                "failed to read shard snapshot file for [" + shardContainer.path() + ']',
                ex
            );
        }
    }

    /**
     * Loads all available snapshots in the repository using the given {@code generation} for a shard. When {@code shardGen}
     * is null it tries to load it using the BwC mode, listing the available index- blobs in the shard container.
     */
    public BlobStoreIndexShardSnapshots getBlobStoreIndexShardSnapshots(IndexId indexId, int shardId, @Nullable ShardGeneration shardGen)
        throws IOException {
        final BlobContainer shardContainer = shardContainer(indexId, shardId);

        Set<String> blobs = Collections.emptySet();
        if (shardGen == null) {
            blobs = shardContainer.listBlobsByPrefix(INDEX_FILE_PREFIX).keySet();
        }

        return buildBlobStoreIndexShardSnapshots(blobs, shardContainer, shardGen).v1();
    }

    /**
     * Loads all available snapshots in the repository using the given {@code generation} or falling back to trying to determine it from
     * the given list of blobs in the shard container.
     *
     * @param blobs      list of blobs in repository
     * @param generation shard generation or {@code null} in case there was no shard generation tracked in the {@link RepositoryData} for
     *                   this shard because its snapshot was created in a version older than
     *                   {@link SnapshotsService#SHARD_GEN_IN_REPO_DATA_VERSION}.
     * @return tuple of BlobStoreIndexShardSnapshots and the last snapshot index generation
     */
    private Tuple<BlobStoreIndexShardSnapshots, ShardGeneration> buildBlobStoreIndexShardSnapshots(
        Set<String> blobs,
        BlobContainer shardContainer,
        @Nullable ShardGeneration generation
    ) throws IOException {
        if (generation != null) {
            if (generation.equals(ShardGenerations.NEW_SHARD_GEN)) {
                return new Tuple<>(BlobStoreIndexShardSnapshots.EMPTY, ShardGenerations.NEW_SHARD_GEN);
            }
            return new Tuple<>(
                INDEX_SHARD_SNAPSHOTS_FORMAT.read(metadata.name(), shardContainer, generation.toBlobNamePart(), namedXContentRegistry),
                generation
            );
        }
        final Tuple<BlobStoreIndexShardSnapshots, Long> legacyIndex = buildBlobStoreIndexShardSnapshots(blobs, shardContainer);
        return new Tuple<>(legacyIndex.v1(), new ShardGeneration(legacyIndex.v2()));
    }

    /**
     * Loads all available snapshots in the repository
     *
     * @param blobs list of blobs in repository
     * @return tuple of BlobStoreIndexShardSnapshots and the last snapshot index generation
     */
    private Tuple<BlobStoreIndexShardSnapshots, Long> buildBlobStoreIndexShardSnapshots(Set<String> blobs, BlobContainer shardContainer)
        throws IOException {
        long latest = latestGeneration(blobs);
        if (latest >= 0) {
            final BlobStoreIndexShardSnapshots shardSnapshots = INDEX_SHARD_SNAPSHOTS_FORMAT.read(
                metadata.name(),
                shardContainer,
                Long.toString(latest),
                namedXContentRegistry
            );
            return new Tuple<>(shardSnapshots, latest);
        } else if (blobs.stream()
            .anyMatch(b -> b.startsWith(SNAPSHOT_PREFIX) || b.startsWith(INDEX_FILE_PREFIX) || b.startsWith(UPLOADED_DATA_BLOB_PREFIX))) {
                logger.warn(
                    "Could not find a readable index-N file in a non-empty shard snapshot directory [" + shardContainer.path() + "]"
                );
            }
        return new Tuple<>(BlobStoreIndexShardSnapshots.EMPTY, latest);
    }

    /**
     * Snapshot individual file
     * @param fileInfo file to be snapshotted
     */
    private void snapshotFile(
        BlobStoreIndexShardSnapshot.FileInfo fileInfo,
        IndexId indexId,
        ShardId shardId,
        SnapshotId snapshotId,
        IndexShardSnapshotStatus snapshotStatus,
        Store store
    ) throws IOException {
        final BlobContainer shardContainer = shardContainer(indexId, shardId);
        final String file = fileInfo.physicalName();
        try (IndexInput indexInput = store.openVerifyingInput(file, IOContext.READONCE, fileInfo.metadata())) {
            for (int i = 0; i < fileInfo.numberOfParts(); i++) {
                final long partBytes = fileInfo.partBytes(i);

                // Make reads abortable by mutating the snapshotStatus object
                final InputStream inputStream = new FilterInputStream(
                    maybeRateLimitSnapshots(new InputStreamIndexInput(indexInput, partBytes))
                ) {
                    @Override
                    public int read() throws IOException {
                        checkAborted();
                        return super.read();
                    }

                    @Override
                    public int read(byte[] b, int off, int len) throws IOException {
                        checkAborted();
                        return super.read(b, off, len);
                    }

                    private void checkAborted() {
                        if (snapshotStatus.isAborted()) {
                            logger.debug("[{}] [{}] Aborted on the file [{}], exiting", shardId, snapshotId, fileInfo.physicalName());
                            throw new AbortedSnapshotException();
                        }
                    }
                };
                final String partName = fileInfo.partName(i);
                logger.trace(() -> new ParameterizedMessage("[{}] Writing [{}] to [{}]", metadata.name(), partName, shardContainer.path()));
                shardContainer.writeBlob(partName, inputStream, partBytes, false);
            }
            Store.verify(indexInput);
            snapshotStatus.addProcessedFile(fileInfo.length());
        } catch (Exception t) {
            failStoreIfCorrupted(store, t);
            snapshotStatus.addProcessedFile(0);
            throw t;
        }
    }

    private static void failStoreIfCorrupted(Store store, Exception e) {
        if (Lucene.isCorruptionException(e)) {
            try {
                store.markStoreCorrupted((IOException) e);
            } catch (IOException inner) {
                inner.addSuppressed(e);
                logger.warn("store cannot be marked as corrupted", inner);
            }
        }
    }

    public boolean supportURLRepo() {
        return supportURLRepo;
    }

    /**
     * @return whether this repository performs overwrites atomically. In practice we only overwrite the `index.latest` blob so this
     * is not very important, but the repository analyzer does test that overwrites happen atomically. It will skip those tests if the
     * repository overrides this method to indicate that it does not support atomic overwrites.
     */
    public boolean hasAtomicOverwrites() {
        return true;
    }

    /**
     * The result of removing a snapshot from a shard folder in the repository.
     */
    private static final class ShardSnapshotMetaDeleteResult {

        // Index that the snapshot was removed from
        private final IndexId indexId;

        // Shard id that the snapshot was removed from
        private final int shardId;

        // Id of the new index-${uuid} blob that does not include the snapshot any more
        private final ShardGeneration newGeneration;

        // Blob names in the shard directory that have become unreferenced in the new shard generation
        private final Collection<String> blobsToDelete;

        ShardSnapshotMetaDeleteResult(IndexId indexId, int shardId, ShardGeneration newGeneration, Collection<String> blobsToDelete) {
            this.indexId = indexId;
            this.shardId = shardId;
            this.newGeneration = newGeneration;
            this.blobsToDelete = blobsToDelete;
        }
    }
}
