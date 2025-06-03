/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
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
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.SingleResultDeduplicator;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.RepositoryCleanupInProgress;
import org.elasticsearch.cluster.SnapshotDeletionsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoriesMetadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.fs.FsBlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.compress.DeflateCompressor;
import org.elasticsearch.common.compress.NotXContentException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.OutputStreamStreamOutput;
import org.elasticsearch.common.io.stream.ReleasableBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.ThrottledIterator;
import org.elasticsearch.common.util.concurrent.ThrottledTaskRunner;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.core.AbstractRefCounted;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexVersion;
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
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.IndexMetaDataGenerations;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
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
import org.elasticsearch.snapshots.PausedSnapshotException;
import org.elasticsearch.snapshots.SnapshotException;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotMissingException;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.LeakTracker;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
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
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import static org.elasticsearch.common.blobstore.OperationPurpose.SNAPSHOT_METADATA;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo;
import static org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot.FileInfo.canonicalName;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;

/**
 * BlobStore - based implementation of Snapshot Repository
 * <p>
 * This repository works with any {@link BlobStore} implementation. The blobStore could be (and is preferably) lazily initialized in
 * {@link #createBlobStore()}.
 * </p>
 * For in depth documentation on how exactly implementations of this class interact with the snapshot functionality please refer to the
 * documentation of the package {@link org.elasticsearch.repositories.blobstore}.
 */
public abstract class BlobStoreRepository extends AbstractLifecycleComponent implements Repository {
    private static final Logger logger = LogManager.getLogger(BlobStoreRepository.class);

    private class ShutdownLogger {
        // Creating a separate logger so that the log-level can be manipulated separately from the parent class.
        private static final Logger shutdownLogger = LogManager.getLogger(ShutdownLogger.class);
    }

    private final ProjectId projectId;
    protected volatile RepositoryMetadata metadata;

    protected final ThreadPool threadPool;

    public static final String STATELESS_SHARD_READ_THREAD_NAME = "stateless_shard_read";
    public static final String STATELESS_TRANSLOG_THREAD_NAME = "stateless_translog";
    public static final String STATELESS_SHARD_WRITE_THREAD_NAME = "stateless_shard_write";
    public static final String STATELESS_CLUSTER_STATE_READ_WRITE_THREAD_NAME = "stateless_cluster_state";
    public static final String STATELESS_SHARD_PREWARMING_THREAD_NAME = "stateless_prewarm";
    public static final String STATELESS_SHARD_UPLOAD_PREWARMING_THREAD_NAME = "stateless_upload_prewarm";
    public static final String SEARCHABLE_SNAPSHOTS_CACHE_FETCH_ASYNC_THREAD_NAME = "searchable_snapshots_cache_fetch_async";
    public static final String SEARCHABLE_SNAPSHOTS_CACHE_PREWARMING_THREAD_NAME = "searchable_snapshots_cache_prewarming";

    /**
     * Prefix for the name of the root {@link RepositoryData} blob.
     * @see #getRepositoryDataBlobName
     */
    public static final String INDEX_FILE_PREFIX = "index-";

    /**
     * Name of the blob which points to the root {@link RepositoryData} blob.
     * @see #supportURLRepo()
     */
    public static final String INDEX_LATEST_BLOB = "index.latest";

    private static final String TESTS_FILE = "tests-";

    /**
     * Name prefix for the blobs that hold the global {@link Metadata} for each snapshot, and the {@link IndexMetadata} for its indices.
     * @see #METADATA_NAME_FORMAT
     */
    public static final String METADATA_PREFIX = "meta-";

    /**
     * Name prefix for the blobs that hold top-level {@link SnapshotInfo} and shard-level {@link BlobStoreIndexShardSnapshot} metadata.
     * @see #SNAPSHOT_NAME_FORMAT
     */
    public static final String SNAPSHOT_PREFIX = "snap-";

    /**
     * Name suffix for all {@code meta-*.dat} and {@code snap-*.dat} blobs.
     * @see #METADATA_NAME_FORMAT
     * @see #SNAPSHOT_NAME_FORMAT
     */
    public static final String METADATA_BLOB_NAME_SUFFIX = ".dat";

    /**
     * Blob name format for global {@link Metadata} and {@link IndexMetadata} blobs.
     * @see #GLOBAL_METADATA_FORMAT
     * @see #INDEX_METADATA_FORMAT
     */
    public static final String METADATA_NAME_FORMAT = METADATA_PREFIX + "%s" + METADATA_BLOB_NAME_SUFFIX;

    /**
     * Blob name format for top-level {@link SnapshotInfo} and shard-level {@link BlobStoreIndexShardSnapshot} blobs.
     * @see #SNAPSHOT_FORMAT
     * @see #INDEX_SHARD_SNAPSHOT_FORMAT
     */
    public static final String SNAPSHOT_NAME_FORMAT = SNAPSHOT_PREFIX + "%s" + METADATA_BLOB_NAME_SUFFIX;

    /**
     * Name prefix for shard-level {@link BlobStoreIndexShardSnapshots} blobs.
     * @see #SNAPSHOT_INDEX_NAME_FORMAT
     * @see #INDEX_SHARD_SNAPSHOTS_FORMAT
     */
    public static final String SNAPSHOT_INDEX_PREFIX = "index-";

    /**
     * Name format for shard-level {@link BlobStoreIndexShardSnapshots} blobs.
     * @see #INDEX_SHARD_SNAPSHOTS_FORMAT
     */
    public static final String SNAPSHOT_INDEX_NAME_FORMAT = SNAPSHOT_INDEX_PREFIX + "%s";

    /**
     * Name prefix for blobs holding the actual shard data.
     */
    public static final String UPLOADED_DATA_BLOB_PREFIX = "__";

    /**
     * @param repositoryGeneration The numeric generation of the {@link RepositoryData} blob.
     * @return The name of the blob holding the corresponding {@link RepositoryData}.
     */
    public static String getRepositoryDataBlobName(long repositoryGeneration) {
        return INDEX_FILE_PREFIX + repositoryGeneration;
    }

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
        Setting.Property.DeprecatedWarning
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

    /**
     * Setting that defines if the repository should be used to recover index files during peer recoveries.
     */
    public static final Setting<Boolean> USE_FOR_PEER_RECOVERY_SETTING = Setting.boolSetting("use_for_peer_recovery", false);

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
        (repoName, parser) -> Metadata.fromXContent(parser),
        ChunkedToXContent::wrapAsToXContent
    );

    public static final ChecksumBlobStoreFormat<IndexMetadata> INDEX_METADATA_FORMAT = new ChecksumBlobStoreFormat<>(
        "index-metadata",
        METADATA_NAME_FORMAT,
        (repoName, parser) -> IndexMetadata.Builder.legacyFromXContent(parser),
        (repoName, parser) -> IndexMetadata.fromXContent(parser),
        Function.identity()
    );

    private static final String SNAPSHOT_CODEC = "snapshot";

    public static final ChecksumBlobStoreFormat<SnapshotInfo> SNAPSHOT_FORMAT = new ChecksumBlobStoreFormat<>(
        SNAPSHOT_CODEC,
        SNAPSHOT_NAME_FORMAT,
        SnapshotInfo::fromXContentInternal,
        Function.identity()
    );

    public static final ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshot> INDEX_SHARD_SNAPSHOT_FORMAT = new ChecksumBlobStoreFormat<>(
        SNAPSHOT_CODEC,
        SNAPSHOT_NAME_FORMAT,
        (repoName, parser) -> BlobStoreIndexShardSnapshot.fromXContent(parser),
        Function.identity()
    );

    public static final ChecksumBlobStoreFormat<BlobStoreIndexShardSnapshots> INDEX_SHARD_SNAPSHOTS_FORMAT = new ChecksumBlobStoreFormat<>(
        "snapshots",
        SNAPSHOT_INDEX_NAME_FORMAT,
        (repoName, parser) -> BlobStoreIndexShardSnapshots.fromXContent(parser),
        Function.identity()
    );

    public static final Setting<ByteSizeValue> MAX_SNAPSHOT_BYTES_PER_SEC = Setting.byteSizeSetting(
        "max_snapshot_bytes_per_sec",
        ByteSizeValue.ofMb(40), // default is overridden to 0 (unlimited) if node bandwidth recovery settings are set
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
     * potentially accounting for the last {@code index-N} write in the cluster state.
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

    private final ShardSnapshotTaskRunner shardSnapshotTaskRunner;

    private final ThrottledTaskRunner staleBlobDeleteRunner;

    /**
     * Constructs new BlobStoreRepository
     * @param metadata   The metadata for this repository including name and settings
     * @param clusterService ClusterService
     */
    @SuppressWarnings("this-escape")
    protected BlobStoreRepository(
        final ProjectId projectId,
        final RepositoryMetadata metadata,
        final NamedXContentRegistry namedXContentRegistry,
        final ClusterService clusterService,
        final BigArrays bigArrays,
        final RecoverySettings recoverySettings,
        final BlobPath basePath
    ) {
        this.projectId = projectId;
        this.metadata = metadata;
        this.threadPool = clusterService.getClusterApplierService().threadPool();
        this.clusterService = clusterService;
        this.bigArrays = bigArrays;
        this.recoverySettings = recoverySettings;
        this.compress = COMPRESS_SETTING.get(metadata.settings());
        this.supportURLRepo = SUPPORT_URL_REPO.get(metadata.settings());
        snapshotRateLimiter = getSnapshotRateLimiter();
        restoreRateLimiter = getRestoreRateLimiter();
        readOnly = metadata.settings().getAsBoolean(READONLY_SETTING_KEY, false);
        cacheRepositoryData = CACHE_REPOSITORY_DATA.get(metadata.settings());
        bufferSize = Math.toIntExact(BUFFER_SIZE_SETTING.get(metadata.settings()).getBytes());
        this.namedXContentRegistry = namedXContentRegistry;
        this.basePath = basePath;
        this.maxSnapshotCount = MAX_SNAPSHOTS_SETTING.get(metadata.settings());
        this.repoDataLoadDeduplicator = new SingleResultDeduplicator<>(
            threadPool.getThreadContext(),
            listener -> threadPool.executor(ThreadPool.Names.SNAPSHOT_META)
                .execute(ActionRunnable.wrap(listener, this::doGetRepositoryData))
        );
        shardSnapshotTaskRunner = new ShardSnapshotTaskRunner(
            threadPool.info(ThreadPool.Names.SNAPSHOT).getMax(),
            threadPool.executor(ThreadPool.Names.SNAPSHOT),
            this::doSnapshotShard,
            this::snapshotFile
        );
        staleBlobDeleteRunner = new ThrottledTaskRunner(
            "cleanupStaleBlobs",
            threadPool.info(ThreadPool.Names.SNAPSHOT).getMax(),
            threadPool.executor(ThreadPool.Names.SNAPSHOT)
        );
    }

    @Override
    public ProjectId getProjectId() {
        return projectId;
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
        activityRefs.decRef();
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

    private final SubscribableListener<Void> closedAndIdleListeners = new SubscribableListener<>();

    private final RefCounted activityRefs = AbstractRefCounted.of(() -> closedAndIdleListeners.onResponse(null));

    @Override
    public void awaitIdle() {
        assert lifecycle.closed();
        final var future = new PlainActionFuture<Void>();
        closedAndIdleListeners.addListener(future);
        future.actionGet(); // wait for as long as it takes
    }

    @SuppressForbidden(reason = "legacy usage of unbatched task") // TODO add support for batching here
    private void submitUnbatchedTask(@SuppressWarnings("SameParameterValue") String source, ClusterStateUpdateTask task) {
        clusterService.submitUnbatchedStateUpdateTask(source, task);
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
                    shardContainer.listBlobsByPrefix(OperationPurpose.SNAPSHOT_METADATA, SNAPSHOT_INDEX_PREFIX).keySet(),
                    shardContainer
                );
                existingShardGen = new ShardGeneration(tuple.v2());
                newGen = new ShardGeneration(tuple.v2() + 1);
                existingSnapshots = tuple.v1();
            } else {
                newGen = ShardGeneration.newGeneration();
                existingSnapshots = buildBlobStoreIndexShardSnapshots(
                    index,
                    shardNum,
                    Collections.emptySet(),
                    shardContainer,
                    shardGeneration
                ).v1();
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
                newGen.getGenerationUUID(),
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
            snapshotRateLimiter = getSnapshotRateLimiter();
            restoreRateLimiter = getRestoreRateLimiter();
        }

        uncleanStart = uncleanStart && metadata.generation() != metadata.pendingGeneration();
        final boolean wasBestEffortConsistency = bestEffortConsistency;
        bestEffortConsistency = uncleanStart || isReadOnly() || metadata.generation() == RepositoryData.UNKNOWN_REPO_GEN;
        if (isReadOnly()) {
            // No need to waste cycles, no operations can run against a read-only repository
            return;
        }
        if (bestEffortConsistency) {
            long bestGenerationFromCS = bestGeneration(SnapshotsInProgress.get(state).forRepo(this.metadata.name()));
            // Don't use generation from the delete task if we already found a generation for an in progress snapshot.
            // In this case, the generation points at the generation the repo will be in after the snapshot finishes so it may not yet
            // exist
            if (bestGenerationFromCS == RepositoryData.EMPTY_REPO_GEN) {
                bestGenerationFromCS = bestGeneration(SnapshotDeletionsInProgress.get(state).getEntries());
            }
            if (bestGenerationFromCS == RepositoryData.EMPTY_REPO_GEN) {
                bestGenerationFromCS = bestGeneration(RepositoryCleanupInProgress.get(state).entries());
            }
            final long finalBestGen = Math.max(bestGenerationFromCS, metadata.generation());
            latestKnownRepoGen.accumulateAndGet(finalBestGen, Math::max);
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
        assertSnapshotOrStatelessPermittedThreadPool();

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
        assertSnapshotOrStatelessPermittedThreadPool();

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

    /**
     * Loads {@link RepositoryData} ensuring that it is consistent with the given {@code rootBlobs} as well of the assumed generation.
     *
     * @param repositoryDataGeneration Expected repository generation
     * @param rootBlobs                Blobs at the repository root
     * @return RepositoryData
     */
    private RepositoryData safeRepositoryData(long repositoryDataGeneration, Map<String, BlobMetadata> rootBlobs) {
        final long generation = latestGeneration(rootBlobs.keySet());
        final long genToLoad;
        final RepositoryData cached;
        if (bestEffortConsistency) {
            genToLoad = latestKnownRepoGen.accumulateAndGet(repositoryDataGeneration, Math::max);
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
        if (genToLoad != repositoryDataGeneration) {
            throw new RepositoryException(
                metadata.name(),
                "concurrent modification of the index-N file, expected current generation ["
                    + repositoryDataGeneration
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
     * Some repositories (i.e. S3) run at extra risk of corruption when using the pre-7.6.0 repository format, against which we try and
     * protect by adding some delays in between operations so that things have a chance to settle down. This method is the hook that allows
     * the delete process to add this protection when necessary.
     */
    protected ActionListener<RepositoryData> wrapWithWeakConsistencyProtection(ActionListener<RepositoryData> listener) {
        return listener;
    }

    @Override
    public void deleteSnapshots(
        Collection<SnapshotId> snapshotIds,
        long repositoryDataGeneration,
        IndexVersion minimumNodeVersion,
        ActionListener<RepositoryData> repositoryDataUpdateListener,
        Runnable onCompletion
    ) {
        createSnapshotsDeletion(
            snapshotIds,
            repositoryDataGeneration,
            minimumNodeVersion,
            repositoryDataUpdateListener.delegateFailureAndWrap((l, snapshotsDeletion) -> snapshotsDeletion.runDelete(l, onCompletion))
        );
    }

    /**
     * Runs cleanup actions on the repository. Increments the repository state id by one before executing any modifications on the
     * repository.
     * TODO: Add shard level cleanups
     * TODO: Add unreferenced index metadata cleanup
     * <ul>
     *     <li>Deleting stale indices</li>
     *     <li>Deleting unreferenced root level blobs</li>
     * </ul>
     *
     * @param repositoryDataGeneration         Generation of {@link RepositoryData} at start of process
     * @param repositoryFormatIndexVersion     Repository format version
     * @param listener                         Listener to complete when done
     */
    public void cleanup(long repositoryDataGeneration, IndexVersion repositoryFormatIndexVersion, ActionListener<DeleteResult> listener) {
        createSnapshotsDeletion(
            List.of(),
            repositoryDataGeneration,
            repositoryFormatIndexVersion,
            listener.delegateFailureAndWrap((delegate, snapshotsDeletion) -> snapshotsDeletion.runCleanup(delegate))
        );
    }

    private void createSnapshotsDeletion(
        Collection<SnapshotId> snapshotIds,
        long repositoryDataGeneration,
        IndexVersion minimumNodeVersion,
        ActionListener<SnapshotsDeletion> listener
    ) {
        if (isReadOnly()) {
            listener.onFailure(new RepositoryException(metadata.name(), "repository is readonly"));
        } else {
            threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.supply(listener, () -> {
                final var originalRootBlobs = blobContainer().listBlobs(OperationPurpose.SNAPSHOT_METADATA);

                // One final best-effort check for other clusters concurrently writing to the repository:
                final var originalRepositoryData = safeRepositoryData(repositoryDataGeneration, originalRootBlobs);

                return new SnapshotsDeletion(
                    snapshotIds,
                    repositoryDataGeneration,
                    SnapshotsService.minCompatibleVersion(minimumNodeVersion, originalRepositoryData, snapshotIds),
                    originalRootBlobs,
                    blobStore().blobContainer(indicesPath()).children(OperationPurpose.SNAPSHOT_DATA),
                    originalRepositoryData
                );
            }));
        }
    }

    /**
     * <p>
     *     Represents the process of deleting some collection of snapshots within this repository which since 7.6.0 looks like this:
     * </p>
     * <ul>
     *     <li>Write a new {@link BlobStoreIndexShardSnapshots} for each affected shard, and compute the blobs to delete.</li>
     *     <li>Update the {@link RepositoryData} to remove references to deleted snapshots/indices and point to the new
     *     {@link BlobStoreIndexShardSnapshots} files.</li>
     *     <li>Remove up any now-unreferenced blobs.</li>
     * </ul>
     * <p>
     *     Until the {@link RepositoryData} is updated there should be no other activities in the repository, and in particular the root
     *     blob must not change until it is updated by this deletion and the {@code repositoryDataUpdateListener} is completed.
     * </p>
     */
    class SnapshotsDeletion {

        /**
         * The IDs of the snapshots to delete. This collection is empty if the deletion is a repository cleanup.
         */
        private final Collection<SnapshotId> snapshotIds;

        /**
         * The {@link RepositoryData} generation at the start of the process, to ensure that the {@link RepositoryData} does not change
         * while the new {@link BlobStoreIndexShardSnapshots} are being written.
         */
        private final long originalRepositoryDataGeneration;

        /**
         * The minimum {@link IndexVersion} of the nodes in the cluster and the snapshots remaining in the repository. The repository must
         * remain readable by all node versions which support this {@link IndexVersion}.
         */
        private final IndexVersion repositoryFormatIndexVersion;

        /**
         * Whether the {@link #repositoryFormatIndexVersion} is new enough to support naming {@link BlobStoreIndexShardSnapshots} blobs with
         * UUIDs (i.e. does not need to remain compatible with versions before v7.6.0). Older repositories use (unsafe) numeric indices for
         * these blobs instead.
         */
        private final boolean useShardGenerations;

        /**
         * All blobs in the repository root at the start of the operation, obtained by listing the repository contents. Note that this may
         * include some blobs which are no longer referenced by the current {@link RepositoryData}, but which have not yet been removed by
         * the cleanup that follows an earlier deletion. This cleanup may still be ongoing (we do not wait for it to complete before
         * starting the next repository operation) or it may have failed before completion (it could have been running on a different node,
         * which crashed for unrelated reasons) so we track all the blobs here and clean them up again at the end.
         */
        private final Map<String, BlobMetadata> originalRootBlobs;

        /**
         * All index containers at the start of the operation, obtained by listing the repository contents. Note that this may include some
         * containers which are no longer referenced by the current {@link RepositoryData}, but which have not yet been removed by
         * the cleanup that follows an earlier deletion. This cleanup may or may not still be ongoing (it could have been running on a
         * different node, which died before completing it) so we track all the blobs here and clean them up again at the end.
         */
        private final Map<String, BlobContainer> originalIndexContainers;

        /**
         * The {@link RepositoryData} at the start of the operation, obtained after verifying that {@link #originalRootBlobs} contains no
         * {@link RepositoryData} blob newer than the one identified by {@link #originalRepositoryDataGeneration}.
         */
        private final RepositoryData originalRepositoryData;

        /**
         * Executor to use for all repository interactions.
         */
        private final Executor snapshotExecutor = threadPool.executor(ThreadPool.Names.SNAPSHOT);

        /**
         * Accumulates the number of blobs deleted by this operation.
         */
        // NB only counts stale root blobs today, not shard-level blobs
        private final AtomicLong blobsDeleted = new AtomicLong();

        /**
         * Accumulates the total size of the blobs deleted by this operation.
         */
        // NB only counts stale root blobs today, not shard-level blobs
        private final AtomicLong bytesDeleted = new AtomicLong();

        /**
         * Tracks the shard-level blobs which can be deleted once all the metadata updates have completed.
         */
        private final ShardBlobsToDelete shardBlobsToDelete = new ShardBlobsToDelete();

        SnapshotsDeletion(
            Collection<SnapshotId> snapshotIds,
            long originalRepositoryDataGeneration,
            IndexVersion repositoryFormatIndexVersion,
            Map<String, BlobMetadata> originalRootBlobs,
            Map<String, BlobContainer> originalIndexContainers,
            RepositoryData originalRepositoryData
        ) {
            this.snapshotIds = snapshotIds;
            this.originalRepositoryDataGeneration = originalRepositoryDataGeneration;
            this.repositoryFormatIndexVersion = repositoryFormatIndexVersion;
            this.useShardGenerations = SnapshotsService.useShardGenerations(repositoryFormatIndexVersion);
            this.originalRootBlobs = originalRootBlobs;
            this.originalIndexContainers = originalIndexContainers;
            this.originalRepositoryData = originalRepositoryData;
        }

        // ---------------------------------------------------------------------------------------------------------------------------------
        // The overall flow of execution

        void runDelete(ActionListener<RepositoryData> repositoryDataUpdateListener, Runnable onCompletion) {
            final var releasingListener = repositoryDataUpdateListener.delegateResponse((l, e) -> {
                try {
                    shardBlobsToDelete.close();
                } finally {
                    l.onFailure(e);
                }
            });
            final Runnable releasingOnCompletion = () -> {
                try {
                    shardBlobsToDelete.close();
                } finally {
                    onCompletion.run();
                }
            };
            if (useShardGenerations) {
                runWithUniqueShardMetadataNaming(releasingListener, releasingOnCompletion);
            } else {
                runWithLegacyNumericShardMetadataNaming(wrapWithWeakConsistencyProtection(releasingListener), releasingOnCompletion);
            }
        }

        private void runWithUniqueShardMetadataNaming(ActionListener<RepositoryData> repositoryDataUpdateListener, Runnable onCompletion) {
            SubscribableListener

                // First write the new shard state metadata (without the removed snapshots) and compute deletion targets
                .newForked(this::writeUpdatedShardMetadataAndComputeDeletes)

                .<RepositoryData>andThen(l -> {
                    // Once we have put the new shard-level metadata into place, we can update the repository metadata as follows:
                    // 1. Remove the snapshots from the list of existing snapshots
                    // 2. Update the index shard generations of all updated shard folders
                    //
                    // Note: If we fail updating any of the individual shard paths, it may not have a new BlobStoreIndexShardSnapshots blob,
                    // but if it does then that doesn't matter, we're naming them uniquely as index-{$shard-generation-uuid} and it won't be
                    // referenced by the new RepositoryData and will be cleaned up by a subsequent delete.
                    //
                    // TODO should we even write the new RepositoryData unless all shard paths have been successfully updated? See #100569.
                    updateRepositoryData(
                        originalRepositoryData.removeSnapshots(snapshotIds, shardBlobsToDelete.getUpdatedShardGenerations()),
                        l
                    );
                })

                .<RepositoryData>andThen(
                    // writeIndexGen finishes on master-service thread so must fork here.
                    snapshotExecutor,
                    threadPool.getThreadContext(),
                    (l, newRepositoryData) -> {
                        l.onResponse(newRepositoryData);
                        // Once we have updated the repository, run the unreferenced blobs cleanup in parallel to shard-level snapshot
                        // deletion
                        try (var refs = new RefCountingRunnable(onCompletion)) {
                            cleanupUnlinkedRootAndIndicesBlobs(newRepositoryData, refs.acquireListener());
                            cleanupUnlinkedShardLevelBlobs(refs.acquireListener());
                        }
                    }
                )

                .addListener(repositoryDataUpdateListener);
        }

        private void runWithLegacyNumericShardMetadataNaming(
            ActionListener<RepositoryData> repositoryDataUpdateListener,
            Runnable onCompletion
        ) {
            // Write the new repository data first (with the removed snapshot), using no shard generations
            updateRepositoryData(
                originalRepositoryData.removeSnapshots(snapshotIds, ShardGenerations.EMPTY),
                repositoryDataUpdateListener.delegateFailure((delegate, newRepositoryData) -> {
                    try (var refs = new RefCountingRunnable(() -> {
                        delegate.onResponse(newRepositoryData);
                        onCompletion.run();
                    })) {
                        // Run unreferenced blobs cleanup in parallel to shard-level snapshot deletion
                        cleanupUnlinkedRootAndIndicesBlobs(newRepositoryData, refs.acquireListener());

                        // writeIndexGen finishes on master-service thread so must fork here.
                        snapshotExecutor.execute(
                            ActionRunnable.wrap(
                                refs.acquireListener(),
                                l0 -> writeUpdatedShardMetadataAndComputeDeletes(
                                    l0.delegateFailure((l, ignored) -> cleanupUnlinkedShardLevelBlobs(l))
                                )
                            )
                        );
                    }
                })
            );
        }

        void runCleanup(ActionListener<DeleteResult> listener) {
            final Set<String> survivingIndexIds = originalRepositoryData.getIndices()
                .values()
                .stream()
                .map(IndexId::getId)
                .collect(Collectors.toSet());
            final List<String> staleRootBlobs = staleRootBlobs(originalRepositoryData, originalRootBlobs.keySet());
            final var releasingListener = ActionListener.releaseAfter(listener, shardBlobsToDelete);
            if (survivingIndexIds.equals(originalIndexContainers.keySet()) && staleRootBlobs.isEmpty()) {
                // Nothing to clean up we return
                releasingListener.onResponse(DeleteResult.ZERO);
            } else {
                // write new index-N blob to ensure concurrent operations will fail
                updateRepositoryData(
                    originalRepositoryData,
                    releasingListener.delegateFailureAndWrap(
                        // TODO should we pass newRepositoryData to cleanupStaleBlobs()?
                        (l, newRepositoryData) -> cleanupUnlinkedRootAndIndicesBlobs(
                            originalRepositoryData,
                            l.map(ignored -> DeleteResult.of(blobsDeleted.get(), bytesDeleted.get()))
                        )
                    )
                );
            }
        }

        // ---------------------------------------------------------------------------------------------------------------------------------
        // Updating the shard-level metadata and accumulating results

        private void writeUpdatedShardMetadataAndComputeDeletes(ActionListener<Void> listener) {
            // noinspection resource -- closed safely at the end of the iteration
            final var listeners = new RefCountingListener(listener);

            // Each per-index process takes some nonzero amount of working memory to hold the relevant snapshot IDs and metadata generations
            // etc. which we can keep under tighter limits and release sooner if we limit the number of concurrently processing indices.
            // Each one needs at least one snapshot thread at all times, so threadPool.info(SNAPSHOT).getMax() of them at once is enough to
            // keep the threadpool fully utilized.
            ThrottledIterator.run(
                originalRepositoryData.indicesToUpdateAfterRemovingSnapshot(snapshotIds),
                (ref, indexId) -> ActionListener.run(
                    ActionListener.releaseAfter(listeners.acquire(), ref),
                    l -> new IndexSnapshotsDeletion(indexId).run(l)
                ),
                threadPool.info(ThreadPool.Names.SNAPSHOT).getMax(),
                listeners::close
            );
        }

        private class IndexSnapshotsDeletion {
            private final IndexId indexId;
            private final Set<SnapshotId> snapshotsWithIndex;
            private final BlobContainer indexContainer;

            IndexSnapshotsDeletion(IndexId indexId) {
                this.indexId = indexId;
                this.indexContainer = indexContainer(indexId);
                this.snapshotsWithIndex = Set.copyOf(originalRepositoryData.getSnapshots(indexId));
            }

            // Computed by reading the relevant index metadata, which happens-before this value is used to determine the shards to process.
            private int shardCount;

            private synchronized void updateShardCount(int newShardCount) {
                shardCount = Math.max(shardCount, newShardCount);
            }

            void run(ActionListener<Void> listener) {
                determineShardCount(listener.delegateFailureAndWrap((l, v) -> processShards(l)));
            }

            // -----------------------------------------------------------------------------------------------------------------------------
            // Determining the shard count by reading all the relevant index metadata

            private void determineShardCount(ActionListener<Void> listener) {
                try (var listeners = new RefCountingListener(listener)) {
                    for (final var indexMetaGeneration : snapshotIds.stream()
                        .filter(snapshotsWithIndex::contains)
                        .map(id -> originalRepositoryData.indexMetaDataGenerations().indexMetaBlobId(id, indexId))
                        .collect(Collectors.toSet())) {
                        // NB since 7.9.0 we deduplicate index metadata blobs, and one of the components of the deduplication key is the
                        // index UUID; the shard count is going to be the same for all metadata with the same index UUID, so it is
                        // unnecessary to read multiple metadata blobs corresponding to the same index UUID.
                        // TODO Skip this unnecessary work? Maybe track the shard count in RepositoryData?
                        snapshotExecutor.execute(ActionRunnable.run(listeners.acquire(), () -> getOneShardCount(indexMetaGeneration)));
                    }
                }
            }

            private void getOneShardCount(String indexMetaGeneration) {
                try {
                    updateShardCount(
                        INDEX_METADATA_FORMAT.read(metadata.name(), indexContainer, indexMetaGeneration, namedXContentRegistry)
                            .getNumberOfShards()
                    );
                } catch (Exception ex) {
                    logger.warn(() -> format("[%s] [%s] failed to read metadata for index", indexMetaGeneration, indexId.getName()), ex);
                    // Definitely indicates something fairly badly wrong with the repo, but not immediately fatal here: we might get the
                    // shard count from another metadata blob, or we might just not process these shards. If we skip these shards then the
                    // repository will technically enter an invalid state (these shards' index-XXX blobs will refer to snapshots that no
                    // longer exist) and may contain dangling blobs too. A subsequent delete that hits this index may repair the state if
                    // the metadata read error is transient, but if not then the stale indices cleanup will eventually remove this index
                    // and all its extra data anyway.
                    // TODO: Should we fail the delete here? See https://github.com/elastic/elasticsearch/issues/100569.
                }
            }

            private void processShards(ActionListener<Void> listener) {
                final Set<SnapshotId> survivingSnapshots = snapshotsWithIndex.stream()
                    .filter(id -> snapshotIds.contains(id) == false)
                    .collect(Collectors.toSet());
                try (var listeners = new RefCountingListener(listener)) {
                    for (int shardId = 0; shardId < shardCount; shardId++) {
                        snapshotExecutor.execute(new ShardSnapshotsDeletion(shardId, survivingSnapshots, listeners.acquire()));
                    }
                }
            }

            // -----------------------------------------------------------------------------------------------------------------------------
            // Updating each shard

            private class ShardSnapshotsDeletion extends AbstractRunnable {

                private final int shardId;
                private final Set<SnapshotId> survivingSnapshots;
                private final ActionListener<Void> listener;

                // Computed at the start of doRun(), after forking, rather than in the constructor. TODO make these local variables perhaps?
                private BlobContainer shardContainer;
                private Set<String> originalShardBlobs;

                ShardSnapshotsDeletion(int shardId, Set<SnapshotId> survivingSnapshots, ActionListener<Void> listener) {
                    this.shardId = shardId;
                    this.survivingSnapshots = survivingSnapshots;
                    this.listener = listener;
                }

                @Override
                protected void doRun() throws Exception {
                    shardContainer = shardContainer(indexId, shardId);
                    originalShardBlobs = shardContainer.listBlobs(OperationPurpose.SNAPSHOT_DATA).keySet();
                    final BlobStoreIndexShardSnapshots blobStoreIndexShardSnapshots;
                    final long newGen;
                    if (useShardGenerations) {
                        newGen = -1L;
                        blobStoreIndexShardSnapshots = buildBlobStoreIndexShardSnapshots(
                            indexId,
                            shardId,
                            originalShardBlobs,
                            shardContainer,
                            originalRepositoryData.shardGenerations().getShardGen(indexId, shardId)
                        ).v1();
                    } else {
                        Tuple<BlobStoreIndexShardSnapshots, Long> tuple = buildBlobStoreIndexShardSnapshots(
                            originalShardBlobs,
                            shardContainer
                        );
                        newGen = tuple.v2() + 1;
                        blobStoreIndexShardSnapshots = tuple.v1();
                    }
                    deleteFromShardSnapshotMeta(blobStoreIndexShardSnapshots.withRetainedSnapshots(survivingSnapshots), newGen);
                }

                /**
                 * Delete snapshot from shard level metadata.
                 *
                 * @param indexGeneration generation to write the new shard level level metadata to. If negative a uuid id shard generation
                 *                        should be used
                 */
                private void deleteFromShardSnapshotMeta(BlobStoreIndexShardSnapshots updatedSnapshots, long indexGeneration) {
                    ShardGeneration writtenGeneration = null;
                    try {
                        if (updatedSnapshots.snapshots().isEmpty()) {
                            shardBlobsToDelete.addShardDeleteResult(
                                indexId,
                                shardId,
                                ShardGenerations.DELETED_SHARD_GEN,
                                originalShardBlobs
                            );
                        } else {
                            if (indexGeneration < 0L) {
                                writtenGeneration = ShardGeneration.newGeneration();
                                INDEX_SHARD_SNAPSHOTS_FORMAT.write(
                                    updatedSnapshots,
                                    shardContainer,
                                    writtenGeneration.getGenerationUUID(),
                                    compress
                                );
                            } else {
                                writtenGeneration = new ShardGeneration(indexGeneration);
                                writeShardIndexBlobAtomic(shardContainer, indexGeneration, updatedSnapshots, Collections.emptyMap());
                            }
                            final Set<String> survivingSnapshotUUIDs = survivingSnapshots.stream()
                                .map(SnapshotId::getUUID)
                                .collect(Collectors.toSet());
                            shardBlobsToDelete.addShardDeleteResult(
                                indexId,
                                shardId,
                                writtenGeneration,
                                unusedBlobs(originalShardBlobs, survivingSnapshotUUIDs, updatedSnapshots)
                            );
                        }
                    } catch (IOException e) {
                        throw new RepositoryException(
                            metadata.name(),
                            "Failed to finalize snapshot deletion "
                                + snapshotIds
                                + " with shard index ["
                                + INDEX_SHARD_SNAPSHOTS_FORMAT.blobName(writtenGeneration.getGenerationUUID())
                                + "]",
                            e
                        );
                    }
                }

                // Unused blobs are all previous index-, data- and meta-blobs and that are not referenced by the new index- as well as all
                // temporary blobs
                private static List<String> unusedBlobs(
                    Set<String> originalShardBlobs,
                    Set<String> survivingSnapshotUUIDs,
                    BlobStoreIndexShardSnapshots updatedSnapshots
                ) {
                    return originalShardBlobs.stream()
                        .filter(
                            blob -> blob.startsWith(SNAPSHOT_INDEX_PREFIX)
                                || (blob.startsWith(SNAPSHOT_PREFIX)
                                    && blob.endsWith(METADATA_BLOB_NAME_SUFFIX)
                                    && survivingSnapshotUUIDs.contains(
                                        blob.substring(SNAPSHOT_PREFIX.length(), blob.length() - METADATA_BLOB_NAME_SUFFIX.length())
                                    ) == false)
                                || (blob.startsWith(UPLOADED_DATA_BLOB_PREFIX)
                                    && updatedSnapshots.findNameFile(canonicalName(blob)) == null)
                                || FsBlobContainer.isTempBlobName(blob)
                        )
                        .toList();
                }

                @Override
                public void onFailure(Exception ex) {
                    // TODO: Should we fail the delete here? See https://github.com/elastic/elasticsearch/issues/100569.
                    logger.warn(
                        () -> format("%s failed to delete shard data for shard [%s][%s]", snapshotIds, indexId.getName(), shardId),
                        ex
                    );
                }

                @Override
                public void onAfter() {
                    listener.onResponse(null);
                }
            }
        }

        // ---------------------------------------------------------------------------------------------------------------------------------
        // Updating the root RepositoryData blob

        private void updateRepositoryData(RepositoryData newRepositoryData, ActionListener<RepositoryData> listener) {
            writeIndexGen(newRepositoryData, originalRepositoryDataGeneration, repositoryFormatIndexVersion, Function.identity(), listener);
        }

        // ---------------------------------------------------------------------------------------------------------------------------------
        // Cleaning up dangling blobs

        private void cleanupUnlinkedShardLevelBlobs(ActionListener<Void> listener) {
            final Iterator<String> filesToDelete = resolveFilesToDelete();
            if (filesToDelete.hasNext() == false) {
                listener.onResponse(null);
                return;
            }
            snapshotExecutor.execute(ActionRunnable.wrap(listener, l -> {
                try {
                    deleteFromContainer(OperationPurpose.SNAPSHOT_DATA, blobContainer(), filesToDelete);
                    l.onResponse(null);
                } catch (Exception e) {
                    logger.warn(() -> format("%s Failed to delete some blobs during snapshot delete", snapshotIds), e);
                    throw e;
                }
            }));
        }

        private Iterator<String> resolveFilesToDelete() {
            // Somewhat surprisingly we can construct the String representations of the blobs to delete with BlobPath#buildAsString even
            // on Windows, because the JDK translates / to \ automatically (and all other blob stores use / as the path separator anyway)
            final String basePath = basePath().buildAsString();
            final int basePathLen = basePath.length();
            return Iterators.map(Iterators.concat(shardBlobsToDelete.getBlobPaths(), getUnreferencedIndexMetadata()), absolutePath -> {
                assert absolutePath.startsWith(basePath);
                return absolutePath.substring(basePathLen);
            });
        }

        private Iterator<String> getUnreferencedIndexMetadata() {
            return Iterators.flatMap(
                originalRepositoryData.indexMetaDataToRemoveAfterRemovingSnapshots(snapshotIds).entrySet().iterator(),
                entry -> {
                    final String indexContainerPath = indexPath(entry.getKey()).buildAsString();
                    return Iterators.map(entry.getValue().iterator(), id -> indexContainerPath + INDEX_METADATA_FORMAT.blobName(id));
                }
            );
        }

        /**
         * Cleans up stale blobs directly under the repository root as well as all indices paths that aren't referenced by any existing
         * snapshots. This method is only to be called directly after a new {@link RepositoryData} was written to the repository.
         *
         * @param newRepositoryData       new repository data that was just written
         * @param listener                listener to invoke when the deletion is complete
         */
        private void cleanupUnlinkedRootAndIndicesBlobs(RepositoryData newRepositoryData, ActionListener<Void> listener) {
            try (var listeners = new RefCountingListener(listener)) {
                final List<String> staleRootBlobs = staleRootBlobs(newRepositoryData, originalRootBlobs.keySet());
                if (staleRootBlobs.isEmpty() == false) {
                    staleBlobDeleteRunner.enqueueTask(listeners.acquire(ref -> {
                        try (ref) {
                            logStaleRootLevelBlobs(newRepositoryData.getGenId() - 1, snapshotIds, staleRootBlobs);
                            deleteFromContainer(OperationPurpose.SNAPSHOT_METADATA, blobContainer(), staleRootBlobs.iterator());
                            for (final var staleRootBlob : staleRootBlobs) {
                                bytesDeleted.addAndGet(originalRootBlobs.get(staleRootBlob).length());
                            }
                            blobsDeleted.addAndGet(staleRootBlobs.size());
                        } catch (Exception e) {
                            logger.warn(
                                () -> format(
                                    "[%s] The following blobs are no longer part of any snapshot [%s] but failed to remove them",
                                    metadata.name(),
                                    staleRootBlobs
                                ),
                                e
                            );
                        }
                    }));
                }

                final var survivingIndexIds = newRepositoryData.getIndices()
                    .values()
                    .stream()
                    .map(IndexId::getId)
                    .collect(Collectors.toSet());
                for (final var indexEntry : originalIndexContainers.entrySet()) {
                    final var indexId = indexEntry.getKey();
                    if (survivingIndexIds.contains(indexId)) {
                        continue;
                    }
                    staleBlobDeleteRunner.enqueueTask(listeners.acquire(ref -> {
                        try (ref) {
                            logger.debug("[{}] Found stale index [{}]. Cleaning it up", metadata.name(), indexId);
                            final var deleteResult = indexEntry.getValue().delete(OperationPurpose.SNAPSHOT_DATA);
                            blobsDeleted.addAndGet(deleteResult.blobsDeleted());
                            bytesDeleted.addAndGet(deleteResult.bytesDeleted());
                            logger.debug("[{}] Cleaned up stale index [{}]", metadata.name(), indexId);
                        } catch (IOException e) {
                            logger.warn(() -> format("""
                                [%s] index %s is no longer part of any snapshot in the repository, \
                                but failed to clean up its index folder""", metadata.name(), indexId), e);
                        }
                    }));
                }
            }

            // If we did the cleanup of stale indices purely using a throttled executor then there would be no backpressure to prevent us
            // from falling arbitrarily far behind. But nor do we want to dedicate all the SNAPSHOT threads to stale index cleanups because
            // that would slow down other snapshot operations in situations that do not need backpressure.
            //
            // The solution is to dedicate one SNAPSHOT thread to doing the cleanups eagerly, alongside the throttled executor which spreads
            // the rest of the work across the other threads if they are free. If the eager cleanup loop doesn't finish before the next one
            // starts then we dedicate another SNAPSHOT thread to the deletions, and so on, until eventually either we catch up or the
            // SNAPSHOT pool is fully occupied with blob deletions, which pushes back on other snapshot operations.

            staleBlobDeleteRunner.runSyncTasksEagerly(snapshotExecutor);
        }

        // Finds all blobs directly under the repository root path that are not referenced by the current RepositoryData
        private static List<String> staleRootBlobs(RepositoryData newRepositoryData, Set<String> originalRootBlobNames) {
            final Set<String> allSnapshotIds = newRepositoryData.getSnapshotIds()
                .stream()
                .map(SnapshotId::getUUID)
                .collect(Collectors.toSet());
            return originalRootBlobNames.stream().filter(blob -> {
                if (FsBlobContainer.isTempBlobName(blob)) {
                    return true;
                }
                if (blob.endsWith(METADATA_BLOB_NAME_SUFFIX)) {
                    final String foundUUID;
                    if (blob.startsWith(SNAPSHOT_PREFIX)) {
                        foundUUID = blob.substring(SNAPSHOT_PREFIX.length(), blob.length() - METADATA_BLOB_NAME_SUFFIX.length());
                        assert SNAPSHOT_FORMAT.blobName(foundUUID).equals(blob);
                    } else if (blob.startsWith(METADATA_PREFIX)) {
                        foundUUID = blob.substring(METADATA_PREFIX.length(), blob.length() - METADATA_BLOB_NAME_SUFFIX.length());
                        assert GLOBAL_METADATA_FORMAT.blobName(foundUUID).equals(blob);
                    } else {
                        return false;
                    }
                    return allSnapshotIds.contains(foundUUID) == false;
                } else if (blob.startsWith(INDEX_FILE_PREFIX)) {
                    // TODO: Include the current generation here once we remove keeping index-(N-1) around from #writeIndexGen
                    try {
                        return newRepositoryData.getGenId() > Long.parseLong(blob.substring(INDEX_FILE_PREFIX.length()));
                    } catch (NumberFormatException nfe) {
                        // odd case of an extra file with the index- prefix that we can't identify
                        return false;
                    }
                }
                return false;
            }).toList();
        }

        private void logStaleRootLevelBlobs(
            long newestStaleRepositoryDataGeneration,
            Collection<SnapshotId> snapshotIds,
            List<String> blobsToDelete
        ) {
            if (logger.isInfoEnabled()) {
                // If we're running root level cleanup as part of a snapshot delete we should not log the snapshot- and global metadata
                // blobs associated with the just deleted snapshots as they are expected to exist and not stale. Otherwise every snapshot
                // delete would also log a confusing INFO message about "stale blobs".
                final Set<String> blobNamesToIgnore = snapshotIds.stream()
                    .flatMap(
                        snapshotId -> Stream.of(
                            GLOBAL_METADATA_FORMAT.blobName(snapshotId.getUUID()),
                            SNAPSHOT_FORMAT.blobName(snapshotId.getUUID()),
                            getRepositoryDataBlobName(newestStaleRepositoryDataGeneration)
                        )
                    )
                    .collect(Collectors.toSet());
                final List<String> blobsToLog = blobsToDelete.stream().filter(b -> blobNamesToIgnore.contains(b) == false).toList();
                if (blobsToLog.isEmpty() == false) {
                    logger.info("[{}] Found stale root level blobs {}. Cleaning them up", metadata.name(), blobsToLog);
                }
            }
        }
    }

    /**
     * Tracks the shard-level blobs which can be deleted once all the metadata updates have completed during a snapshot deletion.
     */
    class ShardBlobsToDelete implements Releasable {

        /**
         * The result of removing a snapshot from a shard folder in the repository.
         *
         * @param indexId       Repository UUID for index that the snapshot was removed from
         * @param shardId       Shard id that the snapshot was removed from
         * @param blobsToDelete Blob names in the shard directory that have become unreferenced in the new shard generation
         */
        private record ShardSnapshotMetaDeleteResult(String indexId, int shardId, Collection<String> blobsToDelete) {
            ShardSnapshotMetaDeleteResult(StreamInput in) throws IOException {
                this(in.readString(), in.readVInt(), in.readStringCollectionAsImmutableList());
                assert in.getTransportVersion().equals(TransportVersion.current()); // only used in memory on the local node
            }

            void writeTo(StreamOutput out) throws IOException {
                assert out.getTransportVersion().equals(TransportVersion.current()); // only used in memory on the local node
                out.writeString(indexId);
                out.writeVInt(shardId);
                out.writeStringCollection(blobsToDelete);
            }
        }

        /**
         * <p>
         *     Shard-level results, i.e. a sequence of {@link ShardSnapshotMetaDeleteResult} objects, except serialized, concatenated, and
         *     compressed in order to reduce the memory footprint by about 4x when compared with a list of bare objects. This can be GiBs in
         *     size if we're deleting snapshots from a large repository, especially if earlier failures left behind lots of dangling blobs
         *     for some reason.
         * </p>
         * <p>
         *     Writes to this object are all synchronized (via {@link #addShardDeleteResult}), and happen-before it is read, so the reads
         *     need no further synchronization.
         * </p>
         */
        // If the size of this continues to be a problem even after compression, consider either a hard limit on its size (preferring leaked
        // blobs over an OOME on the master) or else offloading it to disk or to the repository itself.
        private final BytesStreamOutput shardDeleteResults = new ReleasableBytesStreamOutput(bigArrays);

        private int resultCount = 0;

        private final StreamOutput compressed = new OutputStreamStreamOutput(
            new BufferedOutputStream(
                new DeflaterOutputStream(Streams.flushOnCloseStream(shardDeleteResults)),
                DeflateCompressor.BUFFER_SIZE
            )
        );

        private final ArrayList<Closeable> resources = new ArrayList<>();

        private final ShardGenerations.Builder shardGenerationsBuilder = ShardGenerations.builder();

        ShardBlobsToDelete() {
            resources.add(compressed);
            resources.add(LeakTracker.wrap((Releasable) shardDeleteResults));
        }

        synchronized void addShardDeleteResult(
            IndexId indexId,
            int shardId,
            ShardGeneration newGeneration,
            Collection<String> blobsToDelete
        ) {
            try {
                shardGenerationsBuilder.put(indexId, shardId, newGeneration);
                new ShardSnapshotMetaDeleteResult(Objects.requireNonNull(indexId.getId()), shardId, blobsToDelete).writeTo(compressed);
                resultCount += 1;
            } catch (IOException e) {
                assert false : e; // no IO actually happens here
                throw new UncheckedIOException(e);
            }
        }

        public ShardGenerations getUpdatedShardGenerations() {
            return shardGenerationsBuilder.build();
        }

        public Iterator<String> getBlobPaths() {
            final StreamInput input;
            try {
                compressed.close();
                input = new InputStreamStreamInput(
                    new BufferedInputStream(
                        new InflaterInputStream(shardDeleteResults.bytes().streamInput()),
                        DeflateCompressor.BUFFER_SIZE
                    )
                );
                resources.add(input);
            } catch (IOException e) {
                assert false : e; // no IO actually happens here
                throw new UncheckedIOException(e);
            }

            return Iterators.flatMap(Iterators.forRange(0, resultCount, i -> {
                try {
                    return new ShardSnapshotMetaDeleteResult(input);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }), shardResult -> {
                final var shardPath = shardPath(new IndexId("_na_", shardResult.indexId), shardResult.shardId).buildAsString();
                return Iterators.map(shardResult.blobsToDelete.iterator(), blob -> shardPath + blob);
            });
        }

        @Override
        public void close() {
            try {
                IOUtils.close(resources);
            } catch (IOException e) {
                assert false : e; // no IO actually happens here
                throw new UncheckedIOException(e);
            }
        }

        // exposed for tests
        int sizeInBytes() {
            return shardDeleteResults.size();
        }
    }

    @Override
    public void finalizeSnapshot(final FinalizeSnapshotContext finalizeSnapshotContext) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SNAPSHOT);
        final long repositoryStateId = finalizeSnapshotContext.repositoryStateId();
        final SnapshotInfo snapshotInfo = finalizeSnapshotContext.snapshotInfo();
        assert repositoryStateId > RepositoryData.UNKNOWN_REPO_GEN
            : "Must finalize based on a valid repository generation but received [" + repositoryStateId + "]";
        final Collection<IndexId> indices = finalizeSnapshotContext.updatedShardGenerations().liveIndices().indices();
        final SnapshotId snapshotId = snapshotInfo.snapshotId();
        // Once we are done writing the updated index-N blob we remove the now unreferenced index-${uuid} blobs in each shard
        // directory if all nodes are at least at version SnapshotsService#SHARD_GEN_IN_REPO_DATA_VERSION
        // If there are older version nodes in the cluster, we don't need to run this cleanup as it will have already happened
        // when writing the index-${N} to each shard directory.
        final IndexVersion repositoryMetaVersion = finalizeSnapshotContext.repositoryMetaVersion();
        final boolean writeShardGens = SnapshotsService.useShardGenerations(repositoryMetaVersion);

        final Executor executor = threadPool.executor(ThreadPool.Names.SNAPSHOT);

        final boolean writeIndexGens = SnapshotsService.useIndexGenerations(repositoryMetaVersion);

        record MetadataWriteResult(
            RepositoryData existingRepositoryData,
            Map<IndexId, String> indexMetas,
            Map<String, String> indexMetaIdentifiers
        ) {}

        record RootBlobUpdateResult(RepositoryData oldRepositoryData, RepositoryData newRepositoryData) {}

        SubscribableListener

            // Get the current RepositoryData
            .<RepositoryData>newForked(listener -> getRepositoryData(executor, listener))

            // Identify and write the missing metadata
            .<MetadataWriteResult>andThen((l, existingRepositoryData) -> {
                final int existingSnapshotCount = existingRepositoryData.getSnapshotIds().size();
                if (existingSnapshotCount >= maxSnapshotCount) {
                    throw new RepositoryException(
                        metadata.name(),
                        "Cannot add another snapshot to this repository as it already contains ["
                            + existingSnapshotCount
                            + "] snapshots and is configured to hold up to ["
                            + maxSnapshotCount
                            + "] snapshots only."
                    );
                }

                final MetadataWriteResult metadataWriteResult;
                if (writeIndexGens) {
                    metadataWriteResult = new MetadataWriteResult(
                        existingRepositoryData,
                        ConcurrentCollections.newConcurrentMap(),
                        ConcurrentCollections.newConcurrentMap()
                    );
                } else {
                    metadataWriteResult = new MetadataWriteResult(existingRepositoryData, null, null);
                }

                try (var allMetaListeners = new RefCountingListener(l.map(ignored -> metadataWriteResult))) {
                    // We ignore all FileAlreadyExistsException when writing metadata since otherwise a master failover while in this method
                    // will mean that no snap-${uuid}.dat blob is ever written for this snapshot. This is safe because any updated version
                    // of the index or global metadata will be compatible with the segments written in this snapshot as well.
                    // Failing on an already existing index-${repoGeneration} below ensures that the index.latest blob is not updated in a
                    // way that decrements the generation it points at

                    // Write global metadata
                    final Metadata clusterMetadata = finalizeSnapshotContext.clusterMetadata();
                    executor.execute(
                        ActionRunnable.run(
                            allMetaListeners.acquire(),
                            () -> GLOBAL_METADATA_FORMAT.write(clusterMetadata, blobContainer(), snapshotId.getUUID(), compress)
                        )
                    );

                    // Write the index metadata for each index in the snapshot
                    for (IndexId index : indices) {
                        executor.execute(ActionRunnable.run(allMetaListeners.acquire(), () -> {
                            final IndexMetadata indexMetaData = clusterMetadata.getProject().index(index.getName());
                            if (writeIndexGens) {
                                final String identifiers = IndexMetaDataGenerations.buildUniqueIdentifier(indexMetaData);
                                String metaUUID = existingRepositoryData.indexMetaDataGenerations().getIndexMetaBlobId(identifiers);
                                if (metaUUID == null) {
                                    // We don't yet have this version of the metadata so we write it
                                    metaUUID = UUIDs.base64UUID();
                                    INDEX_METADATA_FORMAT.write(indexMetaData, indexContainer(index), metaUUID, compress);
                                    metadataWriteResult.indexMetaIdentifiers().put(identifiers, metaUUID);
                                } // else this task was largely a no-op - TODO no need to fork in that case
                                metadataWriteResult.indexMetas().put(index, identifiers);
                            } else {
                                INDEX_METADATA_FORMAT.write(
                                    clusterMetadata.getProject().index(index.getName()),
                                    indexContainer(index),
                                    snapshotId.getUUID(),
                                    compress
                                );
                            }
                        }));
                    }

                    // Write the SnapshotInfo blob to the repo (we're already on a SNAPSHOT thread so no need to fork this)
                    assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SNAPSHOT);
                    ActionListener.completeWith(allMetaListeners.acquire(), () -> {
                        SNAPSHOT_FORMAT.write(snapshotInfo, blobContainer(), snapshotId.getUUID(), compress);
                        return null;
                    });

                    // TODO fail fast if any metadata write fails
                    // TODO clean up successful metadata writes on failure (needs care, we must not clobber another node concurrently
                    // finalizing the same snapshot: we can only clean up after removing the failed snapshot from the cluster state)
                }
            })

            // Update the root blob
            .<RootBlobUpdateResult>andThen((l, metadataWriteResult) -> {
                assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.SNAPSHOT);
                final var snapshotDetails = SnapshotDetails.fromSnapshotInfo(snapshotInfo);
                final var existingRepositoryData = metadataWriteResult.existingRepositoryData();
                writeIndexGen(
                    existingRepositoryData.addSnapshot(
                        snapshotId,
                        snapshotDetails,
                        finalizeSnapshotContext.updatedShardGenerations(),
                        metadataWriteResult.indexMetas(),
                        metadataWriteResult.indexMetaIdentifiers()
                    ),
                    repositoryStateId,
                    repositoryMetaVersion,
                    new Function<>() {
                        @Override
                        public ClusterState apply(ClusterState state) {
                            return finalizeSnapshotContext.updatedClusterState(state);
                        }

                        @Override
                        public String toString() {
                            return "finalizing snapshot [" + metadata.name() + "][" + snapshotId + "]";
                        }
                    },
                    l.map(newRepositoryData -> new RootBlobUpdateResult(existingRepositoryData, newRepositoryData))
                );
                // NB failure of writeIndexGen doesn't guarantee the update failed, so we cannot safely clean anything up on failure
            })

            // Report success, then clean up.
            .<RepositoryData>andThen((l, rootBlobUpdateResult) -> {
                l.onResponse(rootBlobUpdateResult.newRepositoryData());
                cleanupOldMetadata(
                    rootBlobUpdateResult.oldRepositoryData(),
                    rootBlobUpdateResult.newRepositoryData(),
                    finalizeSnapshotContext,
                    writeShardGens
                );
            })

            // Finally subscribe the context as the listener, wrapping exceptions if needed
            .addListener(
                finalizeSnapshotContext.delegateResponse(
                    (l, e) -> l.onFailure(new SnapshotException(metadata.name(), snapshotId, "failed to update snapshot in repository", e))
                )
            );
    }

    // Delete all old shard gen and root level index blobs that aren't referenced any longer as a result from moving to updated
    // repository data
    private void cleanupOldMetadata(
        RepositoryData existingRepositoryData,
        RepositoryData updatedRepositoryData,
        FinalizeSnapshotContext finalizeSnapshotContext,
        boolean writeShardGenerations
    ) {
        final Set<String> toDelete = new HashSet<>();
        // Delete all now outdated index files up to 1000 blobs back from the new generation.
        // If there are more than 1000 dangling index-N cleanup functionality on repo delete will take care of them.
        long newRepoGeneration = updatedRepositoryData.getGenId();
        for (long gen = Math.max(
            Math.max(existingRepositoryData.getGenId() - 1, 0),
            newRepoGeneration - 1000
        ); gen < newRepoGeneration; gen++) {
            toDelete.add(getRepositoryDataBlobName(gen));
        }
        if (writeShardGenerations) {
            final int prefixPathLen = basePath().buildAsString().length();
            updatedRepositoryData.shardGenerations()
                .obsoleteShardGenerations(existingRepositoryData.shardGenerations())
                .forEach(
                    (indexId, gens) -> gens.forEach(
                        (shardId, oldGen) -> toDelete.add(
                            shardPath(indexId, shardId).buildAsString().substring(prefixPathLen) + BlobStoreRepository.SNAPSHOT_INDEX_PREFIX
                                + oldGen.getGenerationUUID()
                        )
                    )
                );
            for (Map.Entry<RepositoryShardId, Set<ShardGeneration>> obsoleteEntry : finalizeSnapshotContext.obsoleteShardGenerations()
                .entrySet()) {
                final String containerPath = shardPath(obsoleteEntry.getKey().index(), obsoleteEntry.getKey().shardId()).buildAsString()
                    .substring(prefixPathLen) + SNAPSHOT_INDEX_PREFIX;
                for (ShardGeneration shardGeneration : obsoleteEntry.getValue()) {
                    toDelete.add(containerPath + shardGeneration);
                }
            }
        }

        if (toDelete.isEmpty() == false) {
            threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new AbstractRunnable() {
                @Override
                protected void doRun() throws Exception {
                    deleteFromContainer(OperationPurpose.SNAPSHOT_METADATA, blobContainer(), toDelete.iterator());
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("Failed to clean up old metadata blobs", e);
                }

                @Override
                public void onAfter() {
                    finalizeSnapshotContext.onDone();
                }
            });
        } else {
            finalizeSnapshotContext.onDone();
        }
    }

    @Override
    public void getSnapshotInfo(
        Collection<SnapshotId> snapshotIds,
        boolean abortOnFailure,
        BooleanSupplier isCancelled,
        CheckedConsumer<SnapshotInfo, Exception> consumer,
        ActionListener<Void> listener
    ) {
        final var context = new GetSnapshotInfoContext(snapshotIds, abortOnFailure, isCancelled, (ctx, sni) -> {
            try {
                consumer.accept(sni);
            } catch (Exception e) {
                ctx.onFailure(e);
            }
        }, listener);
        // put snapshot info downloads into a task queue instead of pushing them all into the queue to not completely monopolize the
        // snapshot meta pool for a single request
        final int workers = Math.min(threadPool.info(ThreadPool.Names.SNAPSHOT_META).getMax(), context.snapshotIds().size());
        final BlockingQueue<SnapshotId> queue = new LinkedBlockingQueue<>(context.snapshotIds());
        for (int i = 0; i < workers; i++) {
            getOneSnapshotInfo(queue, context);
        }
    }

    /**
     * Tries to poll a {@link SnapshotId} to load {@link SnapshotInfo} from the given {@code queue}.
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

    private void deleteFromContainer(OperationPurpose purpose, BlobContainer container, Iterator<String> blobs) throws IOException {
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
        container.deleteBlobsIgnoringIfNotExists(purpose, wrappedIterator);
    }

    private BlobPath indicesPath() {
        return basePath().add("indices");
    }

    private BlobContainer indexContainer(IndexId indexId) {
        return blobStore().blobContainer(indexPath(indexId));
    }

    private BlobContainer shardContainer(IndexId indexId, ShardId shardId) {
        return shardContainer(indexId, shardId.getId());
    }

    private BlobPath indexPath(IndexId indexId) {
        return indicesPath().add(indexId.getId());
    }

    private BlobPath shardPath(IndexId indexId, int shardId) {
        return indexPath(indexId).add(Integer.toString(shardId));
    }

    public BlobContainer shardContainer(IndexId indexId, int shardId) {
        return blobStore().blobContainer(shardPath(indexId, shardId));
    }

    /**
     * Configures RateLimiter based on repository and global settings
     *
     * @param rateLimiter              the existing rate limiter to configure (or null if no throttling was previously needed)
     * @param maxConfiguredBytesPerSec the configured max bytes per sec from the settings
     * @param settingKey               setting used to configure the rate limiter
     * @param warnIfOverRecovery       log a warning if rate limit setting is over the effective recovery rate limit
     * @return the newly configured rate limiter or null if no throttling is needed
     */
    private RateLimiter getRateLimiter(
        RateLimiter rateLimiter,
        ByteSizeValue maxConfiguredBytesPerSec,
        String settingKey,
        boolean warnIfOverRecovery
    ) {
        if (maxConfiguredBytesPerSec.getBytes() <= 0) {
            return null;
        } else {
            ByteSizeValue effectiveRecoverySpeed = recoverySettings.getMaxBytesPerSec();
            if (warnIfOverRecovery && effectiveRecoverySpeed.getBytes() > 0) {
                if (maxConfiguredBytesPerSec.getBytes() > effectiveRecoverySpeed.getBytes()) {
                    logger.warn(
                        "repository [{}] has a rate limit [{}={}] per second which is above the effective recovery rate limit "
                            + "[{}={}] per second, thus the repository rate limit will be superseded by the recovery rate limit",
                        metadata.name(),
                        settingKey,
                        maxConfiguredBytesPerSec,
                        INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(),
                        effectiveRecoverySpeed
                    );
                }
            }

            if (rateLimiter != null) {
                rateLimiter.setMBPerSec(maxConfiguredBytesPerSec.getMbFrac());
                return rateLimiter;
            } else {
                return new RateLimiter.SimpleRateLimiter(maxConfiguredBytesPerSec.getMbFrac());
            }
        }
    }

    // package private for testing
    RateLimiter getSnapshotRateLimiter() {
        Settings repositorySettings = metadata.settings();
        ByteSizeValue maxConfiguredBytesPerSec = MAX_SNAPSHOT_BYTES_PER_SEC.get(repositorySettings);
        if (MAX_SNAPSHOT_BYTES_PER_SEC.exists(repositorySettings) == false && recoverySettings.nodeBandwidthSettingsExist()) {
            assert maxConfiguredBytesPerSec.getMb() == 40;
            maxConfiguredBytesPerSec = ByteSizeValue.ZERO;
        }
        return getRateLimiter(
            snapshotRateLimiter,
            maxConfiguredBytesPerSec,
            MAX_SNAPSHOT_BYTES_PER_SEC.getKey(),
            recoverySettings.nodeBandwidthSettingsExist()
        );
    }

    // package private for testing
    RateLimiter getRestoreRateLimiter() {
        return getRateLimiter(
            restoreRateLimiter,
            MAX_RESTORE_BYTES_PER_SEC.get(metadata.settings()),
            MAX_RESTORE_BYTES_PER_SEC.getKey(),
            true
        );
    }

    @Override
    public long getSnapshotThrottleTimeInNanos() {
        return snapshotRateLimitingTimeInNanos.count();
    }

    @Override
    public long getRestoreThrottleTimeInNanos() {
        return restoreRateLimitingTimeInNanos.count();
    }

    private void assertSnapshotOrStatelessPermittedThreadPool() {
        // The Stateless plugin adds custom thread pools for object store operations
        assert ThreadPool.assertCurrentThreadPool(
            ThreadPool.Names.SNAPSHOT,
            ThreadPool.Names.SNAPSHOT_META,
            ThreadPool.Names.GENERIC,
            STATELESS_SHARD_READ_THREAD_NAME,
            STATELESS_TRANSLOG_THREAD_NAME,
            STATELESS_SHARD_WRITE_THREAD_NAME,
            STATELESS_CLUSTER_STATE_READ_WRITE_THREAD_NAME,
            STATELESS_SHARD_PREWARMING_THREAD_NAME,
            STATELESS_SHARD_UPLOAD_PREWARMING_THREAD_NAME,
            SEARCHABLE_SNAPSHOTS_CACHE_FETCH_ASYNC_THREAD_NAME,
            SEARCHABLE_SNAPSHOTS_CACHE_PREWARMING_THREAD_NAME
        );
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
                testContainer.writeBlobAtomic(OperationPurpose.SNAPSHOT_METADATA, "master.dat", new BytesArray(testBytes), true);
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
                blobStore().blobContainer(basePath().add(testPrefix)).delete(OperationPurpose.SNAPSHOT_METADATA);
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
    public void getRepositoryData(Executor responseExecutor, ActionListener<RepositoryData> listener) {
        // RepositoryData is the responsibility of the elected master: we shouldn't be loading it on other nodes as we don't have good
        // consistency guarantees there, but electedness is too ephemeral to assert. We can say for sure that this node should be
        // master-eligible, which is almost as strong since all other snapshot-related activity happens on data nodes whether they be
        // master-eligible or not.
        assert clusterService.localNode().isMasterNode() : "should only load repository data on master nodes";

        while (true) {
            // retry loop, in case the state changes underneath us somehow

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
                listener.onResponse(cached);
                return;
            }
            if (metadata.generation() == RepositoryData.UNKNOWN_REPO_GEN && isReadOnly() == false) {
                logger.debug("""
                    [{}] loading repository metadata for the first time, trying to determine correct generation and to store it in the \
                    cluster state""", metadata.name());
                if (initializeRepoGenerationTracking(responseExecutor, listener)) {
                    return;
                } // else there was a concurrent modification, retry from the start
            } else {
                logger.trace(
                    "[{}] loading un-cached repository data with best known repository generation [{}]",
                    metadata.name(),
                    latestKnownRepoGen
                );
                repoDataLoadDeduplicator.execute(new ThreadedActionListener<>(responseExecutor, listener));
                return;
            }
        }
    }

    private RepositoryException notStartedException() {
        return new RepositoryException(metadata.name(), "repository is not in started state");
    }

    // Listener used to ensure that repository data is only initialized once in the cluster state by #initializeRepoGenerationTracking
    @Nullable // unless we're in the process of initializing repo-generation tracking
    private SubscribableListener<RepositoryData> repoDataInitialized;

    /**
     * Method used to set the current repository generation in the cluster state's {@link RepositoryMetadata} to the latest generation that
     * can be physically found in the repository before passing the latest {@link RepositoryData} to the given listener.
     * This ensures that operations using {@code SnapshotsService#executeConsistentStateUpdate} right after mounting a fresh repository will
     * have a consistent view of the {@link RepositoryData} before any data has been written to the repository.
     *
     * @param responseExecutor executor to use to complete the listener if not completing it on the calling thread
     * @param listener         listener to resolve with new repository data
     * @return {@code true} if this method at least started the initialization process successfully and will eventually complete the
     * listener, {@code false} if there was some concurrent state change which prevents us from starting repo generation tracking (typically
     * that some other node got there first) and the caller should check again and possibly retry or complete the listener in some other
     * way.
     */
    private boolean initializeRepoGenerationTracking(Executor responseExecutor, ActionListener<RepositoryData> listener) {
        final SubscribableListener<RepositoryData> listenerToSubscribe;
        final ActionListener<RepositoryData> listenerToComplete;

        synchronized (this) {
            if (repoDataInitialized == null) {
                // double-check the generation since we checked it outside the mutex in the caller and it could have changed by a
                // concurrent initialization of the repo metadata and just load repository normally in case we already finished the
                // initialization
                if (metadata.generation() != RepositoryData.UNKNOWN_REPO_GEN) {
                    return false; // retry
                }
                logger.trace("[{}] initializing repository generation in cluster state", metadata.name());
                repoDataInitialized = listenerToSubscribe = new SubscribableListener<>();
                listenerToComplete = new ActionListener<>() {
                    private ActionListener<RepositoryData> acquireAndClearRepoDataInitialized() {
                        synchronized (BlobStoreRepository.this) {
                            assert repoDataInitialized == listenerToSubscribe;
                            repoDataInitialized = null;
                            return listenerToSubscribe;
                        }
                    }

                    @Override
                    public void onResponse(RepositoryData repositoryData) {
                        acquireAndClearRepoDataInitialized().onResponse(repositoryData);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn(
                            () -> format("[%s] Exception when initializing repository generation in cluster state", metadata.name()),
                            e
                        );
                        acquireAndClearRepoDataInitialized().onFailure(e);
                    }
                };
            } else {
                logger.trace(
                    "[{}] waiting for existing initialization of repository metadata generation in cluster state",
                    metadata.name()
                );
                listenerToComplete = null;
                listenerToSubscribe = repoDataInitialized;
            }
        }

        if (listenerToComplete != null) {
            SubscribableListener
                // load the current repository data
                .newForked(repoDataLoadDeduplicator::execute)
                // write its generation to the cluster state
                .<RepositoryData>andThen(
                    (l, repoData) -> submitUnbatchedTask(
                        "set initial safe repository generation [" + metadata.name() + "][" + repoData.getGenId() + "]",
                        new ClusterStateUpdateTask() {
                            @Override
                            public ClusterState execute(ClusterState currentState) {
                                return getClusterStateWithUpdatedRepositoryGeneration(currentState, repoData);
                            }

                            @Override
                            public void onFailure(Exception e) {
                                l.onFailure(e);
                            }

                            @Override
                            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                                l.onResponse(repoData);
                            }
                        }
                    )
                )
                // fork to generic pool since we're on the applier thread and some callbacks for repository data do additional IO
                .<RepositoryData>andThen((l, repoData) -> {
                    logger.trace("[{}] initialized repository generation in cluster state to [{}]", metadata.name(), repoData.getGenId());
                    threadPool.generic().execute(ActionRunnable.supply(ActionListener.runAfter(l, () -> {
                        logger.trace(
                            "[{}] called listeners after initializing repository to generation [{}]",
                            metadata.name(),
                            repoData.getGenId()
                        );
                    }), () -> repoData));
                })
                // and finally complete the listener
                .addListener(listenerToComplete);
        }

        listenerToSubscribe.addListener(listener, responseExecutor, threadPool.getThreadContext());
        return true;
    }

    private ClusterState getClusterStateWithUpdatedRepositoryGeneration(ClusterState currentState, RepositoryData repoData) {
        // In theory we might have failed over to a different master which initialized the repo and then failed back to this node, so we
        // must check the repository generation in the cluster state is still unknown here.
        final RepositoryMetadata repoMetadata = getRepoMetadata(currentState);
        if (repoMetadata.generation() != RepositoryData.UNKNOWN_REPO_GEN) {
            throw new RepositoryException(repoMetadata.name(), "Found unexpected initialized repo metadata [" + repoMetadata + "]");
        }
        return ClusterState.builder(currentState)
            .metadata(
                Metadata.builder(currentState.getMetadata())
                    .putDefaultProjectCustom(
                        RepositoriesMetadata.TYPE,
                        RepositoriesMetadata.get(currentState)
                            .withUpdatedGeneration(repoMetadata.name(), repoData.getGenId(), repoData.getGenId())
                    )
            )
            .build();
    }

    /**
     * Deduplicator that deduplicates the physical loading of {@link RepositoryData} from the repositories' underlying storage.
     */
    private final SingleResultDeduplicator<RepositoryData> repoDataLoadDeduplicator;

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
                genToLoad = latestKnownRepoGen.accumulateAndGet(generation, Math::max);
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
                        cacheRepositoryData(loaded, IndexVersion.current());
                    }
                    if (loaded.getUuid().equals(metadata.uuid())) {
                        listener.onResponse(loaded);
                    } else {
                        // someone switched the repo contents out from under us
                        RepositoriesService.updateRepositoryUuidInMetadata(
                            clusterService,
                            metadata.name(),
                            loaded,
                            new ThreadedActionListener<>(threadPool.generic(), listener.map(v -> loaded))
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
                        listener.delegateFailureAndWrap((l, v) -> l.onFailure(corruptedStateException(e, finalLastInfo)))
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
    private void cacheRepositoryData(RepositoryData repositoryData, IndexVersion version) {
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
                    + version.toReleaseVersion()
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
        return new RepositoryException(metadata.name(), Strings.format("""
            The repository has been disabled to prevent data corruption because its contents were found not to match its expected state. \
            This is either because something other than this cluster modified the repository contents, or because the repository's \
            underlying storage behaves incorrectly. To re-enable this repository, first ensure that this cluster has exclusive write \
            access to it, and then re-register the repository with this cluster. See %s for further information.\
            %s""", ReferenceDocs.CONCURRENT_REPOSITORY_WRITERS, previousWriterMessage(previousWriterInfo)), cause);
    }

    private static String previousWriterMessage(@Nullable Tuple<Long, String> previousWriterInfo) {
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
        logger.warn(() -> "Marking repository [" + metadata.name() + "] as corrupted", originalException);
        submitUnbatchedTask(
            "mark repository corrupted [" + metadata.name() + "][" + corruptedGeneration + "]",
            new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    final RepositoriesMetadata state = RepositoriesMetadata.get(currentState);
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
                public void onFailure(Exception e) {
                    listener.onFailure(
                        new RepositoryException(
                            metadata.name(),
                            "Failed marking repository state as corrupted",
                            ExceptionsHelper.useOrSuppress(e, originalException)
                        )
                    );
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
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
            final var repositoryDataBlobName = getRepositoryDataBlobName(indexGen);

            // EMPTY is safe here because RepositoryData#fromXContent calls namedObject
            try (
                InputStream blob = blobContainer().readBlob(OperationPurpose.SNAPSHOT_METADATA, repositoryDataBlobName);
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
     * Writing a new index generation (root) blob is a three-step process. Typically, it starts from a stable state where the pending
     * generation {@link RepositoryMetadata#pendingGeneration()} is equal to the safe generation {@link RepositoryMetadata#generation()},
     * but after a failure it may be that the pending generation starts out greater than the safe generation.
     * <ol>
     * <li>
     * We reserve ourselves a new root blob generation {@code G}, greater than {@link RepositoryMetadata#pendingGeneration()}, via a
     * cluster state update which edits the {@link RepositoryMetadata} entry for this repository, increasing its pending generation to
     * {@code G} without changing its safe generation.
     * <li>
     * We write the updated {@link RepositoryData} to a new root blob with generation {@code G}.
     * <li>
     * We mark the successful end of the update of the repository data with a cluster state update which edits the
     * {@link RepositoryMetadata} entry for this repository again, increasing its safe generation to equal to its pending generation
     * {@code G}.
     * </ol>
     * We use this process to protect against problems such as a master failover part-way through. If a new master is elected while we're
     * writing the root blob with generation {@code G} then we will fail to update the safe repository generation in the final step, and
     * meanwhile the new master will choose a generation greater than {@code G} for all subsequent root blobs so there is no risk that we
     * will clobber its writes. See the package level documentation for {@link org.elasticsearch.repositories.blobstore} for more details.
     * <p>
     * Note that a failure here does not imply that the process was unsuccessful or the repository is unchanged. Once we have written the
     * new root blob the repository is updated from the point of view of any other clusters reading from it, and if we performed a full
     * cluster restart at that point then we would also pick up the new root blob. Writing the root blob may succeed without us receiving
     * a successful response from the repository, leading us to report that the write operation failed. Updating the safe generation may
     * likewise succeed on a majority of master-eligible nodes which does not include this one, again leading to an apparent failure.
     * <p>
     * We therefore cannot safely clean up apparently-dangling blobs after a failure here. Instead, we defer any cleanup until after the
     * next successful root-blob write, which may happen on a different master node or possibly even in a different cluster.
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
        IndexVersion version,
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
        final ListenableFuture<Long> setPendingStep = new ListenableFuture<>();
        final String setPendingGenerationSource = "set pending repository generation [" + metadata.name() + "][" + expectedGen + "]";
        submitUnbatchedTask(setPendingGenerationSource, new ClusterStateUpdateTask() {

            private long newGen;

            @Override
            public ClusterState execute(ClusterState currentState) {
                final RepositoryMetadata meta = getRepoMetadata(currentState);
                final String repoName = metadata.name();

                if (RepositoriesService.isReadOnly(meta.settings())) {
                    // Last resort check: we shouldn't have been able to mark the repository as readonly while the operation that led to
                    // this writeIndexGen() call was in progress, and conversely shouldn't have started any such operation if the repo
                    // was already readonly, but these invariants are not obviously true and it is disastrous to proceed here.
                    throw new RepositoryException(meta.name(), "repository is readonly, cannot update root blob");
                }

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
                                RepositoriesMetadata.get(currentState).withUpdatedGeneration(repoName, safeGeneration, newGen)
                            )
                            .build()
                    )
                    .build();
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(
                    new RepositoryException(
                        metadata.name(),
                        "Failed to execute cluster state update [" + setPendingGenerationSource + "]",
                        e
                    )
                );
            }

            @Override
            public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                logger.trace("[{}] successfully set pending repository generation to [{}]", metadata.name(), newGen);
                setPendingStep.onResponse(newGen);
            }

            @Override
            public String toString() {
                return Strings.format("start RepositoryData update from generation [%d], stateFilter=[%s]", expectedGen, stateFilter);
            }
        });

        final ListenableFuture<RepositoryData> filterRepositoryDataStep = new ListenableFuture<>();

        // Step 2: Write new index-N blob to repository and update index.latest
        setPendingStep.addListener(
            listener.delegateFailureAndWrap(
                (delegate, newGen) -> threadPool().executor(ThreadPool.Names.SNAPSHOT).execute(ActionRunnable.wrap(delegate, l -> {
                    // BwC logic: Load snapshot version information if any snapshot is missing details in RepositoryData so that the new
                    // RepositoryData contains full details for every snapshot
                    final List<SnapshotId> snapshotIdsWithMissingDetails = repositoryData.getSnapshotIds()
                        .stream()
                        .filter(repositoryData::hasMissingDetails)
                        .toList();
                    if (snapshotIdsWithMissingDetails.isEmpty() == false) {
                        final Map<SnapshotId, SnapshotDetails> extraDetailsMap = new ConcurrentHashMap<>();
                        getSnapshotInfo(
                            snapshotIdsWithMissingDetails,
                            false,
                            () -> false,
                            snapshotInfo -> extraDetailsMap.put(snapshotInfo.snapshotId(), SnapshotDetails.fromSnapshotInfo(snapshotInfo)),
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
                        );
                    } else {
                        filterRepositoryDataStep.onResponse(repositoryData);
                    }
                }))
            )
        );
        filterRepositoryDataStep.addListener(listener.delegateFailureAndWrap((delegate, filteredRepositoryData) -> {
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
            if (ensureSafeGenerationExists(expectedGen, delegate::onFailure) == false) {
                return;
            }
            final String indexBlob = getRepositoryDataBlobName(newGen);
            logger.debug("Repository [{}] writing new index generational blob [{}]", metadata.name(), indexBlob);
            writeAtomic(OperationPurpose.SNAPSHOT_METADATA, blobContainer(), indexBlob, out -> {
                try (XContentBuilder xContentBuilder = XContentFactory.jsonBuilder(org.elasticsearch.core.Streams.noCloseStream(out))) {
                    newRepositoryData.snapshotsToXContent(xContentBuilder, version);
                }
            }, true);
            maybeWriteIndexLatest(newGen);

            if (filteredRepositoryData.getUuid().equals(RepositoryData.MISSING_UUID) && SnapshotsService.includesUUIDs(version)) {
                assert newRepositoryData.getUuid().equals(RepositoryData.MISSING_UUID) == false;
                logger.info(
                    Strings.format(
                        "Generated new repository UUID [%s] for repository [%s] in generation [%d]",
                        newRepositoryData.getUuid(),
                        metadata.name(),
                        newGen
                    )
                );
            } else {
                // repo UUID is not new
                assert filteredRepositoryData.getUuid().equals(newRepositoryData.getUuid())
                    : filteredRepositoryData.getUuid() + " vs " + newRepositoryData.getUuid();
            }

            // Step 3: Update CS to reflect new repository generation.
            final String setSafeGenerationSource = "set safe repository generation [" + metadata.name() + "][" + newGen + "]";
            submitUnbatchedTask(setSafeGenerationSource, new ClusterStateUpdateTask() {
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
                    final RepositoriesMetadata withGenerations = RepositoriesMetadata.get(currentState)
                        .withUpdatedGeneration(metadata.name(), newGen, newGen);
                    final RepositoriesMetadata withUuid = meta.uuid().equals(newRepositoryData.getUuid())
                        ? withGenerations
                        : withGenerations.withUuid(metadata.name(), newRepositoryData.getUuid());
                    final ClusterState newClusterState = stateFilter.apply(
                        ClusterState.builder(currentState)
                            .metadata(
                                Metadata.builder(currentState.getMetadata()).putDefaultProjectCustom(RepositoriesMetadata.TYPE, withUuid)
                            )
                            .build()
                    );
                    return updateRepositoryGenerationsIfNecessary(newClusterState, expectedGen, newGen);
                }

                @Override
                public void onFailure(Exception e) {
                    delegate.onFailure(
                        new RepositoryException(
                            metadata.name(),
                            "Failed to execute cluster state update [" + setSafeGenerationSource + "]",
                            e
                        )
                    );
                }

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    logger.trace("[{}] successfully set safe repository generation to [{}]", metadata.name(), newGen);
                    cacheRepositoryData(newRepositoryData, version);
                    delegate.onResponse(newRepositoryData);
                }

                @Override
                public String toString() {
                    return Strings.format(
                        "complete RepositoryData update from generation [%d] to generation [%d], stateFilter=[%s]",
                        expectedGen,
                        newGen,
                        stateFilter
                    );
                }
            });
        }));
    }

    private RepositoryData updateRepositoryData(RepositoryData repositoryData, IndexVersion repositoryMetaversion, long newGen) {
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
                writeAtomic(
                    OperationPurpose.SNAPSHOT_METADATA,
                    blobContainer(),
                    INDEX_LATEST_BLOB,
                    out -> out.write(Numbers.longToBytes(newGen)),
                    false
                );
            } catch (Exception e) {
                logger.warn(
                    () -> format(
                        "Failed to write index.latest blob. If you do not intend to use this "
                            + "repository as the basis for a URL repository you may turn off attempting to write the index.latest blob by "
                            + "setting repository setting [%s] to [false]",
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
        if (safeGeneration != RepositoryData.EMPTY_REPO_GEN
            && blobContainer().blobExists(SNAPSHOT_METADATA, getRepositoryDataBlobName(safeGeneration)) == false) {
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
        final SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.get(state);
        for (SnapshotsInProgress.Entry entry : snapshotsInProgress.forRepo(repoName)) {
            if (entry.repositoryStateId() == oldGen) {
                snapshotEntries.add(entry.withRepoGen(newGen));
                changedSnapshots = true;
            } else {
                snapshotEntries.add(entry);
            }
        }
        updatedSnapshotsInProgress = changedSnapshots
            ? snapshotsInProgress.createCopyWithUpdatedEntriesForRepo(repoName, snapshotEntries)
            : null;
        final SnapshotDeletionsInProgress updatedDeletionsInProgress;
        boolean changedDeletions = false;
        final List<SnapshotDeletionsInProgress.Entry> deletionEntries = new ArrayList<>();
        for (SnapshotDeletionsInProgress.Entry entry : SnapshotDeletionsInProgress.get(state).getEntries()) {
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
        final RepositoryMetadata repositoryMetadata = RepositoriesMetadata.get(state).repository(metadata.name());
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
        final BytesReference content = Streams.readFully(
            Streams.limitStream(blobContainer().readBlob(OperationPurpose.SNAPSHOT_METADATA, INDEX_LATEST_BLOB), Long.BYTES + 1)
        );
        if (content.length() != Long.BYTES) {
            throw new RepositoryException(
                metadata.name(),
                "exception reading blob ["
                    + INDEX_LATEST_BLOB
                    + "]: expected 8 bytes but blob was "
                    + (content.length() < Long.BYTES ? content.length() + " bytes" : "longer")
            );
        }
        return Numbers.bytesToLong(content.toBytesRef());
    }

    private long listBlobsToGetLatestIndexId() throws IOException {
        return latestGeneration(blobContainer().listBlobsByPrefix(OperationPurpose.SNAPSHOT_METADATA, INDEX_FILE_PREFIX).keySet());
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
        OperationPurpose purpose,
        BlobContainer container,
        final String blobName,
        CheckedConsumer<OutputStream, IOException> writer,
        boolean failIfAlreadyExists
    ) throws IOException {
        logger.trace(() -> format("[%s] Writing [%s] to %s atomically", metadata.name(), blobName, container.path()));
        container.writeMetadataBlob(purpose, blobName, failIfAlreadyExists, true, writer);
    }

    @Override
    public void snapshotShard(SnapshotShardContext context) {
        context.status().updateStatusDescription("queued in snapshot task runner");
        shardSnapshotTaskRunner.enqueueShardSnapshot(context);
    }

    private void doSnapshotShard(SnapshotShardContext context) {
        if (isReadOnly()) {
            context.onFailure(new RepositoryException(metadata.name(), "cannot snapshot shard on a readonly repository"));
            return;
        }
        final Store store = context.store();
        final ShardId shardId = store.shardId();
        final SnapshotId snapshotId = context.snapshotId();
        final IndexShardSnapshotStatus snapshotStatus = context.status();
        snapshotStatus.updateStatusDescription("snapshot task runner: setting up shard snapshot");
        final long startTime = threadPool.absoluteTimeInMillis();
        try {
            final ShardGeneration generation = snapshotStatus.generation();
            final BlobContainer shardContainer = shardContainer(context.indexId(), shardId);
            logger.debug("[{}][{}] snapshot to [{}][{}][{}] ...", shardId, snapshotId, metadata.name(), context.indexId(), generation);
            final Set<String> blobs;
            if (generation == null) {
                snapshotStatus.ensureNotAborted();
                snapshotStatus.updateStatusDescription("snapshot task runner: listing blob prefixes");
                try {
                    blobs = shardContainer.listBlobsByPrefix(OperationPurpose.SNAPSHOT_METADATA, SNAPSHOT_INDEX_PREFIX).keySet();
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId, "failed to list blobs", e);
                }
            } else {
                blobs = Collections.singleton(SNAPSHOT_INDEX_PREFIX + generation);
            }

            snapshotStatus.ensureNotAborted();
            snapshotStatus.updateStatusDescription("snapshot task runner: loading snapshot blobs");
            Tuple<BlobStoreIndexShardSnapshots, ShardGeneration> tuple = buildBlobStoreIndexShardSnapshots(
                context.indexId(),
                shardId.id(),
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
            int filesInShardMetadataCount = 0;
            long filesInShardMetadataSize = 0;

            if (store.indexSettings().getIndexMetadata().isSearchableSnapshot()) {
                indexCommitPointFiles = Collections.emptyList();
            } else if (filesFromSegmentInfos == null) {
                // If we did not find a set of files that is equal to the current commit we determine the files to upload by comparing files
                // in the commit with files already in the repository
                indexCommitPointFiles = new ArrayList<>();
                final Collection<String> fileNames;
                final Store.MetadataSnapshot metadataFromStore;
                try (Releasable ignored = context.withCommitRef()) {
                    // TODO apparently we don't use the MetadataSnapshot#.recoveryDiff(...) here but we should
                    try {
                        final IndexCommit snapshotIndexCommit = context.indexCommit();
                        logger.trace("[{}] [{}] Loading store metadata using index commit [{}]", shardId, snapshotId, snapshotIndexCommit);
                        metadataFromStore = store.getMetadata(snapshotIndexCommit);
                        fileNames = snapshotIndexCommit.getFileNames();
                    } catch (IOException e) {
                        throw new IndexShardSnapshotFailedException(shardId, "Failed to get store file metadata", e);
                    }
                }
                for (String fileName : fileNames) {
                    ensureNotAborted(shardId, snapshotId, snapshotStatus, fileName);

                    logger.trace("[{}] [{}] Processing [{}]", shardId, snapshotId, fileName);
                    final StoreFileMetadata md = metadataFromStore.get(fileName);
                    BlobStoreIndexShardSnapshot.FileInfo existingFileInfo = snapshots.findPhysicalIndexFile(md);

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
                        } else {
                            assert assertFileContentsMatchHash(snapshotStatus, snapshotFileInfo, store);
                            filesInShardMetadataCount += 1;
                            filesInShardMetadataSize += md.length();
                        }
                    } else {
                        // a commit point file with the same name, size and checksum was already copied to repository
                        // we will reuse it for this snapshot
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

            snapshotStatus.updateStatusDescription("snapshot task runner: starting shard snapshot");
            snapshotStatus.moveToStarted(
                startTime,
                indexIncrementalFileCount,
                indexTotalNumberOfFiles,
                indexIncrementalSize,
                indexTotalFileSize
            );

            final ShardGeneration indexGeneration;
            final boolean writeShardGens = SnapshotsService.useShardGenerations(context.getRepositoryMetaVersion());
            final boolean writeFileInfoWriterUUID = SnapshotsService.includeFileInfoWriterUUID(context.getRepositoryMetaVersion());
            // build a new BlobStoreIndexShardSnapshot, that includes this one and all the saved ones
            final BlobStoreIndexShardSnapshots updatedBlobStoreIndexShardSnapshots = snapshots.withAddedSnapshot(
                new SnapshotFiles(snapshotId.getName(), indexCommitPointFiles, context.stateIdentifier())
            );
            final Runnable afterWriteSnapBlob;
            if (writeShardGens) {
                // When using shard generations we can safely write the index-${uuid} blob before writing out any of the actual data
                // for this shard since the uuid named blob will simply not be referenced in case of error and thus we will never
                // reference a generation that has not had all its files fully upload.
                indexGeneration = ShardGeneration.newGeneration();
                try {
                    final Map<String, String> serializationParams = Collections.singletonMap(
                        BlobStoreIndexShardSnapshot.FileInfo.SERIALIZE_WRITER_UUID,
                        Boolean.toString(writeFileInfoWriterUUID)
                    );
                    snapshotStatus.updateStatusDescription("snapshot task runner: updating blob store with new shard generation");
                    INDEX_SHARD_SNAPSHOTS_FORMAT.write(
                        updatedBlobStoreIndexShardSnapshots,
                        shardContainer,
                        indexGeneration.getGenerationUUID(),
                        compress,
                        serializationParams
                    );
                    snapshotStatus.addProcessedFiles(filesInShardMetadataCount, filesInShardMetadataSize);
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(
                        shardId,
                        "Failed to write shard level snapshot metadata for ["
                            + snapshotId
                            + "] to ["
                            + INDEX_SHARD_SNAPSHOTS_FORMAT.blobName(indexGeneration.getGenerationUUID())
                            + "]",
                        e
                    );
                }
                afterWriteSnapBlob = () -> {};
            } else {
                // When not using shard generations we can only write the index-${N} blob after all other work for this shard has
                // completed.
                // Also, in case of numeric shard generations the data node has to take care of deleting old shard generations.
                final long newGen = Long.parseLong(fileListGeneration.getGenerationUUID()) + 1;
                indexGeneration = new ShardGeneration(newGen);
                // Delete all previous index-N blobs
                final List<String> blobsToDelete = blobs.stream().filter(blob -> blob.startsWith(SNAPSHOT_INDEX_PREFIX)).toList();
                assert blobsToDelete.stream()
                    .mapToLong(b -> Long.parseLong(b.replaceFirst(SNAPSHOT_INDEX_PREFIX, "")))
                    .max()
                    .orElse(-1L) < Long.parseLong(indexGeneration.toString())
                    : "Tried to delete an index-N blob newer than the current generation ["
                        + indexGeneration
                        + "] when deleting index-N blobs "
                        + blobsToDelete;
                final var finalFilesInShardMetadataCount = filesInShardMetadataCount;
                final var finalFilesInShardMetadataSize = filesInShardMetadataSize;

                afterWriteSnapBlob = () -> {
                    try {
                        final Map<String, String> serializationParams = Collections.singletonMap(
                            BlobStoreIndexShardSnapshot.FileInfo.SERIALIZE_WRITER_UUID,
                            Boolean.toString(writeFileInfoWriterUUID)
                        );
                        snapshotStatus.updateStatusDescription("no shard generations: writing new index-${N} file");
                        writeShardIndexBlobAtomic(shardContainer, newGen, updatedBlobStoreIndexShardSnapshots, serializationParams);
                    } catch (IOException e) {
                        throw new IndexShardSnapshotFailedException(
                            shardId,
                            "Failed to finalize snapshot creation ["
                                + snapshotId
                                + "] with shard index ["
                                + INDEX_SHARD_SNAPSHOTS_FORMAT.blobName(indexGeneration.getGenerationUUID())
                                + "]",
                            e
                        );
                    }
                    snapshotStatus.addProcessedFiles(finalFilesInShardMetadataCount, finalFilesInShardMetadataSize);
                    try {
                        snapshotStatus.updateStatusDescription("no shard generations: deleting blobs");
                        deleteFromContainer(OperationPurpose.SNAPSHOT_METADATA, shardContainer, blobsToDelete.iterator());
                    } catch (IOException e) {
                        logger.warn(
                            () -> format("[%s][%s] failed to delete old index-N blobs during finalization", snapshotId, shardId),
                            e
                        );
                    }
                };
            }

            // filesToSnapshot will be emptied while snapshotting the file. We make a copy here for cleanup purpose in case of failure.
            final AtomicReference<List<FileInfo>> fileToCleanUp = new AtomicReference<>(List.copyOf(filesToSnapshot));
            final ActionListener<Collection<Void>> allFilesUploadedListener = ActionListener.assertOnce(ActionListener.wrap(ignore -> {
                snapshotStatus.updateStatusDescription("all files uploaded: finalizing");
                final IndexShardSnapshotStatus.Copy lastSnapshotStatus = snapshotStatus.moveToFinalize();

                // now create and write the commit point
                logger.trace("[{}] [{}] writing shard snapshot file", shardId, snapshotId);
                final BlobStoreIndexShardSnapshot blobStoreIndexShardSnapshot = new BlobStoreIndexShardSnapshot(
                    snapshotId.getName(),
                    indexCommitPointFiles,
                    lastSnapshotStatus.getStartTime(),
                    threadPool.absoluteTimeInMillis() - lastSnapshotStatus.getStartTime(),
                    lastSnapshotStatus.getIncrementalFileCount(),
                    lastSnapshotStatus.getIncrementalSize()
                );
                // Once we start writing the shard level snapshot file, no cleanup will be performed because it is possible that
                // written files are referenced by another concurrent process.
                fileToCleanUp.set(List.of());
                try {
                    final String snapshotUUID = snapshotId.getUUID();
                    final Map<String, String> serializationParams = Collections.singletonMap(
                        BlobStoreIndexShardSnapshot.FileInfo.SERIALIZE_WRITER_UUID,
                        Boolean.toString(writeFileInfoWriterUUID)
                    );
                    snapshotStatus.updateStatusDescription("all files uploaded: writing to index shard file");
                    INDEX_SHARD_SNAPSHOT_FORMAT.write(
                        blobStoreIndexShardSnapshot,
                        shardContainer,
                        snapshotUUID,
                        compress,
                        serializationParams
                    );
                } catch (IOException e) {
                    throw new IndexShardSnapshotFailedException(shardId, "Failed to write commit point", e);
                }
                afterWriteSnapBlob.run();
                final ShardSnapshotResult shardSnapshotResult = new ShardSnapshotResult(
                    indexGeneration,
                    ByteSizeValue.ofBytes(blobStoreIndexShardSnapshot.totalSize()),
                    getSegmentInfoFileCount(blobStoreIndexShardSnapshot.indexFiles())
                );
                snapshotStatus.updateStatusDescription("all files uploaded: done");
                snapshotStatus.moveToDone(threadPool.absoluteTimeInMillis(), shardSnapshotResult);
                context.onResponse(shardSnapshotResult);
            }, e -> {
                try {
                    snapshotStatus.updateStatusDescription("all files uploaded: cleaning up data files, exception while finalizing: " + e);
                    shardContainer.deleteBlobsIgnoringIfNotExists(
                        OperationPurpose.SNAPSHOT_DATA,
                        Iterators.flatMap(fileToCleanUp.get().iterator(), f -> Iterators.forRange(0, f.numberOfParts(), f::partName))
                    );
                } catch (Exception innerException) {
                    e.addSuppressed(innerException);
                }
                context.onFailure(e);
            }));

            if (indexIncrementalFileCount == 0 || filesToSnapshot.isEmpty()) {
                allFilesUploadedListener.onResponse(Collections.emptyList());
                return;
            }
            snapshotFiles(context, filesToSnapshot, allFilesUploadedListener);
        } catch (Exception e) {
            context.onFailure(e);
        }
    }

    private static void ensureNotAborted(ShardId shardId, SnapshotId snapshotId, IndexShardSnapshotStatus snapshotStatus, String fileName) {
        var shardSnapshotStage = snapshotStatus.getStage();
        try {
            IndexShardSnapshotStatus.ensureNotAborted(shardSnapshotStage);

            if (shardSnapshotStage != IndexShardSnapshotStatus.Stage.INIT && shardSnapshotStage != IndexShardSnapshotStatus.Stage.STARTED) {
                // A normally running shard snapshot should be in stage INIT or STARTED. And we know it's not in PAUSING or ABORTED because
                // the ensureNotAborted() call above did not throw. The remaining options don't make sense, if they ever happen.
                logger.error(
                    "Shard snapshot found an unexpected state. ShardId [{}], SnapshotID [{}], Stage [{}]",
                    shardId,
                    snapshotId,
                    shardSnapshotStage
                );
                assert false;
            }
        } catch (Exception e) {
            // We want to see when a shard snapshot operation checks for and finds an interrupt signal during shutdown. A
            // PausedSnapshotException indicates we're in shutdown because that's the only case when shard snapshots are signaled to pause.
            // An AbortedSnapshotException may also occur during shutdown if an uncommon error occurs.
            ShutdownLogger.shutdownLogger.debug(
                () -> Strings.format(
                    "Shard snapshot operation is aborting. ShardId [%s], SnapshotID [%s], File [%s], Stage [%s]",
                    shardId,
                    snapshotId,
                    fileName,
                    shardSnapshotStage
                ),
                e
            );
            assert e instanceof AbortedSnapshotException || e instanceof PausedSnapshotException : e;
            throw e;
        }
    }

    protected void snapshotFiles(
        SnapshotShardContext context,
        BlockingQueue<FileInfo> filesToSnapshot,
        ActionListener<Collection<Void>> allFilesUploadedListener
    ) {
        final int noOfFilesToSnapshot = filesToSnapshot.size();
        final ActionListener<Void> filesListener = fileQueueListener(filesToSnapshot, noOfFilesToSnapshot, allFilesUploadedListener);
        context.status().updateStatusDescription("enqueued file snapshot tasks: threads running concurrent file uploads");
        for (int i = 0; i < noOfFilesToSnapshot; i++) {
            shardSnapshotTaskRunner.enqueueFileSnapshot(context, filesToSnapshot::poll, filesListener);
        }
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
            try {
                snapshotStatus.ensureNotAborted();
                assert false : "if the store is already closed we must have been aborted";
            } catch (Exception e) {
                assert e instanceof AbortedSnapshotException : e;
            }
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
        if (store.isClosing()) {
            restoreListener.onFailure(new AlreadyClosedException("store is closing"));
            return;
        }
        if (lifecycle.started() == false) {
            restoreListener.onFailure(new AlreadyClosedException("repository [" + metadata.name() + "] closed"));
            return;
        }
        if (activityRefs.tryIncRef() == false) {
            restoreListener.onFailure(new AlreadyClosedException("repository [" + metadata.name() + "] closing"));
            return;
        }
        executor.execute(ActionRunnable.wrap(ActionListener.runBefore(restoreListener, activityRefs::decRef), l -> {
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
                    logger.trace(() -> format("[%s] restoring [%s] to [%s]", metadata.name(), fileInfo, store));
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
                                    return container.readBlob(OperationPurpose.SNAPSHOT_DATA, fileInfo.partName(slice));
                                }

                                @Override
                                public boolean markSupported() {
                                    return false;
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
                    if (lifecycle.started() == false) {
                        throw new AlreadyClosedException("repository [" + metadata.name() + "] closed");
                    }
                }

            }.restore(snapshotFiles, store, l);
        }));
    }

    private static ActionListener<Void> fileQueueListener(
        BlockingQueue<BlobStoreIndexShardSnapshot.FileInfo> files,
        int numberOfFiles,
        ActionListener<Collection<Void>> listener
    ) {
        return new GroupedActionListener<>(numberOfFiles, listener).delegateResponse((l, e) -> {
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
     * Wrap the snapshot rate limiter around the given stream. Any throttling is recorded in the value returned by
     * {@link BlobStoreRepository#getSnapshotThrottleTimeInNanos()}. Note that speed is throttled by the repository setting
     * `max_snapshot_bytes_per_sec` and, if recovery node bandwidth settings have been set, additionally by the
     * `indices.recovery.max_bytes_per_sec` speed.
     */
    public InputStream maybeRateLimitSnapshots(InputStream stream) {
        return maybeRateLimitSnapshots(stream, snapshotRateLimitingTimeInNanos::inc);
    }

    /**
     * Wrap the snapshot rate limiter around the given stream. Any throttling is recorded in the value returned by
     * {@link BlobStoreRepository#getSnapshotThrottleTimeInNanos()}. Note that speed is throttled by the repository setting
     * `max_snapshot_bytes_per_sec` and, if recovery node bandwidth settings have been set, additionally by the
     * `indices.recovery.max_bytes_per_sec` speed.
     */
    public InputStream maybeRateLimitSnapshots(InputStream stream, RateLimitingInputStream.Listener throttleListener) {
        InputStream rateLimitStream = maybeRateLimit(stream, () -> snapshotRateLimiter, throttleListener);
        if (recoverySettings.nodeBandwidthSettingsExist()) {
            rateLimitStream = maybeRateLimit(rateLimitStream, recoverySettings::rateLimiter, throttleListener);
        }
        return rateLimitStream;
    }

    @Override
    public IndexShardSnapshotStatus.Copy getShardSnapshotStatus(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
        BlobStoreIndexShardSnapshot snapshot = loadShardSnapshot(shardContainer(indexId, shardId), snapshotId);
        return IndexShardSnapshotStatus.newDone(
            snapshot.startTime(),
            snapshot.time(),
            snapshot.incrementalFileCount(),
            snapshot.totalFileCount(),
            snapshot.incrementalSize(),
            snapshot.totalSize(),
            null // Not adding a real generation here as it doesn't matter to callers
        );
    }

    @Override
    public void verify(String seed, DiscoveryNode localNode) {
        assertSnapshotOrStatelessPermittedThreadPool();
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
                testBlobContainer.writeBlob(
                    OperationPurpose.SNAPSHOT_METADATA,
                    "data-" + localNode.getId() + METADATA_BLOB_NAME_SUFFIX,
                    new BytesArray(seed),
                    true
                );
            } catch (Exception exp) {
                throw new RepositoryVerificationException(
                    metadata.name(),
                    "store location [" + blobStore() + "] is not accessible on the node [" + localNode + "]",
                    exp
                );
            }
            try (InputStream masterDat = testBlobContainer.readBlob(OperationPurpose.SNAPSHOT_METADATA, "master.dat")) {
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
     * Utility for atomically writing shard level metadata to a numeric shard generation. This is only required for writing
     * numeric shard generations where atomic writes with fail-if-already-exists checks are useful in preventing repository corruption.
     */
    private void writeShardIndexBlobAtomic(
        BlobContainer shardContainer,
        long indexGeneration,
        BlobStoreIndexShardSnapshots updatedSnapshots,
        Map<String, String> serializationParams
    ) throws IOException {
        assert indexGeneration >= 0 : "Shard generation must not be negative but saw [" + indexGeneration + "]";
        logger.trace(() -> format("[%s] Writing shard index [%s] to [%s]", metadata.name(), indexGeneration, shardContainer.path()));
        final String blobName = INDEX_SHARD_SNAPSHOTS_FORMAT.blobName(String.valueOf(indexGeneration));
        writeAtomic(
            OperationPurpose.SNAPSHOT_METADATA,
            shardContainer,
            blobName,
            out -> INDEX_SHARD_SNAPSHOTS_FORMAT.serialize(updatedSnapshots, blobName, compress, serializationParams, out),
            true
        );
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
            blobs = shardContainer.listBlobsByPrefix(OperationPurpose.SNAPSHOT_METADATA, SNAPSHOT_INDEX_PREFIX).keySet();
        }

        return buildBlobStoreIndexShardSnapshots(indexId, shardId, blobs, shardContainer, shardGen).v1();
    }

    /**
     * Loads all available snapshots in the repository using the given {@code generation} or falling back to trying to determine it from
     * the given list of blobs in the shard container.
     *
     * @param indexId    {@link IndexId} identifying the corresponding index
     * @param shardId    The 0-based shard id, see also {@link ShardId#id()}
     * @param blobs      list of blobs in repository
     * @param generation shard generation or {@code null} in case there was no shard generation tracked in the {@link RepositoryData} for
     *                   this shard because its snapshot was created in a version older than
     *                   {@link SnapshotsService#SHARD_GEN_IN_REPO_DATA_VERSION}.
     * @return tuple of BlobStoreIndexShardSnapshots and the last snapshot index generation
     */
    private Tuple<BlobStoreIndexShardSnapshots, ShardGeneration> buildBlobStoreIndexShardSnapshots(
        IndexId indexId,
        int shardId,
        Set<String> blobs,
        BlobContainer shardContainer,
        @Nullable ShardGeneration generation
    ) throws IOException {
        if (generation != null) {
            if (generation.equals(ShardGenerations.NEW_SHARD_GEN)) {
                return new Tuple<>(BlobStoreIndexShardSnapshots.EMPTY, ShardGenerations.NEW_SHARD_GEN);
            }
            try {
                return new Tuple<>(
                    INDEX_SHARD_SNAPSHOTS_FORMAT.read(
                        metadata.name(),
                        shardContainer,
                        generation.getGenerationUUID(),
                        namedXContentRegistry
                    ),
                    generation
                );
            } catch (NoSuchFileException noSuchFileException) {
                // Master may have concurrently mutated the shard generation. This can happen when master fails over
                // which is "expected". We do not need to apply the following workaround for missing file in this case.
                final RepositoryData currentRepositoryData;
                try {
                    final long latestGeneration = latestIndexBlobId();
                    currentRepositoryData = getRepositoryData(latestGeneration);
                } catch (Exception e) {
                    noSuchFileException.addSuppressed(e);
                    throw noSuchFileException;
                }
                final ShardGeneration latestShardGen = currentRepositoryData.shardGenerations().getShardGen(indexId, shardId);
                if (latestShardGen == null || latestShardGen.equals(generation) == false) {
                    throw noSuchFileException;
                }

                // This shouldn't happen (absent an external force deleting blobs from the repo) but in practice we've found bugs in the way
                // we manipulate shard generation UUIDs under concurrent snapshot load which can lead to incorrectly deleting a referenced
                // shard-level `index-UUID` blob during finalization. We definitely want to treat this as a test failure (see the `assert`
                // below), but in production it is rather poor UX to leave this shard in a state where it permanently cannot take any new
                // snapshots. Especially if we got into this state because of a bug. Thus we fall back to reconstructing the contents of the
                // missing blob from all available shard-level snapshot metadata blobs.
                //
                // Note that if a shard-level snapshot metadata blob is also missing when executing this fallback then we'll lose track of,
                // and eventually delete, the corresponding data blobs. We haven't seen any bugs that can lead to missing shard-level
                // metadata blobs, and any such bug would prevent the shard snapshot from being restored anyway so there's no point in
                // keeping hold of its data blobs.
                try {
                    final var message = Strings.format(
                        "index %s shard generation [%s] in [%s][%s] not found - falling back to reading all shard snapshots",
                        indexId,
                        generation,
                        metadata.name(),
                        shardContainer.path()
                    );
                    logger.error(message, noSuchFileException);
                    assert BlobStoreIndexShardSnapshots.areIntegrityAssertionsEnabled() == false
                        : new AssertionError(message, noSuchFileException);

                    final var shardSnapshotBlobs = shardContainer.listBlobsByPrefix(OperationPurpose.SNAPSHOT_METADATA, SNAPSHOT_PREFIX);
                    var blobStoreIndexShardSnapshots = BlobStoreIndexShardSnapshots.EMPTY;
                    final var messageBuilder = new StringBuilder();
                    final var shardSnapshotBlobNameLengthBeforeExt = SNAPSHOT_PREFIX.length() + UUIDs.RANDOM_BASED_UUID_STRING_LENGTH;
                    final var shardSnapshotBlobNameLength = shardSnapshotBlobNameLengthBeforeExt + METADATA_BLOB_NAME_SUFFIX.length();
                    for (final var shardSnapshotBlobName : shardSnapshotBlobs.keySet()) {
                        if (shardSnapshotBlobName.startsWith(SNAPSHOT_PREFIX)
                            && shardSnapshotBlobName.endsWith(METADATA_BLOB_NAME_SUFFIX)
                            && shardSnapshotBlobName.length() == shardSnapshotBlobNameLength) {
                            final var shardSnapshot = INDEX_SHARD_SNAPSHOT_FORMAT.read(
                                metadata.name(),
                                shardContainer,
                                shardSnapshotBlobName.substring(SNAPSHOT_PREFIX.length(), shardSnapshotBlobNameLengthBeforeExt),
                                namedXContentRegistry
                            );
                            blobStoreIndexShardSnapshots = blobStoreIndexShardSnapshots.withAddedSnapshot(
                                new SnapshotFiles(shardSnapshot.snapshot(), shardSnapshot.indexFiles(), null)
                            );
                            if (messageBuilder.length() > 0) {
                                messageBuilder.append(", ");
                            }
                            messageBuilder.append(shardSnapshotBlobName);
                        } else {
                            throw new IllegalStateException(
                                Strings.format(
                                    "unexpected shard snapshot blob [%s] found in [%s][%s]",
                                    shardSnapshotBlobName,
                                    metadata.name(),
                                    shardContainer.path()
                                )
                            );
                        }
                    }
                    logger.error(
                        "read shard snapshots [{}] due to missing shard generation [{}] for index {} in [{}][{}]",
                        messageBuilder,
                        generation,
                        indexId,
                        metadata.name(),
                        shardContainer.path()
                    );
                    return new Tuple<>(blobStoreIndexShardSnapshots, generation);
                } catch (Exception fallbackException) {
                    logger.error(
                        Strings.format("failed while reading all shard snapshots from [%s][%s]", metadata.name(), shardContainer.path()),
                        fallbackException
                    );
                    noSuchFileException.addSuppressed(fallbackException);
                    throw noSuchFileException;
                }
            }
        } else {
            final Tuple<BlobStoreIndexShardSnapshots, Long> legacyIndex = buildBlobStoreIndexShardSnapshots(blobs, shardContainer);
            return new Tuple<>(legacyIndex.v1(), new ShardGeneration(legacyIndex.v2()));
        }
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
            .anyMatch(
                b -> b.startsWith(SNAPSHOT_PREFIX) || b.startsWith(SNAPSHOT_INDEX_PREFIX) || b.startsWith(UPLOADED_DATA_BLOB_PREFIX)
            )) {
                logger.warn(
                    "Could not find a readable index-N file in a non-empty shard snapshot directory [" + shardContainer.path() + "]"
                );
            }
        return new Tuple<>(BlobStoreIndexShardSnapshots.EMPTY, latest);
    }

    /**
     * Snapshot individual file
     * @param fileInfo file to snapshot
     */
    protected void snapshotFile(SnapshotShardContext context, FileInfo fileInfo) throws IOException {
        final IndexId indexId = context.indexId();
        final Store store = context.store();
        final ShardId shardId = store.shardId();
        final IndexShardSnapshotStatus snapshotStatus = context.status();
        final SnapshotId snapshotId = context.snapshotId();
        final BlobContainer shardContainer = shardContainer(indexId, shardId);
        final String file = fileInfo.physicalName();
        try (
            Releasable ignored = context.withCommitRef();
            IndexInput indexInput = store.openVerifyingInput(file, IOContext.DEFAULT, fileInfo.metadata())
        ) {
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
                        ensureNotAborted(shardId, snapshotId, snapshotStatus, fileInfo.physicalName());
                    }
                };
                final String partName = fileInfo.partName(i);
                logger.trace("[{}] Writing [{}] to [{}]", metadata.name(), partName, shardContainer.path());
                final long startMS = threadPool.relativeTimeInMillis();
                shardContainer.writeBlob(OperationPurpose.SNAPSHOT_DATA, partName, inputStream, partBytes, false);
                logger.trace(
                    "[{}] Writing [{}] of size [{}b] to [{}] took [{}ms]",
                    metadata.name(),
                    partName,
                    partBytes,
                    shardContainer.path(),
                    threadPool.relativeTimeInMillis() - startMS
                );
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

    public int getReadBufferSizeInBytes() {
        return bufferSize;
    }

    /**
     * @return extra information to be included in the exception message emitted on failure of a repository analysis.
     */
    public String getAnalysisFailureExtraDetail() {
        return Strings.format(
            """
                Elasticsearch observed the storage system underneath this repository behaved incorrectly which indicates it is not \
                suitable for use with Elasticsearch snapshots. See [%s] for further information.""",
            ReferenceDocs.SNAPSHOT_REPOSITORY_ANALYSIS
        );
    }

    public static final String READ_ONLY_USAGE_STATS_NAME = "read_only";
    public static final String READ_WRITE_USAGE_STATS_NAME = "read_write";

    @Override
    public final Set<String> getUsageFeatures() {
        final var extraUsageFeatures = getExtraUsageFeatures();
        assert extraUsageFeatures.contains(READ_ONLY_USAGE_STATS_NAME) == false : extraUsageFeatures;
        assert extraUsageFeatures.contains(READ_WRITE_USAGE_STATS_NAME) == false : extraUsageFeatures;
        return Set.copyOf(
            Stream.concat(Stream.of(isReadOnly() ? READ_ONLY_USAGE_STATS_NAME : READ_WRITE_USAGE_STATS_NAME), extraUsageFeatures.stream())
                .toList()
        );
    }

    /**
     * All blob-store repositories include the counts of read-only and read-write repositories in their telemetry. This method returns other
     * features of the repositories in use.
     *
     * @return a set of the names of the extra features that this repository instance uses, for reporting in the cluster stats for telemetry
     *         collection.
     */
    protected Set<String> getExtraUsageFeatures() {
        return Set.of();
    }
}
