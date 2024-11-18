/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.objectstore;

import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.BlobFile;
import co.elastic.elasticsearch.stateless.commits.BlobFileRanges;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StaleCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.VirtualBatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;
import co.elastic.elasticsearch.stateless.lucene.BlobStoreCacheDirectory;
import co.elastic.elasticsearch.stateless.lucene.IndexBlobStoreCacheDirectory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.PrioritizedThrottledTaskRunner;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.Level;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryStats;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.stateless.commits.BlobFileRanges.computeBlobFileRanges;
import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.parseGenerationFromBlobName;
import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.startsWithBlobPrefix;
import static org.elasticsearch.core.Strings.format;

public class ObjectStoreService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(ObjectStoreService.class);
    private static final Logger SHARD_FILES_DELETES_LOGGER = LogManager.getLogger(
        ObjectStoreService.class.getCanonicalName() + ".shard_files_deletes"
    );

    /**
     * This setting refers to the destination of the blobs in the object store.
     * Depending on the underlying object store type, it may be a bucket (for S3 or GCP), a location (for FS), or a container (for Azure).
     */
    public static final Setting<String> BUCKET_SETTING = Setting.simpleString("stateless.object_store.bucket", Setting.Property.NodeScope);

    public static final Setting<String> CLIENT_SETTING = Setting.simpleString("stateless.object_store.client", Setting.Property.NodeScope);

    public static final Setting<String> BASE_PATH_SETTING = Setting.simpleString(
        "stateless.object_store.base_path",
        Setting.Property.NodeScope
    );
    public static final int DELETE_BATCH_SIZE = 100;

    public enum ObjectStoreType {
        FS("location") {
            @Override
            @SuppressForbidden(reason = "creates path to external blobstore")
            public Settings createRepositorySettings(String bucket, String client, String basePath) {
                return Settings.builder().put("location", basePath != null ? PathUtils.get(bucket, basePath).toString() : bucket).build();
            }
        },
        MOCK("location") {
            @Override
            public Settings createRepositorySettings(String bucket, String client, String basePath) {
                return FS.createRepositorySettings(bucket, client, basePath);
            }
        },
        S3("bucket"),
        GCS("bucket"),
        AZURE("container");

        private final String bucketSettingName;

        ObjectStoreType(String bucketSettingName) {
            this.bucketSettingName = bucketSettingName;
        }

        public Settings createRepositorySettings(String bucket, String client, String basePath) {
            Settings.Builder builder = Settings.builder();
            builder.put(bucketSettingName, bucket);
            builder.put("client", client);
            if (basePath != null) {
                builder.put("base_path", basePath);
            }
            return builder.build();
        }

        public boolean needsClient() {
            return this != FS && this != MOCK;
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    public record IndexingShardState(
        BatchedCompoundCommit latestCommit,
        Set<BlobFile> unreferencedBlobs,
        Map<String, BlobFileRanges> blobFileRanges
    ) {
        public static IndexingShardState EMPTY = new IndexingShardState(null, Set.of(), Map.of());
    }

    public static final Setting<ObjectStoreType> TYPE_SETTING = Setting.enumSetting(
        ObjectStoreType.class,
        "stateless.object_store.type",
        ObjectStoreType.FS,
        new Setting.Validator<>() {
            @Override
            public void validate(ObjectStoreType value) {}

            @Override
            public void validate(final ObjectStoreType value, final Map<Setting<?>, Object> settings, boolean isPresent) {
                final String bucket = (String) settings.get(BUCKET_SETTING);
                final String client = (String) settings.get(CLIENT_SETTING);
                if (bucket.isEmpty()) {
                    throw new IllegalArgumentException(
                        "setting " + BUCKET_SETTING.getKey() + " must be set for an object store of type " + value
                    );
                }
                if (value.needsClient() && client.isEmpty()) {
                    throw new IllegalArgumentException(
                        "setting " + CLIENT_SETTING.getKey() + " must be set for an object store of type " + value
                    );
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return List.<Setting<?>>of(BUCKET_SETTING, CLIENT_SETTING).iterator();
            }
        },
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> OBJECT_STORE_FILE_DELETION_DELAY = Setting.timeSetting(
        "stateless.object_store.file_deletion_delay",
        TimeValue.ZERO,
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> OBJECT_STORE_SHUTDOWN_TIMEOUT = Setting.timeSetting(
        "stateless.object_store.shutdown_timeout",
        TimeValue.timeValueSeconds(10L),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    private static final int UPLOAD_PERMITS = Integer.MAX_VALUE;

    private final Settings settings;
    private final RepositoriesService repositoriesService;
    private final ThreadPool threadPool;
    private final PrioritizedThrottledTaskRunner<TranslogFileUploadTask> uploadTranslogTaskRunner;
    private final PrioritizedThrottledTaskRunner<ObjectStoreTask> uploadTaskRunner;
    private final ConcurrentLinkedQueue<String> translogBlobsToDelete = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<StaleCompoundCommit> commitBlobsToDelete = new ConcurrentLinkedQueue<>();
    private final Semaphore translogDeleteSchedulePermit = new Semaphore(1);
    private final Semaphore shardFileDeleteSchedulePermit = new Semaphore(1);
    private final Semaphore permits;

    private BlobStoreRepository objectStore;

    private final ClusterService clusterService;

    public ObjectStoreService(
        Settings settings,
        RepositoriesService repositoriesService,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        this.settings = settings;
        this.repositoriesService = repositoriesService;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.uploadTranslogTaskRunner = new PrioritizedThrottledTaskRunner<>(
            getClass().getSimpleName() + "#upload-translog-file-task-runner",
            threadPool.info(Stateless.TRANSLOG_THREAD_POOL).getMax(),
            threadPool.executor(Stateless.TRANSLOG_THREAD_POOL)
        );
        this.uploadTaskRunner = new PrioritizedThrottledTaskRunner<>(
            getClass().getSimpleName() + "#upload-task-runner",
            threadPool.info(Stateless.SHARD_WRITE_THREAD_POOL).getMax(),
            threadPool.executor(Stateless.SHARD_WRITE_THREAD_POOL)
        );
        this.permits = new Semaphore(0);
    }

    // package private for tests
    BlobStoreRepository getObjectStore() {
        if (objectStore == null) {
            throw new IllegalStateException("Blob store is null");
        }
        if (objectStore.lifecycleState() != Lifecycle.State.STARTED) {
            throw new IllegalStateException("Blob store is not started");
        }
        return objectStore;
    }

    public BlobPath shardBasePath(ShardId shardId) {
        return getObjectStore().basePath().add("indices").add(shardId.getIndex().getUUID()).add(String.valueOf(shardId.id()));
    }

    public BlobStore blobStore() {
        return getObjectStore().blobStore();
    }

    // public for testing
    public BlobContainer getBlobContainer(ShardId shardId, long primaryTerm) {
        final BlobStoreRepository objectStore = getObjectStore();
        return objectStore.blobStore()
            .blobContainer(
                objectStore.basePath()
                    .add("indices")
                    .add(shardId.getIndex().getUUID())
                    .add(String.valueOf(shardId.id()))
                    .add(String.valueOf(primaryTerm))
            );
    }

    public BlobContainer getBlobContainer(ShardId shardId) {
        final BlobStoreRepository objectStore = getObjectStore();
        return objectStore.blobStore()
            .blobContainer(objectStore.basePath().add("indices").add(shardId.getIndex().getUUID()).add(String.valueOf(shardId.id())));
    }

    public BlobContainer getIndicesBlobContainer() {
        final BlobStoreRepository objectStore = getObjectStore();
        return objectStore.blobStore().blobContainer(objectStore.basePath().add("indices"));
    }

    public BlobContainer getIndexBlobContainer(String indexUUID) {
        final BlobStoreRepository objectStore = getObjectStore();
        return objectStore.blobStore().blobContainer(objectStore.basePath().add("indices").add(indexUUID));
    }

    public BlobContainer getClusterStateBlobContainer() {
        final BlobStoreRepository objectStore = getObjectStore();
        return objectStore.blobStore().blobContainer(objectStore.basePath().add("cluster_state"));
    }

    public BlobContainer getClusterStateBlobContainerForTerm(long term) {
        final BlobStoreRepository objectStore = getObjectStore();
        return objectStore.blobStore().blobContainer(objectStore.basePath().add("cluster_state").add(String.valueOf(term)));
    }

    public BlobContainer getClusterStateHeartbeatContainer() {
        final BlobStoreRepository objectStore = getObjectStore();
        return objectStore.blobStore().blobContainer(objectStore.basePath().add("cluster_state").add("heartbeat"));
    }

    /**
     * Gets the translog blob container of the local node
     */
    public BlobContainer getTranslogBlobContainer() {
        final DiscoveryNode discoveryNode = clusterService.localNode();
        return getTranslogBlobContainer(discoveryNode.getEphemeralId());
    }

    /**
     * Gets the translog blob container of a node
     */
    public BlobContainer getTranslogBlobContainer(String nodeEphemeralId) {
        return getObjectStore().blobStore().blobContainer(objectStore.basePath().add("nodes").add(nodeEphemeralId).add("translog"));
    }

    /**
     * Gets the set of node ephemeral IDs that have translog blob containers
     */
    public Set<String> getNodesWithTranslogBlobContainers() throws IOException {
        return getObjectStore().blobStore().blobContainer(objectStore.basePath().add("nodes")).children(OperationPurpose.TRANSLOG).keySet();
    }

    public RepositoryStats stats() {
        return objectStore.stats();
    }

    protected Settings getRepositorySettings(ObjectStoreType type) {
        return type.createRepositorySettings(BUCKET_SETTING.get(settings), CLIENT_SETTING.get(settings), BASE_PATH_SETTING.get(settings));
    }

    private RepositoryMetadata getRepositoryMetadata(Settings settings) {
        ObjectStoreType type = TYPE_SETTING.get(settings);
        return new RepositoryMetadata(Stateless.NAME, type.toString(), getRepositorySettings(type));
    }

    @Override
    protected void doStart() {
        if (repositoriesService == null) {
            throw new IllegalStateException("Repositories service is not initialized");
        }
        assert objectStore == null;
        Repository repository = repositoriesService.createRepository(getRepositoryMetadata(settings));
        assert repository instanceof BlobStoreRepository;
        this.objectStore = (BlobStoreRepository) repository;
        this.objectStore.start();
        this.permits.release(UPLOAD_PERMITS);
        logger.info(
            "using object store type [{}], bucket [{}], base path [{}], client [{}]",
            TYPE_SETTING.get(settings),
            BUCKET_SETTING.get(settings),
            objectStore.basePath().buildAsString(),
            CLIENT_SETTING.get(settings)
        );
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {
        logger.debug("object store service closing...");
        try {
            final TimeValue timeout = OBJECT_STORE_SHUTDOWN_TIMEOUT.get(settings);
            var acquired = permits.tryAcquire(UPLOAD_PERMITS, timeout.duration(), timeout.timeUnit());
            if (acquired == false) {
                logger.warn("failed to wait [{}] for object store tasks to complete", timeout);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.warn("interrupted while waiting for the object store service to shut down", e);
        }
        objectStore.close();
        objectStore = null;
        logger.info("object store service closed");
    }

    private void ensureRunning() {
        final Lifecycle.State state = lifecycleState();
        if (state == Lifecycle.State.INITIALIZED || state == Lifecycle.State.CLOSED) {
            throw new IllegalStateException("Object store service is not running [" + state + ']');
        }
    }

    public void uploadTranslogFile(String fileName, BytesReference reference, ActionListener<Void> listener) {
        enqueueTask(listener, uploadTranslogTaskRunner, l -> new TranslogFileUploadTask(fileName, reference, l));
    }

    public void uploadBatchedCompoundCommitFile(
        long primaryTerm,
        Directory directory,
        long commitStartNanos,
        VirtualBatchedCompoundCommit pendingCommit,
        ActionListener<Void> listener
    ) {
        enqueueTask(
            listener,
            uploadTaskRunner,
            l -> new BatchedCommitFileUploadTask(
                commitStartNanos,
                pendingCommit,
                BlobStoreCacheDirectory.unwrapDirectory(directory).getBlobContainer(primaryTerm),
                l
            )
        );
    }

    public void asyncDeleteTranslogFile(String fileToDelete) {
        asyncDeleteFile(() -> {
            translogBlobsToDelete.add(fileToDelete);
            if (translogDeleteSchedulePermit.tryAcquire()) {
                threadPool.executor(Stateless.TRANSLOG_THREAD_POOL).execute(new FileDeleteTask(this::getTranslogBlobContainer));
            }
        }, Stateless.TRANSLOG_THREAD_POOL);
    }

    public void asyncDeleteShardFile(StaleCompoundCommit staleCompoundCommit) {
        asyncDeleteFile(() -> {
            commitBlobsToDelete.add(staleCompoundCommit);
            if (shardFileDeleteSchedulePermit.tryAcquire()) {
                threadPool.executor(Stateless.SHARD_WRITE_THREAD_POOL).execute(new ShardFilesDeleteTask());
            }
        }, Stateless.SHARD_WRITE_THREAD_POOL);
    }

    private void asyncDeleteFile(Runnable deleteFileRunnable, String executor) {
        TimeValue delay = OBJECT_STORE_FILE_DELETION_DELAY.get(settings);
        if (delay.compareTo(TimeValue.ZERO) == 0) {
            deleteFileRunnable.run();
        } else {
            threadPool.schedule(deleteFileRunnable, delay, threadPool.executor(executor));
        }
    }

    private <R, T extends AbstractRunnable & Comparable<T>> void enqueueTask(
        ActionListener<R> listener,
        PrioritizedThrottledTaskRunner<T> runner,
        Function<ActionListener<R>, T> task
    ) {
        try {
            ensureRunning();
            if (permits.tryAcquire() == false) {
                throw new IllegalStateException("Failed to acquire permit to enqueue task");
            }
            final var releasable = Releasables.releaseOnce(permits::release);
            boolean enqueued = false;
            try {
                runner.enqueueTask(task.apply(ActionListener.releaseAfter(listener, releasable)));
                enqueued = true;
            } finally {
                if (enqueued == false) {
                    Releasables.closeExpectNoException(releasable);
                }
            }
        } catch (Exception e) {
            assert false : "enqueue task failed: " + e;
            listener.onFailure(e);
        }
    }

    /**
     * List all blobs located in a blob container and return a sorted map of blob's term/generation with their {@link BlobMetadata}.
     *
     * @param blobPrimaryTerm   the primary term to associate to blobs (used to build the term/generation key)
     * @param blobContainer     the blob container to list blobs from
     * @return                  a sorted map of blob's {@link PrimaryTermAndGeneration} and their  {@link BlobMetadata}
     * @throws IOException      if an I/O exception occurs while listing blobs
     */
    // package private for testing
    static NavigableMap<PrimaryTermAndGeneration, BlobMetadata> listBlobs(long blobPrimaryTerm, BlobContainer blobContainer)
        throws IOException {
        var blobs = blobContainer.listBlobs(OperationPurpose.INDICES);
        logger.trace(() -> format("listing blobs in [%s]: %s", blobContainer.path().buildAsString(), blobs));
        var map = new TreeMap<PrimaryTermAndGeneration, BlobMetadata>();
        for (var blob : blobs.entrySet()) {
            if (startsWithBlobPrefix(blob.getKey())) {
                map.put(new PrimaryTermAndGeneration(blobPrimaryTerm, parseGenerationFromBlobName(blob.getKey())), blob.getValue());
            } else {
                logger.warn(
                    () -> format(
                        "found object store file which does not match compound commit file naming pattern [%s] in [%s]",
                        blob.getKey(),
                        blobContainer.path().buildAsString()
                    )
                );
            }
        }
        return Collections.unmodifiableNavigableMap(map);
    }

    /**
     * Read a batched compound commit blob identified by a term/generation from the object store, fetching bytes using a prewarming instance
     * from the provided {@link IndexBlobStoreCacheDirectory} and therefore populating the cache for every region that contains a compound
     * commit header.
     *
     * @param directory         the {@link IndexBlobStoreCacheDirectory} used to read the blob in the object store
     * @param blobTermAndGen    the term/generation of the blob to read
     * @param maxBlobLength     the blob's maximum length to read
     * @param exactBlobLength   a flag indicating that the max. blob length is equal to the real blob length in the object store (flag is
     *                          {@code true}) or not (flag is {@code false}) in which case we are OK to not read the blob fully. This flag
     *                          is used in assertions only.
     * @return                  a {@link BatchedCompoundCommit}
     * @throws IOException      if an I/O error occurs
     */
    private static BatchedCompoundCommit readBatchedCompoundCommitUsingCache(
        IndexBlobStoreCacheDirectory directory,
        IOContext context,
        PrimaryTermAndGeneration blobTermAndGen,
        long maxBlobLength,
        boolean exactBlobLength
    ) throws IOException {
        assert directory.getBlobContainer(blobTermAndGen.primaryTerm()) != null;
        var blobName = StatelessCompoundCommit.blobNameFromGeneration(blobTermAndGen.generation());
        logger.trace(
            () -> format(
                "%s reading blob [name=%s, length=%d]%s from object store using cache",
                directory.getShardId(),
                blobName,
                maxBlobLength,
                blobTermAndGen
            )
        );
        var dir = directory.createNewInstance();
        dir.updateMetadata(
            Map.of(blobName, new BlobFileRanges(new BlobLocation(new BlobFile(blobName, blobTermAndGen), 0L, maxBlobLength))),
            maxBlobLength
        );
        return BatchedCompoundCommit.readFromStore(blobName, maxBlobLength, (ignored, offset, length) -> {
            assert offset + length <= maxBlobLength : offset + " + " + length + " > " + maxBlobLength;
            var input = dir.openInput(blobName, context);
            try {
                return new InputStreamStreamInput(new InputStreamIndexInput(input.slice(blobName, offset, length), length) {
                    @Override
                    public void close() throws IOException {
                        IOUtils.close(super::close, input);
                    }
                }, length);
            } catch (IOException e) {
                IOUtils.closeWhileHandlingException(input);
                throw e;
            }
        }, exactBlobLength);
    }

    /**
     * Find the latest batched compound commit in the provided map and read it using
     * {@link #readBatchedCompoundCommitUsingCache(IndexBlobStoreCacheDirectory, IOContext, PrimaryTermAndGeneration, long, boolean)}
     *
     * @param directory the {@link IndexBlobStoreCacheDirectory} to use for reading the blob
     * @param blobs     a sorted map of batched compound commit blobs
     * @return          a {@link BatchedCompoundCommit}
     * @throws IOException if an I/O exception occurs while reading the blob using the cache
     */
    // package private for testing
    static BatchedCompoundCommit readLatestBcc(
        IndexBlobStoreCacheDirectory directory,
        IOContext context,
        NavigableMap<PrimaryTermAndGeneration, BlobMetadata> blobs
    ) throws IOException {
        var lastEntry = blobs.lastEntry();
        if (lastEntry != null) {
            assert startsWithBlobPrefix(lastEntry.getValue().name()) : lastEntry.getValue();
            return readBatchedCompoundCommitUsingCache(directory, context, lastEntry.getKey(), lastEntry.getValue().length(), true);
        }
        return null;
    }

    // Package private for testing
    static BatchedCompoundCommit readNewestBcc(BlobContainer blobContainer, Map<String, BlobMetadata> allBlobs) throws IOException {

        final BlobMetadata blobMetadataOfMaxGeneration = allBlobs.values()
            .stream()
            .filter(blobMetadata -> startsWithBlobPrefix(blobMetadata.name()))
            .max(Comparator.comparingLong(m -> parseGenerationFromBlobName(m.name())))
            .orElse(null);

        if (blobMetadataOfMaxGeneration == null) {
            return null;
        }
        return readBatchedCompoundCommitFromStore(blobContainer, blobMetadataOfMaxGeneration.name(), blobMetadataOfMaxGeneration.length());
    }

    private static BatchedCompoundCommit readBatchedCompoundCommitFromStore(BlobContainer blobContainer, String blobName, long blobLength)
        throws IOException {
        return BatchedCompoundCommit.readFromStore(
            blobName,
            blobLength,
            // The following issues a new call to blobstore for each CC as suggested by the BlobReader interface.
            (name, offset, length) -> new InputStreamStreamInput(blobContainer.readBlob(OperationPurpose.INDICES, name, offset, length)),
            true
        );
    }

    private static List<Tuple<Long, BlobContainer>> getContainersToSearch(BlobContainer shardContainer, long primaryTerm)
        throws IOException {
        return shardContainer.children(OperationPurpose.INDICES).entrySet().stream().filter(e -> {
            try {
                Long.parseLong(e.getKey());
                return true;
            } catch (NumberFormatException ex) {
                return false;
            }
        })
            .map(e -> new Tuple<>(Long.valueOf(e.getKey()), e.getValue()))
            .filter(t -> t.v1() <= primaryTerm)
            .sorted(Comparator.comparingLong((Tuple<Long, BlobContainer> o) -> o.v1()).reversed())
            .toList();
    }

    public static BatchedCompoundCommit readSearchShardState(BlobContainer shardContainer, long primaryTerm) throws IOException {
        BatchedCompoundCommit latestBcc = null;
        List<Tuple<Long, BlobContainer>> containersToSearch = getContainersToSearch(shardContainer, primaryTerm);
        for (Tuple<Long, BlobContainer> container : containersToSearch) {
            final var blobContainer = container.v2();

            Map<String, BlobMetadata> allBlobs = blobContainer.listBlobs(OperationPurpose.INDICES);
            logger.trace(() -> format("listing blobs in [%s]: %s", blobContainer.path().buildAsString(), allBlobs));

            latestBcc = ObjectStoreService.readNewestBcc(blobContainer, allBlobs);
            if (latestBcc != null) {
                logLatestBcc(latestBcc, blobContainer);
                break;
            }
        }
        return latestBcc;
    }

    public static void readIndexingShardState(
        IndexBlobStoreCacheDirectory directory,
        IOContext context,
        BlobContainer shardContainer,
        long primaryTerm,
        ThreadPool threadPool,
        boolean useReplicatedRanges,
        ActionListener<IndexingShardState> listener
    ) {
        assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);
        SubscribableListener
            // List the existing blob containers (with their primary terms) in the object store
            .<List<Tuple<Long, BlobContainer>>>newForked(l -> ActionListener.completeWith(l, () -> {
                var blobContainersWithTerms = getContainersToSearch(shardContainer, primaryTerm);
                if (logger.isTraceEnabled() && blobContainersWithTerms.isEmpty()) {
                    logger.trace(
                        () -> format(
                            "%s no blob containers found for recovery in [%s]",
                            directory.getShardId(),
                            shardContainer.path().buildAsString()
                        )
                    );
                }
                return blobContainersWithTerms;
            }))
            // Build a list of all blobs in the object store that are in a container with term <= primary term
            .<NavigableMap<PrimaryTermAndGeneration, BlobMetadata>>andThen((l, blobContainersWithTerms) -> {
                if (blobContainersWithTerms.isEmpty()) {
                    ActionListener.completeWith(l, Collections::emptyNavigableMap);
                    return;
                }
                var blobs = new ConcurrentSkipListMap<PrimaryTermAndGeneration, BlobMetadata>();
                try (
                    var listeners = new RefCountingListener(
                        // Fork back to generic thread pool to continue the recovery (or fail the recovery if a listing throws)
                        ActionListener.wrap(ignored -> threadPool.generic().execute(ActionRunnable.supply(l, () -> blobs)), l::onFailure)
                    )
                ) {
                    for (var blobContainerWithTerm : blobContainersWithTerms) {
                        // use the prewarm thread pool to avoid exceeding the connection pool size with blob listings
                        threadPool.executor(Stateless.PREWARM_THREAD_POOL)
                            .execute(
                                ActionRunnable.run(
                                    listeners.acquire(),
                                    () -> blobs.putAll(listBlobs(blobContainerWithTerm.v1(), blobContainerWithTerm.v2()))
                                )
                            );
                    }
                }
            })
            .<IndexingShardState>andThen((l, blobs) -> {
                assert ThreadPool.assertCurrentThreadPool(ThreadPool.Names.GENERIC);

                // Find the most recent batched compound commit and read its headers using cache
                var latestBcc = ObjectStoreService.readLatestBcc(directory, context, blobs);
                if (latestBcc == null) {
                    logger.trace(() -> format("%s no blob found for recovery", directory.getShardId()));
                    ActionListener.completeWith(l, () -> IndexingShardState.EMPTY);
                    return;
                }

                // List of non latest blobs found in the object store
                var otherBlobs = blobs.entrySet()
                    .stream()
                    .filter(entry -> Objects.equals(entry.getKey(), latestBcc.primaryTermAndGeneration()) == false)
                    .map(entry -> new BlobFile(entry.getValue().name(), entry.getKey()))
                    .collect(Collectors.toUnmodifiableSet());
                logger.trace(
                    () -> format(
                        "%s found non-latest blobs in [%s]: %s",
                        directory.getShardId(),
                        shardContainer.path().buildAsString(),
                        otherBlobs
                    )
                );

                // Map of blobs used as location in commit files (including the latest bcc). The key is the blob's term/generation and the
                // value is a tuple of the blob's max length that includes the set of commit files in that blob (used later to stop reading
                // the blob's commit headers early) and the set of commit files that are contained in the blob.
                var referencedBlobs = computedReferencedBlobs(latestBcc);

                // Read/Warm header(s) of every referenced blob and compute BlobFileRanges
                readBlobsAndComputeBlobFileRanges(
                    directory,
                    context,
                    latestBcc,
                    referencedBlobs,
                    useReplicatedRanges,
                    threadPool.generic(),
                    l.map(blobFileRanges -> new IndexingShardState(latestBcc, otherBlobs, Map.copyOf(blobFileRanges)))
                );
            })
            .addListener(listener);
    }

    private static Map<PrimaryTermAndGeneration, ReferencedBlobMaxBlobLengthAndFiles> computedReferencedBlobs(
        BatchedCompoundCommit latestBcc
    ) {
        var referencedBlobs = new HashMap<PrimaryTermAndGeneration, ReferencedBlobMaxBlobLengthAndFiles>();
        for (var commitFile : latestBcc.lastCompoundCommit().commitFiles().entrySet()) {
            var blobLocation = commitFile.getValue();
            referencedBlobs.compute(blobLocation.getBatchedCompoundCommitTermAndGeneration(), (ignored, existing) -> {
                long maxBlobLength = blobLocation.offset();
                if (existing == null) {
                    return new ReferencedBlobMaxBlobLengthAndFiles(maxBlobLength, Set.of(commitFile.getKey()));
                } else {
                    return new ReferencedBlobMaxBlobLengthAndFiles(
                        // max position in the blob to read (header is located before that)
                        Math.max(existing.maxBlobLength(), maxBlobLength),
                        // set of files contained in the blob
                        Sets.union(existing.files(), Set.of(commitFile.getKey()))
                    );
                }
            });
        }
        return referencedBlobs;
    }

    private static void readBlobsAndComputeBlobFileRanges(
        IndexBlobStoreCacheDirectory directory,
        IOContext context,
        BatchedCompoundCommit latestBcc,
        Map<PrimaryTermAndGeneration, ReferencedBlobMaxBlobLengthAndFiles> referencedBlobs,
        boolean useReplicatedRanges,
        ExecutorService executor,
        ActionListener<Map<String, BlobFileRanges>> listener
    ) {
        // Map of commit files names and their corresponding BlobFileRanges
        final Map<String, BlobFileRanges> blobFileRanges = ConcurrentCollections.newConcurrentMap();

        try (var listeners = new RefCountingListener(listener.map(unused -> blobFileRanges))) {
            // Read/warm header(s) of every referenced blob
            for (var referencedBlob : referencedBlobs.entrySet()) {
                executor.execute(ActionRunnable.run(listeners.acquire(), () -> {
                    var blobTermAndGen = referencedBlob.getKey();
                    var blobLengthAndFiles = referencedBlob.getValue();

                    BatchedCompoundCommit bcc = blobTermAndGen.equals(latestBcc.primaryTermAndGeneration()) == false
                        ? readBatchedCompoundCommitUsingCache(directory, context, blobTermAndGen, blobLengthAndFiles.maxBlobLength(), false)
                        : latestBcc;
                    blobFileRanges.putAll(computeBlobFileRanges(bcc, blobLengthAndFiles.files(), useReplicatedRanges));
                }));
            }
        }
    }

    private record ReferencedBlobMaxBlobLengthAndFiles(long maxBlobLength, Set<String> files) {}

    private static void logLatestBcc(BatchedCompoundCommit latestBcc, BlobContainer blobContainer) {
        if (logger.isTraceEnabled()) {
            logger.trace(
                "found latest CC in [{}]: {}",
                blobContainer.path().buildAsString(),
                latestBcc.lastCompoundCommit().toLongDescription()
            );
        }
    }

    /**
     * Abstract class for commit and files upload tasks.
     *
     * Tasks belonging to the same shard are ordered by commit generation. Tasks belonging to different shards are ordered by the commit
     * enqueue time.
     */
    private abstract static class ObjectStoreTask extends AbstractRunnable implements Comparable<ObjectStoreTask> {

        protected final ShardId shardId;
        protected final long generation;
        protected final long timeInNanos;

        ObjectStoreTask(ShardId shardId, long generation, long timeInNanos) {
            this.shardId = Objects.requireNonNull(shardId);
            this.generation = generation;
            this.timeInNanos = timeInNanos;
        }

        @Override
        public final int compareTo(ObjectStoreTask other) {
            if (shardId.equals(other.shardId)) {
                return Long.compare(generation, other.generation);
            }

            // Nano time requires relative comparisons
            long diff = timeInNanos - other.timeInNanos;
            if (diff == 0) {
                return 0;
            } else if (diff > 0) {
                return 1;
            } else {
                return -1;
            }
        }
    }

    /**
     * {@link TranslogFileUploadTask} uploads the compound translog file to the object store
     */
    private class TranslogFileUploadTask extends AbstractRunnable implements Comparable<TranslogFileUploadTask> {

        private final String fileName;
        private final BytesReference reference;
        private final ActionListener<Void> listener;

        TranslogFileUploadTask(String fileName, BytesReference reference, ActionListener<Void> listener) {
            this.fileName = Objects.requireNonNull(fileName);
            this.reference = Objects.requireNonNull(reference);
            this.listener = Objects.requireNonNull(listener);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        protected void doRun() throws Exception {
            final BlobContainer blobContainer = getTranslogBlobContainer();

            var before = threadPool.relativeTimeInMillis();
            blobContainer.writeBlob(OperationPurpose.TRANSLOG, fileName, reference, false);
            var after = threadPool.relativeTimeInMillis();
            logger.debug(
                () -> format(
                    "translog file %s of size [%s] bytes uploaded in [%s] ms",
                    blobContainer.path().add(fileName),
                    reference.length(),
                    TimeValue.timeValueNanos(after - before).millis()
                )
            );
            listener.onResponse(null);
        }

        @Override
        public int compareTo(TranslogFileUploadTask o) {
            // TODO: Implement
            return 0;
        }
    }

    private class BatchedCommitFileUploadTask extends ObjectStoreTask {
        private final VirtualBatchedCompoundCommit virtualBatchedCompoundCommit;
        private final BlobContainer blobContainer;
        private final ActionListener<Void> listener;

        BatchedCommitFileUploadTask(
            long timeInNanos,
            VirtualBatchedCompoundCommit virtualBatchedCompoundCommit,
            BlobContainer blobContainer,
            ActionListener<Void> listener
        ) {
            super(
                virtualBatchedCompoundCommit.getShardId(),
                virtualBatchedCompoundCommit.getPrimaryTermAndGeneration().generation(),
                timeInNanos
            );
            this.virtualBatchedCompoundCommit = Objects.requireNonNull(virtualBatchedCompoundCommit);
            this.blobContainer = Objects.requireNonNull(blobContainer);
            this.listener = Objects.requireNonNull(listener);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        protected void doRun() {
            boolean success = false;
            try {
                var before = threadPool.relativeTimeInMillis();
                try (var vbccInputStream = virtualBatchedCompoundCommit.getFrozenInputStreamForUpload()) {
                    blobContainer.writeBlobAtomic(
                        OperationPurpose.INDICES,
                        virtualBatchedCompoundCommit.getBlobName(),
                        vbccInputStream,
                        virtualBatchedCompoundCommit.getTotalSizeInBytes(),
                        false
                    );
                }
                var after = threadPool.relativeTimeInMillis();
                logger.debug(
                    () -> format(
                        "%s file %s of size [%s] bytes from batched compound commit [%s] uploaded in [%s] ms",
                        shardId,
                        blobContainer.path().add(virtualBatchedCompoundCommit.getBlobName()),
                        virtualBatchedCompoundCommit.getTotalSizeInBytes(),
                        generation,
                        TimeValue.timeValueNanos(after - before).millis()
                    )
                );
                // assign this last, since it is used as a flag to successfully complete the listener below.
                // this is critically important, since completing the listener successfully for a failed write can lead to
                // erroneously deleted files.
                success = true;
            } catch (IOException e) {
                // TODO GoogleCloudStorageBlobStore should throw IOException too (https://github.com/elastic/elasticsearch/issues/92357)
                onFailure(e);
            } finally {
                if (success) {
                    listener.onResponse(null);
                }
            }
        }
    }

    private class FileDeleteTask extends AbstractRunnable {

        private final Supplier<BlobContainer> blobContainer;
        private final ArrayList<String> toDeleteInThisTask;

        private FileDeleteTask(Supplier<BlobContainer> blobContainer) {
            this.toDeleteInThisTask = new ArrayList<>();
            this.blobContainer = blobContainer;
            for (int i = 0; i < DELETE_BATCH_SIZE; ++i) {
                String polled = translogBlobsToDelete.poll();
                if (polled != null) {
                    toDeleteInThisTask.add(polled);
                } else {
                    break;
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            // Might be 100 files only log when debug enabled
            if (logger.isDebugEnabled()) {
                logger.warn(() -> format("exception while attempting to delete blob files [%s]", toDeleteInThisTask), e);
            } else {
                logger.warn("exception while attempting to delete blob files", e);
            }
        }

        @Override
        public void onAfter() {
            translogDeleteSchedulePermit.release();
            if (translogBlobsToDelete.isEmpty() == false && translogDeleteSchedulePermit.tryAcquire()) {
                threadPool.executor(Stateless.TRANSLOG_THREAD_POOL).execute(new FileDeleteTask(blobContainer));
            }
        }

        @Override
        protected void doRun() throws Exception {
            boolean success = false;
            try {
                blobContainer.get().deleteBlobsIgnoringIfNotExists(OperationPurpose.TRANSLOG, toDeleteInThisTask.iterator());
                logger.debug(() -> format("deleted translog files %s", toDeleteInThisTask));
                success = true;
            } finally {
                if (success == false) {
                    translogBlobsToDelete.addAll(toDeleteInThisTask);
                }
            }
        }
    }

    private class ShardFilesDeleteTask extends AbstractRunnable {
        private final List<StaleCompoundCommit> toDeleteInThisTask;

        private ShardFilesDeleteTask() {
            this.toDeleteInThisTask = new ArrayList<>();
            for (int i = 0; i < DELETE_BATCH_SIZE; ++i) {
                StaleCompoundCommit polled = commitBlobsToDelete.poll();
                if (polled != null) {
                    toDeleteInThisTask.add(polled);
                } else {
                    break;
                }
            }
        }

        @Override
        public void onFailure(Exception e) {
            final var level = lifecycle.started() ? Level.WARN : Level.DEBUG;
            if (logger.isDebugEnabled()) {
                // Might be 100 files, only log when debug enabled
                try {
                    logger.log(level, () -> format("exception while attempting to delete blob files [%s]", blobPathStream().toList()), e);
                } catch (Exception blobException) {
                    e.addSuppressed(blobException);
                    logger.log(level, () -> "exception while attempting to delete blob files (and could not list blob files)", e);
                }
            } else {
                logger.log(level, () -> "exception while attempting to delete blob files", e);
            }
        }

        @Override
        public void onAfter() {
            shardFileDeleteSchedulePermit.release();
            if (lifecycle.started() && commitBlobsToDelete.isEmpty() == false && shardFileDeleteSchedulePermit.tryAcquire()) {
                threadPool.executor(Stateless.SHARD_WRITE_THREAD_POOL).execute(new ShardFilesDeleteTask());
            }
        }

        @Override
        public void onRejection(Exception e) {
            assert e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown() : e;
            assert lifecycle.closed() : lifecycle;
            // no need to retry or even log, we're shutting down
        }

        @Override
        protected void doRun() throws Exception {
            boolean success = false;
            try {
                getObjectStore().blobStore().deleteBlobsIgnoringIfNotExists(OperationPurpose.INDICES, blobPathStream().iterator());
                SHARD_FILES_DELETES_LOGGER.debug(() -> format("deleted shard files %s", blobPathStream().toList()));
                success = true;
            } finally {
                if (success == false) {
                    commitBlobsToDelete.addAll(toDeleteInThisTask);
                }
            }
        }

        private Stream<String> blobPathStream() {
            return toDeleteInThisTask.stream()
                .map(commit -> commit.absoluteBlobPath(getBlobContainer(commit.shardId(), commit.primaryTerm()).path()));
        }
    }
}
