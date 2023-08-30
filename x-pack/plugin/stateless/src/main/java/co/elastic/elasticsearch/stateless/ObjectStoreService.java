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

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.commits.BlobFile;
import co.elastic.elasticsearch.stateless.commits.BlobLocation;
import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;
import co.elastic.elasticsearch.stateless.lucene.SearchDirectory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.InputStreamStreamInput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.PrioritizedThrottledTaskRunner;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;

public class ObjectStoreService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(ObjectStoreService.class);

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

    public static final Setting<TimeValue> OBJECT_STORE_SHUTDOWN_TIMEOUT = Setting.timeSetting(
        "stateless.object_store.shutdown_timeout",
        TimeValue.timeValueSeconds(10L),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    private static final int UPLOAD_PERMITS = Integer.MAX_VALUE;

    private final Settings settings;
    private final Supplier<RepositoriesService> repositoriesServiceSupplier;
    private final ThreadPool threadPool;
    private final PrioritizedThrottledTaskRunner<TranslogFileUploadTask> uploadTranslogTaskRunner;
    private final PrioritizedThrottledTaskRunner<ObjectStoreTask> uploadTaskRunner;
    private final ConcurrentLinkedQueue<String> translogBlobsToDelete = new ConcurrentLinkedQueue<>();
    private final Semaphore translogDeleteSchedulePermit = new Semaphore(1);
    private final Semaphore permits;

    private BlobStoreRepository objectStore;

    private final ClusterService clusterService;

    @Inject
    public ObjectStoreService(
        Settings settings,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        this.settings = settings;
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.uploadTranslogTaskRunner = new PrioritizedThrottledTaskRunner<>(
            getClass().getSimpleName() + "#upload-translog-file-task-runner",
            threadPool.info(ThreadPool.Names.SNAPSHOT).getMax(),
            threadPool.executor(ThreadPool.Names.SNAPSHOT) // TODO use dedicated object store thread pool
        );
        this.uploadTaskRunner = new PrioritizedThrottledTaskRunner<>(
            getClass().getSimpleName() + "#upload-task-runner",
            threadPool.info(ThreadPool.Names.SNAPSHOT).getMax(),
            threadPool.executor(ThreadPool.Names.SNAPSHOT) // TODO use dedicated object store thread pool
        );
        this.permits = new Semaphore(0);
    }

    private RepositoriesService getRepositoriesService() {
        return Objects.requireNonNull(repositoriesServiceSupplier.get());
    }

    public BlobStoreRepository getObjectStore() {
        return Objects.requireNonNull(objectStore);
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

    public BlobContainer getTermLeaseBlobContainer() {
        final BlobStoreRepository objectStore = getObjectStore();
        return objectStore.blobStore().blobContainer(objectStore.basePath().add("cluster_state"));
    }

    public BlobContainer getClusterStateBlobContainerForTerm(long term) {
        final BlobStoreRepository objectStore = getObjectStore();
        return objectStore.blobStore().blobContainer(objectStore.basePath().add("cluster_state").add(String.valueOf(term)));
    }

    public BlobContainer getLeaderHeartbeatContainer() {
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
        final BlobStoreRepository objectStore = getObjectStore();
        return objectStore.blobStore().blobContainer(objectStore.basePath().add("nodes").add(nodeEphemeralId).add("translog"));
    }

    private static RepositoryMetadata getRepositoryMetadata(Settings settings) {
        ObjectStoreType type = TYPE_SETTING.get(settings);
        return new RepositoryMetadata(
            Stateless.NAME,
            type.toString(),
            type.createRepositorySettings(BUCKET_SETTING.get(settings), CLIENT_SETTING.get(settings), BASE_PATH_SETTING.get(settings))
        );
    }

    @Override
    protected void doStart() {
        assert objectStore == null;
        Repository repository = getRepositoriesService().createRepository(getRepositoryMetadata(settings));
        assert repository instanceof BlobStoreRepository;
        this.objectStore = (BlobStoreRepository) repository;
        this.objectStore.start();
        this.permits.release(UPLOAD_PERMITS);
        logger.info(
            "started object store service with type [{}], bucket [{}], client [{}]",
            TYPE_SETTING.get(settings),
            BUCKET_SETTING.get(settings),
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
        logger.debug("starting translog file upload [{}]", fileName);
        enqueueTask(listener, uploadTranslogTaskRunner, l -> new TranslogFileUploadTask(fileName, reference, l));
    }

    public void uploadStatelessCommitFile(
        ShardId shardId,
        long primaryTerm,
        long generation,
        Directory directory,
        String commitFileName,
        long commitStartNanos,
        StatelessCompoundCommit.Writer pendingCommit,
        ActionListener<StatelessCompoundCommit> listener
    ) {
        enqueueTask(
            listener,
            uploadTaskRunner,
            l -> new CommitFileUploadTask(
                shardId,
                generation,
                commitStartNanos,
                commitFileName,
                directory,
                pendingCommit,
                SearchDirectory.unwrapDirectory(directory).getBlobContainer(primaryTerm),
                l
            )
        );
    }

    public void asyncDeleteTranslogFile(String fileToDelete) {
        logger.debug("scheduling translog blob file for async delete [{}]", fileToDelete);
        translogBlobsToDelete.add(fileToDelete);
        if (translogDeleteSchedulePermit.tryAcquire()) {
            threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new FileDeleteTask(this::getTranslogBlobContainer));
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

    private static StatelessCompoundCommit readNewestCommit(BlobContainer blobContainer, Map<String, BlobMetadata> allBlobs)
        throws IOException {
        OptionalLong maxGeneration = allBlobs.keySet()
            .stream()
            .filter(StatelessCompoundCommit::startsWithBlobPrefix)
            .mapToLong(StatelessCompoundCommit::parseGenerationFromBlobName)
            .max();
        if (maxGeneration.isEmpty()) {
            return null;
        }
        String commitFileName = StatelessCompoundCommit.blobNameFromGeneration(maxGeneration.getAsLong());
        try (StreamInput streamInput = new InputStreamStreamInput(blobContainer.readBlob(commitFileName))) {
            long fileLength = allBlobs.get(commitFileName).length();
            return StatelessCompoundCommit.readFromStore(streamInput, fileLength);
        }
    }

    public static StatelessCompoundCommit readSearchShardState(BlobContainer shardContainer, long primaryTerm) throws IOException {
        StatelessCompoundCommit latestCommit = null;
        List<Tuple<Long, BlobContainer>> containersToSearch = shardContainer.children()
            .entrySet()
            .stream()
            .map(e -> new Tuple<>(Long.valueOf(e.getKey()), e.getValue()))
            .filter(t -> t.v1() <= primaryTerm)
            .sorted(Comparator.comparingLong((Tuple<Long, BlobContainer> o) -> o.v1()).reversed())
            .toList();
        for (Tuple<Long, BlobContainer> container : containersToSearch) {
            latestCommit = ObjectStoreService.readNewestCommit(container.v2(), container.v2().listBlobs());
            if (latestCommit != null) {
                break;
            }
        }
        return latestCommit;
    }

    public static Tuple<StatelessCompoundCommit, Set<BlobFile>> readIndexingShardState(BlobContainer shardContainer, long primaryTerm)
        throws IOException {
        HashMap<String, BlobFile> unreferencedBlobs = new HashMap<>();
        StatelessCompoundCommit latestCommit = null;
        List<Tuple<Long, BlobContainer>> containersToSearch = shardContainer.children().entrySet().stream().filter(e -> {
            try {
                Long.parseLong(e.getKey());
                return true;
            } catch (NumberFormatException ex) {
                return false;
            }
        })
            .map(e -> new Tuple<>(Long.parseLong(e.getKey()), e.getValue()))
            .filter(t -> t.v1() <= primaryTerm)
            .sorted(Comparator.comparingLong((Tuple<Long, BlobContainer> o) -> o.v1()).reversed())
            .toList();
        for (Tuple<Long, BlobContainer> container : containersToSearch) {
            Map<String, BlobMetadata> allBlobs = container.v2().listBlobs();
            if (latestCommit == null) {
                latestCommit = ObjectStoreService.readNewestCommit(container.v2(), allBlobs);
                if (latestCommit != null) {
                    allBlobs.entrySet()
                        .stream()
                        .map(entry -> new BlobFile(container.v1(), entry.getKey(), entry.getValue().length()))
                        .forEach(f -> unreferencedBlobs.put(f.blobName(), f));
                    BlobFile removed = unreferencedBlobs.remove(StatelessCompoundCommit.blobNameFromGeneration(latestCommit.generation()));
                    assert removed != null;
                }
            } else {
                allBlobs.values()
                    .stream()
                    .map(metadata -> new BlobFile(container.v1(), metadata.name(), metadata.length()))
                    .forEach(f -> unreferencedBlobs.put(f.blobName(), f));
            }
        }
        return new Tuple<>(latestCommit, new HashSet<>(unreferencedBlobs.values()));
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
            logFailure(e);
            listener.onFailure(e);
        }

        @Override
        protected void doRun() throws Exception {
            final BlobContainer blobContainer = getTranslogBlobContainer();

            var before = threadPool.relativeTimeInMillis();
            blobContainer.writeBlob(fileName, reference, false);
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

        private void logFailure(Exception e) {
            logger.error(() -> format("failed to translog file [%s] to object store", fileName), e);
        }

        @Override
        public int compareTo(TranslogFileUploadTask o) {
            // TODO: Implement
            return 0;
        }
    }

    /**
     * {@link FileUploadTask} uploads a blob to the object store
     */
    private class FileUploadTask extends ObjectStoreTask {

        private final long primaryTerm;
        private final String name;
        private final Directory directory;
        private final BlobContainer blobContainer;
        private final ActionListener<BlobLocation> listener;

        FileUploadTask(
            ShardId shardId,
            long primaryTerm,
            long generation,
            long timeInNanos,
            String name,
            Directory directory,
            BlobContainer blobContainer,
            ActionListener<BlobLocation> listener
        ) {
            super(shardId, generation, timeInNanos);
            this.primaryTerm = primaryTerm;
            this.name = Objects.requireNonNull(name);
            this.directory = Objects.requireNonNull(directory);
            this.blobContainer = Objects.requireNonNull(blobContainer);
            this.listener = Objects.requireNonNull(listener);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        protected void doRun() throws Exception {
            // TODO Rate limit or some type of throttling?
            BlobLocation blobLocation = null;
            try (IndexInput input = directory.openInput(name, IOContext.READONCE)) {
                final long length = input.length();
                var before = threadPool.relativeTimeInMillis();
                final InputStream inputStream = new InputStreamIndexInput(input, length);
                blobContainer.writeBlob(name, inputStream, length, false);
                var after = threadPool.relativeTimeInMillis();
                logger.debug(
                    () -> format(
                        "%s file %s of size [%s] bytes from commit [%s] uploaded in [%s] ms",
                        shardId,
                        blobContainer.path().add(name),
                        length,
                        generation,
                        TimeValue.timeValueNanos(after - before).millis()
                    )
                );
                blobLocation = new BlobLocation(primaryTerm, name, length, 0, length);
            } catch (IOException e) {
                // TODO GoogleCloudStorageBlobStore should throw IOException too (https://github.com/elastic/elasticsearch/issues/92357)
                onFailure(e);
            } finally {
                if (blobLocation != null) {
                    listener.onResponse(blobLocation);
                }
            }
        }
    }

    /**
     * {@link FileUploadTask} uploads a blob to the object store
     */
    private class CommitFileUploadTask extends ObjectStoreTask {

        private final String commitFileName;
        private final Directory directory;
        private final StatelessCompoundCommit.Writer pendingCommit;
        private final BlobContainer blobContainer;
        private final ActionListener<StatelessCompoundCommit> listener;

        CommitFileUploadTask(
            ShardId shardId,
            long generation,
            long timeInNanos,
            String commitFileName,
            Directory directory,
            StatelessCompoundCommit.Writer pendingCommit,
            BlobContainer blobContainer,
            ActionListener<StatelessCompoundCommit> listener
        ) {
            super(shardId, generation, timeInNanos);
            this.commitFileName = Objects.requireNonNull(commitFileName);
            this.directory = Objects.requireNonNull(directory);
            this.pendingCommit = Objects.requireNonNull(pendingCommit);
            this.blobContainer = Objects.requireNonNull(blobContainer);
            this.listener = Objects.requireNonNull(listener);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        protected void doRun() {
            StatelessCompoundCommit compoundCommit = null;
            try {
                var before = threadPool.relativeTimeInMillis();
                AtomicLong bytesWritten = new AtomicLong();
                // TODO: Ensure that out usage of this method for writing files is appropriate. The javadoc is a bit concerning. "This
                // method is only used for streaming serialization of repository metadata that is known to be of limited size at any point
                // in time and across all concurrent invocations of this method."
                blobContainer.writeMetadataBlob(commitFileName, false, true, out -> {
                    long written = pendingCommit.writeToStore(out, directory);
                    bytesWritten.set(written);
                });
                var after = threadPool.relativeTimeInMillis();
                logger.debug(
                    () -> format(
                        "%s file %s of size [%s] bytes from commit [%s] uploaded in [%s] ms",
                        shardId,
                        blobContainer.path().add(commitFileName),
                        bytesWritten.get(),
                        generation,
                        TimeValue.timeValueNanos(after - before).millis()
                    )
                );
                compoundCommit = pendingCommit.finish(commitFileName);
            } catch (IOException e) {
                // TODO GoogleCloudStorageBlobStore should throw IOException too (https://github.com/elastic/elasticsearch/issues/92357)
                onFailure(e);
            } finally {
                if (compoundCommit != null) {
                    listener.onResponse(compoundCommit);
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
                logger.warn(() -> format("exception while attempting to delete blob files [{}]", toDeleteInThisTask), e);
            } else {
                logger.warn("exception while attempting to delete blob files", e);
            }
        }

        @Override
        public void onAfter() {
            translogDeleteSchedulePermit.release();
            if (translogBlobsToDelete.isEmpty() == false && translogDeleteSchedulePermit.tryAcquire()) {
                threadPool.executor(ThreadPool.Names.SNAPSHOT).execute(new FileDeleteTask(blobContainer));
            }
        }

        @Override
        protected void doRun() throws Exception {
            boolean success = false;
            try {
                blobContainer.get().deleteBlobsIgnoringIfNotExists(toDeleteInThisTask.iterator());
                success = true;
            } finally {
                if (success == false) {
                    translogBlobsToDelete.addAll(toDeleteInThisTask);
                }
            }
        }
    }
}
