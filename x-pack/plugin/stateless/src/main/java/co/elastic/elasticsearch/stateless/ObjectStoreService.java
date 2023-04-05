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

import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.action.TransportNewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.PrioritizedThrottledTaskRunner;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Streams;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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

    private static final String MISSING_CHECKSUM = "_na_";

    public enum ObjectStoreType {
        FS("location") {
            @Override
            @SuppressForbidden(reason = "creates path to external blobstore")
            public Settings createRepositorySettings(String bucket, String client, String basePath) {
                return Settings.builder().put("location", basePath != null ? PathUtils.get(bucket, basePath).toString() : bucket).build();
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
            return this != FS;
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
    private final Semaphore permits;

    private BlobStoreRepository objectStore;

    private final ClusterService clusterService;
    private final Client client;

    @Inject
    public ObjectStoreService(
        Settings settings,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        ThreadPool threadPool,
        ClusterService clusterService,
        Client client
    ) {
        this.settings = settings;
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
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

    // package-private for testing
    BlobContainer getBlobContainer(ShardId shardId, long primaryTerm) {
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

    BlobContainer getTranslogBlobContainer(DiscoveryNode discoveryNode) {
        final BlobStoreRepository objectStore = getObjectStore();
        return objectStore.blobStore()
            .blobContainer(objectStore.basePath().add("nodes").add(discoveryNode.getEphemeralId()).add("translog"));
    }

    public BlobContainer getTermLeaseBlobContainer() {
        final BlobStoreRepository objectStore = getObjectStore();
        return objectStore.blobStore().blobContainer(objectStore.basePath().add("cluster_state"));
    }

    /**
     * Gets the translog blob container of the local node
     */
    public BlobContainer getLocalTranslogBlobContainer() {
        final DiscoveryNode discoveryNode = clusterService.localNode();
        return getTranslogBlobContainer(discoveryNode);
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

    void onCommitCreation(StatelessCommitRef commit) {
        logger.debug("{} commit created [{}][{}]", commit.getShardId(), commit.getSegmentsFileName(), commit.getGeneration());
        ensureRunning();
        if (permits.tryAcquire()) {
            uploadTaskRunner.enqueueTask(new CommitUploadTask(commit, threadPool.relativeTimeInNanos(), permits::release));
        }
    }

    public void uploadTranslogFile(String fileName, BytesReference reference, ActionListener<Void> listener) {
        logger.debug("starting translog file upload [{}]", fileName);
        ensureRunning();
        if (permits.tryAcquire()) {
            uploadTranslogTaskRunner.enqueueTask(
                new TranslogFileUploadTask(fileName, reference, ActionListener.runAfter(listener, permits::release))
            );
        }
    }

    public static Map<String, StoreFileMetadata> findSearchShardFiles(BlobContainer blobContainer) throws IOException {
        // TODO ES-5310 must block the primary from deleting anything while we sort out at which commit to start
        // TODO ES-5258 This looks for a commit from the latest primary term only, is that sufficient?
        final var allBlobs = Map.copyOf(blobContainer.listBlobs());
        if (allBlobs.keySet().stream().noneMatch(s -> s.startsWith(IndexFileNames.SEGMENTS))) {
            return Map.of();
        }
        try (var directory = new SegmentInfoCachingDirectory(blobContainer, allBlobs)) {
            // SegmentInfos#readLatestCommit lists segments_N files, picks the latest, then loads it and all the .si files it mentions:
            final var segmentInfos = SegmentInfos.readLatestCommit(directory);
            // TODO ES-5310 can now notify the primary which commit we're going to be using, allowing cleanup of other commits

            // TODO ES-5258 search shards are not aware of previous primary terms
            final Collection<String> commitFiles = segmentInfos.files(true);
            final var blobs = new HashMap<String, StoreFileMetadata>();
            for (String commitFile : commitFiles) {
                final BlobMetadata blob = allBlobs.get(commitFile);
                if (blob == null) {
                    throw new FileNotFoundException(commitFile + " not found");
                }
                blobs.put(commitFile, toStoreFileMetadata(blob));
            }
            return blobs;
        }
    }

    private static StoreFileMetadata toStoreFileMetadata(BlobMetadata metadata) {
        return new StoreFileMetadata(metadata.name(), metadata.length(), MISSING_CHECKSUM, Version.CURRENT.toString());
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
            return Long.compare(timeInNanos, other.timeInNanos);
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
            final BlobContainer blobContainer = getLocalTranslogBlobContainer();

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
     * {@link CommitUploadTask} expands a commit into one or more file upload tasks.
     */
    private class CommitUploadTask extends ObjectStoreTask {

        protected final StatelessCommitRef reference;
        private final Releasable releasable;

        CommitUploadTask(StatelessCommitRef reference, long timeInNanos, Releasable releasable) {
            super(reference.getShardId(), reference.getGeneration(), timeInNanos);
            this.reference = Objects.requireNonNull(reference);
            this.releasable = releasable;
        }

        @Override
        public void onFailure(Exception e) {
            IOUtils.closeWhileHandlingException(reference, releasable);
            logFailure(e);
        }

        @Override
        protected void doRun() {
            boolean success = false;
            try {
                final Collection<String> additionalFiles = reference.getAdditionalFiles();
                logger.trace("{} uploading commit [{}] with [{}] additional files", shardId, generation, additionalFiles.size());
                assert additionalFiles.stream().filter(f -> f.startsWith(IndexFileNames.SEGMENTS)).count() == 1L : additionalFiles;

                // this listener releases the reference on the index commit and a permit once all commit files are uploaded
                final AtomicLong successCount = new AtomicLong();
                final AtomicLong successSize = new AtomicLong();
                final ActionListener<Object> releaseCommitListener = ActionListener.runBefore(ActionListener.wrap(ignored -> {
                    final long end = threadPool.relativeTimeInNanos();
                    logger.debug(
                        () -> format(
                            "%s commit [%s] uploaded in [%s] ms (%s files, %s total bytes)",
                            shardId,
                            generation,
                            TimeValue.nsecToMSec(end - timeInNanos),
                            successCount.get(),
                            successSize.get()
                        )
                    );

                    NewCommitNotificationRequest request = new NewCommitNotificationRequest(
                        clusterService.state().routingTable().shardRoutingTable(shardId),
                        reference.getPrimaryTerm(),
                        generation,
                        reference.getCommitFiles()
                    );
                    client.execute(TransportNewCommitNotificationAction.TYPE, request);
                }, e -> logger.error(() -> format("%s failed to upload files of commit [%s] to object store", shardId, generation), e)),
                    () -> IOUtils.close(reference, releasable)
                );

                final BlobContainer blobContainer = getBlobContainer(shardId, reference.getPrimaryTerm());

                final Consumer<FileUploadTask.Result> addResult = r -> {
                    successCount.incrementAndGet();
                    successSize.addAndGet(r.length());
                };

                final ActionListener<Void> allCommitFilesListener = releaseCommitListener.delegateFailure((l, v) ->
                // Note that the segments_N file is uploaded last after all other files of the commit have been successfully uploaded, and
                // only if none of the other files failed to upload, so that a process listing the content of the bucket in the object store
                // will be able to access a consistent set of commit files (assuming read after write consistency).
                uploadTaskRunner.enqueueTask(
                    new FileUploadTask(
                        shardId,
                        generation,
                        timeInNanos,
                        reference.getSegmentsFileName(),
                        reference.getDirectory(),
                        blobContainer,
                        true,
                        l.map(r -> {
                            addResult.accept(r);
                            return null;
                        })
                    )
                ));

                final ActionListener<Void> additionalFilesListener = allCommitFilesListener.delegateFailure((l, v) -> {
                    try {
                        // TODO: A list call is as expensive as a put. We should hold a cache here locally of
                        // the currently uploaded files to prevent unnecessary list calls. This will require
                        // a listener on shard close to clean-up necessary cached data. ES-5739 tracks this.
                        Map<String, BlobMetadata> blobMetadataMap = blobContainer.listBlobs();
                        List<String> missingFiles = reference.getCommitFiles()
                            .keySet()
                            .stream()
                            .filter(s -> s.startsWith(IndexFileNames.SEGMENTS) == false)
                            .filter(Predicate.not(blobMetadataMap::containsKey))
                            .collect(Collectors.toList());
                        if (missingFiles.isEmpty()) {
                            l.onResponse(null);
                        } else {
                            enqueueFileUploads(missingFiles, blobContainer, addResult, l);
                        }
                    } catch (IOException e) {
                        l.onFailure(e);
                    }

                });

                enqueueFileUploads(additionalFiles, blobContainer, addResult, additionalFilesListener);

                success = true;
            } catch (Exception e) {
                logFailure(e);
                assert false : e;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(reference, releasable);
                }
            }
        }

        private void enqueueFileUploads(
            Collection<String> additionalFiles,
            BlobContainer blobContainer,
            Consumer<FileUploadTask.Result> addResult,
            ActionListener<Void> listener
        ) {
            try (var listeners = new RefCountingListener(listener)) {
                // Use a basic upload strategy where every file is always uploaded in a dedicated FileTask. Here we could split large
                // files into multiple FileChunkTask of similar sizes and/or combine multiple small files into one single FileTask. We
                // could also prioritize FileTask depending on the index.
                additionalFiles.stream()
                    .filter(file -> file.startsWith(IndexFileNames.SEGMENTS) == false)
                    .forEach(
                        file -> uploadTaskRunner.enqueueTask(
                            new FileUploadTask(
                                shardId,
                                generation,
                                timeInNanos,
                                file,
                                reference.getDirectory(),
                                blobContainer,
                                false,
                                listeners.acquire(addResult)
                            )
                        )
                    );
            }
        }

        private void logFailure(Exception e) {
            logger.error(() -> format("%s failed to upload commit [%s] to object store", shardId, generation), e);
        }
    }

    /**
     * {@link FileUploadTask} uploads a blob to the object store
     */
    private class FileUploadTask extends ObjectStoreTask {

        private final String name;
        private final Directory directory;
        private final BlobContainer blobContainer;
        private final boolean writeAtomic;
        private final ActionListener<Result> listener;

        FileUploadTask(
            ShardId shardId,
            long generation,
            long timeInNanos,
            String name,
            Directory directory,
            BlobContainer blobContainer,
            boolean writeAtomic,
            ActionListener<Result> listener
        ) {
            super(shardId, generation, timeInNanos);
            this.name = Objects.requireNonNull(name);
            this.directory = Objects.requireNonNull(directory);
            this.blobContainer = Objects.requireNonNull(blobContainer);
            this.writeAtomic = writeAtomic;
            this.listener = Objects.requireNonNull(listener);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        protected void doRun() throws Exception {
            // TODO Retry
            // TODO Rate limit or some type of throttling?
            // TODO Are there situations where we need to abort an upload?

            Result result = null;
            try (ChecksumIndexInput input = directory.openChecksumInput(name, IOContext.READONCE)) {
                final long length = input.length();
                var before = threadPool.relativeTimeInMillis();
                final InputStream inputStream = new InputStreamIndexInput(input, length);
                if (writeAtomic) {
                    blobContainer.writeMetadataBlob(name, false, true, out -> Streams.copy(inputStream, out, false));
                } else {
                    blobContainer.writeBlob(name, inputStream, length, false);
                }
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
                result = new Result(name, length, Store.digestToString(input.getChecksum()), after - before);
            } catch (IOException e) {
                // TODO GoogleCloudStorageBlobStore should throw IOException too (https://github.com/elastic/elasticsearch/issues/92357)
                onFailure(e);
            } finally {
                if (result != null) {
                    listener.onResponse(result);
                }
            }

        }

        record Result(String name, long length, String checksum, long elapsedInMillis) {}
    }

}
