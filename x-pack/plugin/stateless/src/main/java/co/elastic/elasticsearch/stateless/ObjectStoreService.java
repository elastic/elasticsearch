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

import co.elastic.elasticsearch.stateless.action.NewCommitNotificationAction;
import co.elastic.elasticsearch.stateless.action.NewCommitNotificationRequest;
import co.elastic.elasticsearch.stateless.lucene.StatelessCommitRef;

import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.store.InputStreamIndexInput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.PrioritizedThrottledTaskRunner;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.MultiFileWriter;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Semaphore;
import java.util.function.BiConsumer;
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

    public enum ObjectStoreType {
        FS((bucket, builder) -> builder.put("location", bucket), (client, builder) -> {}, false),
        S3((bucket, builder) -> builder.put("bucket", bucket), (client, builder) -> builder.put("client", client), true),
        GCS((bucket, builder) -> builder.put("bucket", bucket), (client, builder) -> builder.put("client", client), true),
        AZURE((bucket, builder) -> builder.put("container", bucket), (client, builder) -> builder.put("client", client), true);

        private final BiConsumer<String, Settings.Builder> bucketConsumer;
        private final BiConsumer<String, Settings.Builder> clientConsumer;
        private final boolean needsClient;

        ObjectStoreType(
            BiConsumer<String, Settings.Builder> bucketConsumer,
            BiConsumer<String, Settings.Builder> clientConsumer,
            boolean needsClient
        ) {
            this.bucketConsumer = bucketConsumer;
            this.clientConsumer = clientConsumer;
            this.needsClient = needsClient;
        }

        public Settings repositorySettings(String bucket, String client) {
            Settings.Builder builder = Settings.builder();
            bucketConsumer.accept(bucket, builder);
            clientConsumer.accept(client, builder);
            return builder.build();
        }

        public boolean needsClient() {
            return needsClient;
        }

        @Override
        public String toString() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    private static final List<Setting<?>> TYPE_VALIDATOR_SETTINGS_LIST = List.of(BUCKET_SETTING, CLIENT_SETTING);
    public static final Setting<ObjectStoreType> TYPE_SETTING = Setting.enumSetting(
        ObjectStoreType.class,
        "stateless.object_store.type",
        ObjectStoreType.FS,
        new Setting.Validator<ObjectStoreType>() {
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
                if (value.needsClient()) {
                    if (client.isEmpty()) {
                        throw new IllegalArgumentException(
                            "setting " + CLIENT_SETTING.getKey() + " must be set for an object store of type " + value
                        );
                    }
                }
            }

            @Override
            public Iterator<Setting<?>> settings() {
                return TYPE_VALIDATOR_SETTINGS_LIST.iterator();
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
    private final PrioritizedThrottledTaskRunner<ObjectStoreTask> uploadTaskRunner;
    private final PrioritizedThrottledTaskRunner<ObjectStoreTask> downloadTaskRunner;
    private final Semaphore permits;

    private BlobStoreRepository objectStore;

    private final Client client;
    private final ClusterService clusterService;

    @Inject
    public ObjectStoreService(
        Settings settings,
        Supplier<RepositoriesService> repositoriesServiceSupplier,
        ThreadPool threadPool,
        Client client,
        ClusterService clusterService
    ) {
        this.settings = settings;
        this.repositoriesServiceSupplier = repositoriesServiceSupplier;
        this.threadPool = threadPool;
        this.client = client;
        this.clusterService = clusterService;
        this.uploadTaskRunner = new PrioritizedThrottledTaskRunner<>(
            getClass().getSimpleName() + "#upload-task-runner",
            threadPool.info(ThreadPool.Names.SNAPSHOT).getMax(),
            threadPool.executor(ThreadPool.Names.SNAPSHOT) // TODO use dedicated object store thread pool
        );
        this.downloadTaskRunner = new PrioritizedThrottledTaskRunner<>(
            getClass().getSimpleName() + "#download-task-runner",
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

    private static RepositoryMetadata getRepositoryMetadata(Settings settings) {
        ObjectStoreType type = TYPE_SETTING.get(settings);
        String bucket = BUCKET_SETTING.get(settings);
        String client = CLIENT_SETTING.get(settings);

        return new RepositoryMetadata(Stateless.NAME, type.toString(), type.repositorySettings(bucket, client));
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
        protected void doRun() throws Exception {
            boolean success = false;
            try {
                final Collection<String> additionalFiles = reference.getAdditionalFiles();
                logger.trace("{} uploading commit [{}] with [{}] additional files", shardId, generation, additionalFiles.size());

                // this listener releases the reference on the index commit and a permit once all commit files are uploaded
                final ActionListener<Collection<FileUploadTask.Result>> releaseCommitlistener = ActionListener.runBefore(
                    ActionListener.wrap(results -> {
                        final long end = threadPool.relativeTimeInNanos();
                        logger.debug(
                            () -> format(
                                "%s commit [%s] uploaded in [%s] ms (%s files, %s total bytes)",
                                shardId,
                                generation,
                                TimeValue.nsecToMSec(end - timeInNanos),
                                results.size(),
                                results.stream().mapToLong(FileUploadTask.Result::length).sum()
                            )
                        );

                        NewCommitNotificationRequest request = new NewCommitNotificationRequest(
                            shardId,
                            true,
                            reference.getPrimaryTerm(),
                            generation,
                            reference.getCommitFiles()
                        );
                        client.execute(NewCommitNotificationAction.INSTANCE, request);
                    }, e -> logger.error(() -> format("%s failed to upload files of commit [%s] to object store", shardId, generation), e)),
                    () -> IOUtils.close(reference, releasable)
                );

                // Use a basic upload strategy where every file is always uploaded in a dedicated FileTask. Here we could split large files
                // into multiple FileChunkTask of similar sizes and/or combine multiple small files into one single FileTask. We could also
                // prioritize FileTask depending on the index. Note that the segments_N file is uploaded last after all other files of the
                // commit have been successfully uploaded, so that a process listing the content of the bucket in the object store will
                // be able to access a consistent set of commit files (assuming read after write consistency).
                final BlobContainer blobContainer = getBlobContainer(shardId, reference.getPrimaryTerm());

                if (additionalFiles.size() == 1) {
                    assert additionalFiles.stream().allMatch(f -> f.startsWith(IndexFileNames.SEGMENTS)) : additionalFiles;
                    uploadTaskRunner.enqueueTask(
                        new FileUploadTask(
                            shardId,
                            generation,
                            timeInNanos,
                            reference.getSegmentsFileName(),
                            reference.getDirectory(),
                            blobContainer,
                            ActionListener.wrap(
                                result -> releaseCommitlistener.onResponse(List.of(result)),
                                releaseCommitlistener::onFailure
                            )
                        )
                    );
                } else {
                    final CountDown countDown = new CountDown(additionalFiles.size() - 1);
                    assert additionalFiles.stream().filter(f -> f.startsWith(IndexFileNames.SEGMENTS)).count() == 1L : additionalFiles;
                    var groupedListener = new GroupedActionListener<>(additionalFiles.size(), releaseCommitlistener);
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
                                    ActionListener.runAfter(groupedListener, () -> {
                                        if (countDown.countDown()) {
                                            uploadTaskRunner.enqueueTask(
                                                new FileUploadTask(
                                                    shardId,
                                                    generation,
                                                    timeInNanos,
                                                    reference.getSegmentsFileName(),
                                                    reference.getDirectory(),
                                                    blobContainer,
                                                    groupedListener
                                                )
                                            );
                                        }
                                    })
                                )
                            )
                        );
                }
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
        private final ActionListener<Result> listener;

        FileUploadTask(
            ShardId shardId,
            long generation,
            long timeInNanos,
            String name,
            Directory directory,
            BlobContainer blobContainer,
            ActionListener<Result> listener
        ) {
            super(shardId, generation, timeInNanos);
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
            // TODO Retry
            // TODO Rate limit or some type of throttling?
            // TODO Are there situations where we need to abort an upload?

            Result result = null;
            try (ChecksumIndexInput input = directory.openChecksumInput(name, IOContext.READONCE)) {
                final long length = input.length();
                var before = threadPool.relativeTimeInMillis();
                blobContainer.writeBlob(name, new InputStreamIndexInput(input, length), length, false);
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

    public void onNewCommitReceived(
        final ShardId shardId,
        final long primaryTerm,
        final long generation,
        final Map<String, StoreFileMetadata> toDownload,
        final MultiFileWriter multiFileWriter,
        final ActionListener<Void> listener
    ) {
        logger.debug("{} downloading new commit [{}]", shardId, generation);
        ensureRunning();
        if (permits.tryAcquire()) {
            downloadTaskRunner.enqueueTask(
                new CommitDownloadTask(
                    multiFileWriter,
                    toDownload,
                    shardId,
                    primaryTerm,
                    generation,
                    threadPool.relativeTimeInNanos(),
                    ActionListener.runAfter(listener, permits::release)
                )
            );
        }
    }

    private class CommitDownloadTask extends ObjectStoreTask {

        private final Map<String, StoreFileMetadata> files;
        private final MultiFileWriter multiFileWriter;
        private final ShardId shardId;
        private final long primaryTerm;
        private final ActionListener<Void> listener;

        CommitDownloadTask(
            MultiFileWriter multiFileWriter,
            Map<String, StoreFileMetadata> files,
            ShardId shardId,
            long primaryTerm,
            long generation,
            long timeInNanos,
            ActionListener<Void> listener
        ) {
            super(shardId, generation, timeInNanos);
            this.files = files;
            this.shardId = shardId;
            this.primaryTerm = primaryTerm;
            this.multiFileWriter = multiFileWriter;
            this.listener = listener;
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        protected void doRun() throws Exception {
            logger.trace("{} downloading commit [{}] with [{}] new files", shardId, generation, files.size());

            final ActionListener<Collection<Result>> finalListener = ActionListener.wrap(results -> {
                final long end = threadPool.relativeTimeInNanos();
                logger.debug(
                    () -> format(
                        "%s commit [%s] downloaded in [%s] ms (%s files, %s total bytes)",
                        shardId,
                        generation,
                        TimeValue.nsecToMSec(end - timeInNanos),
                        results.size(),
                        results.stream().mapToLong(Result::length).reduce(Math::addExact).getAsLong()
                    )
                );
                listener.onResponse(null);
            }, e -> {
                logger.error(() -> format("%s failed to download files of commit [%s] to object store", shardId, generation), e);
                listener.onFailure(e);
            });
            if (files.isEmpty()) {
                finalListener.onResponse(null);
                return;
            }

            var groupedListener = new GroupedActionListener<>(files.size(), finalListener);
            try {

                final BlobContainer blobContainer = getBlobContainer(shardId, primaryTerm);

                files.entrySet()
                    .forEach(
                        file -> downloadTaskRunner.enqueueTask(
                            new FileDownloadTask(shardId, generation, timeInNanos, file, multiFileWriter, blobContainer, groupedListener)
                        )
                    );
            } catch (Exception e) {
                finalListener.onFailure(e);
            }
        }
    }

    private class FileDownloadTask extends ObjectStoreTask {

        private final String objectStoreFileName;
        private final StoreFileMetadata fileMetadata;
        private final MultiFileWriter multiFileWriter;
        private final BlobContainer blobContainer;
        private final ActionListener<Result> listener;

        FileDownloadTask(
            ShardId shardId,
            long generation,
            long startTimeInNanos,
            Map.Entry<String, StoreFileMetadata> fileToDownload,
            MultiFileWriter multiFileWriter,
            BlobContainer blobContainer,
            ActionListener<Result> listener
        ) {
            super(shardId, generation, startTimeInNanos);
            this.objectStoreFileName = Objects.requireNonNull(fileToDownload.getKey());
            this.fileMetadata = Objects.requireNonNull(fileToDownload.getValue());
            this.multiFileWriter = Objects.requireNonNull(multiFileWriter);
            this.blobContainer = Objects.requireNonNull(blobContainer);
            this.listener = Objects.requireNonNull(listener);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        protected void doRun() throws Exception {

            try (InputStream inputStream = blobContainer.readBlob(objectStoreFileName)) {
                int readBufferSizeInBytes = getObjectStore().getReadBufferSizeInBytes();
                blobContainer.readBlobPreferredLength();
                var before = threadPool.relativeTimeInMillis();
                multiFileWriter.writeFile(fileMetadata, readBufferSizeInBytes, inputStream);
                var after = threadPool.relativeTimeInMillis();
                logger.debug(
                    () -> format(
                        "%s file %s of size [%s] bytes from commit [%s] uploaded in [%s] ms",
                        shardId,
                        blobContainer.path().add(objectStoreFileName),
                        fileMetadata.length(),
                        generation,
                        TimeValue.timeValueNanos(after - before).millis()
                    )
                );
                listener.onResponse(new Result(objectStoreFileName, fileMetadata.length(), after - before));
            } catch (Exception e) {
                listener.onFailure(e);
            }
        }

    }

    record Result(String name, long length, long elapsedInMillis) {}
}
