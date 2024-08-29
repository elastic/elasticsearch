/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.monitor.jvm.JvmInfo;
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.MeteredBlobStoreRepository;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotsService;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Shared file system implementation of the BlobStoreRepository
 * <p>
 * Shared file system repository supports the following settings
 * <dl>
 * <dt>{@code bucket}</dt><dd>S3 bucket</dd>
 * <dt>{@code base_path}</dt><dd>Specifies the path within bucket to repository data. Defaults to root directory.</dd>
 * <dt>{@code concurrent_streams}</dt><dd>Number of concurrent read/write stream (per repository on each node). Defaults to 5.</dd>
 * <dt>{@code chunk_size}</dt>
 * <dd>Large file can be divided into chunks. This parameter specifies the chunk size. Defaults to not chucked.</dd>
 * <dt>{@code compress}</dt><dd>If set to true metadata files will be stored compressed. Defaults to false.</dd>
 * </dl>
 */
class S3Repository extends MeteredBlobStoreRepository {
    private static final Logger logger = LogManager.getLogger(S3Repository.class);
    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(logger.getName());

    static final String TYPE = "s3";

    /** The access key to authenticate with s3. This setting is insecure because cluster settings are stored in cluster state */
    static final Setting<SecureString> ACCESS_KEY_SETTING = SecureSetting.insecureString("access_key");

    /** The secret key to authenticate with s3. This setting is insecure because cluster settings are stored in cluster state */
    static final Setting<SecureString> SECRET_KEY_SETTING = SecureSetting.insecureString("secret_key");

    /**
     * Default is to use 100MB (S3 defaults) for heaps above 2GB and 5% of
     * the available memory for smaller heaps.
     */
    private static final ByteSizeValue DEFAULT_BUFFER_SIZE = ByteSizeValue.ofBytes(
        Math.max(
            ByteSizeUnit.MB.toBytes(5), // minimum value
            Math.min(ByteSizeUnit.MB.toBytes(100), JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 20)
        )
    );

    static final Setting<String> BUCKET_SETTING = Setting.simpleString("bucket");

    /**
     * When set to true files are encrypted on server side using AES256 algorithm.
     * Defaults to false.
     */
    static final Setting<Boolean> SERVER_SIDE_ENCRYPTION_SETTING = Setting.boolSetting("server_side_encryption", false);

    /**
     * Maximum size of files that can be uploaded using a single upload request.
     */
    static final ByteSizeValue MAX_FILE_SIZE = new ByteSizeValue(5, ByteSizeUnit.GB);

    /**
     * Minimum size of parts that can be uploaded using the Multipart Upload API.
     * (see http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html)
     */
    static final ByteSizeValue MIN_PART_SIZE_USING_MULTIPART = new ByteSizeValue(5, ByteSizeUnit.MB);

    /**
     * Maximum size of parts that can be uploaded using the Multipart Upload API.
     * (see http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html)
     */
    static final ByteSizeValue MAX_PART_SIZE_USING_MULTIPART = MAX_FILE_SIZE;

    /**
     * Maximum size of files that can be uploaded using the Multipart Upload API.
     */
    static final ByteSizeValue MAX_FILE_SIZE_USING_MULTIPART = new ByteSizeValue(5, ByteSizeUnit.TB);

    /**
     * Minimum threshold below which the chunk is uploaded using a single request. Beyond this threshold,
     * the S3 repository will use the AWS Multipart Upload API to split the chunk into several parts, each of buffer_size length, and
     * to upload each part in its own request. Note that setting a buffer size lower than 5mb is not allowed since it will prevents the
     * use of the Multipart API and may result in upload errors. Defaults to the minimum between 100MB and 5% of the heap size.
     */
    static final Setting<ByteSizeValue> BUFFER_SIZE_SETTING = Setting.byteSizeSetting(
        "buffer_size",
        DEFAULT_BUFFER_SIZE,
        MIN_PART_SIZE_USING_MULTIPART,
        MAX_PART_SIZE_USING_MULTIPART
    );

    /**
     * Big files can be broken down into chunks during snapshotting if needed. Defaults to 5tb.
     */
    static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING = Setting.byteSizeSetting(
        "chunk_size",
        MAX_FILE_SIZE_USING_MULTIPART,
        new ByteSizeValue(5, ByteSizeUnit.MB),
        MAX_FILE_SIZE_USING_MULTIPART
    );

    /**
     * Sets the S3 storage class type for the backup files. Values may be standard, reduced_redundancy,
     * standard_ia, onezone_ia and intelligent_tiering. Defaults to standard.
     */
    static final Setting<String> STORAGE_CLASS_SETTING = Setting.simpleString("storage_class");

    /**
     * The S3 repository supports all S3 canned ACLs : private, public-read, public-read-write,
     * authenticated-read, log-delivery-write, bucket-owner-read, bucket-owner-full-control. Defaults to private.
     */
    static final Setting<String> CANNED_ACL_SETTING = Setting.simpleString("canned_acl");

    static final Setting<String> CLIENT_NAME = new Setting<>("client", "default", Function.identity());

    /**
     * Artificial delay to introduce after a snapshot finalization or delete has finished so long as the repository is still using the
     * backwards compatible snapshot format from before
     * {@link org.elasticsearch.snapshots.SnapshotsService#SHARD_GEN_IN_REPO_DATA_VERSION} ({@link IndexVersions#V_7_6_0}).
     * This delay is necessary so that the eventually consistent nature of AWS S3 does not randomly result in repository corruption when
     * doing repository operations in rapid succession on a repository in the old metadata format.
     * This setting should not be adjusted in production when working with an AWS S3 backed repository. Doing so risks the repository
     * becoming silently corrupted. To get rid of this waiting period, either create a new S3 repository or remove all snapshots older than
     * {@link IndexVersions#V_7_6_0} from the repository which will trigger an upgrade of the repository metadata to the new
     * format and disable the cooldown period.
     */
    static final Setting<TimeValue> COOLDOWN_PERIOD = Setting.timeSetting(
        "cooldown_period",
        new TimeValue(3, TimeUnit.MINUTES),
        new TimeValue(0, TimeUnit.MILLISECONDS),
        Setting.Property.Dynamic
    );

    /**
     * Specifies the path within bucket to repository data. Defaults to root directory.
     */
    static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path");

    /**
     * The batch size for DeleteObjects request
     */
    static final Setting<Integer> DELETION_BATCH_SIZE_SETTING = Setting.intSetting(
        "delete_objects_max_size",
        S3BlobStore.MAX_BULK_DELETES,
        1,
        S3BlobStore.MAX_BULK_DELETES
    );

    /**
     * Maximum number of uploads to request for cleanup when doing a snapshot delete.
     */
    static final Setting<Integer> MAX_MULTIPART_UPLOAD_CLEANUP_SIZE = Setting.intSetting(
        "max_multipart_upload_cleanup_size",
        1000,
        0,
        Setting.Property.Dynamic
    );

    private final S3Service service;

    private final String bucket;

    private final ByteSizeValue bufferSize;

    private final ByteSizeValue chunkSize;

    private final boolean serverSideEncryption;

    private final String storageClass;

    private final String cannedACL;

    /**
     * Time period to delay repository operations by after finalizing or deleting a snapshot.
     * See {@link #COOLDOWN_PERIOD} for details.
     */
    private final TimeValue coolDown;

    private final Executor snapshotExecutor;

    private final S3RepositoriesMetrics s3RepositoriesMetrics;

    /**
     * Constructs an s3 backed repository
     */
    S3Repository(
        final RepositoryMetadata metadata,
        final NamedXContentRegistry namedXContentRegistry,
        final S3Service service,
        final ClusterService clusterService,
        final BigArrays bigArrays,
        final RecoverySettings recoverySettings,
        final S3RepositoriesMetrics s3RepositoriesMetrics
    ) {
        super(
            metadata,
            namedXContentRegistry,
            clusterService,
            bigArrays,
            recoverySettings,
            buildBasePath(metadata),
            buildLocation(metadata)
        );
        this.service = service;
        this.s3RepositoriesMetrics = s3RepositoriesMetrics;
        this.snapshotExecutor = threadPool().executor(ThreadPool.Names.SNAPSHOT);

        // Parse and validate the user's S3 Storage Class setting
        this.bucket = BUCKET_SETTING.get(metadata.settings());
        if (Strings.hasLength(bucket) == false) {
            throw new IllegalArgumentException("Invalid S3 bucket name, cannot be null or empty");
        }

        this.bufferSize = BUFFER_SIZE_SETTING.get(metadata.settings());
        this.chunkSize = CHUNK_SIZE_SETTING.get(metadata.settings());

        // We make sure that chunkSize is bigger or equal than/to bufferSize
        if (this.chunkSize.getBytes() < bufferSize.getBytes()) {
            throw new RepositoryException(
                metadata.name(),
                CHUNK_SIZE_SETTING.getKey()
                    + " ("
                    + this.chunkSize
                    + ") can't be lower than "
                    + BUFFER_SIZE_SETTING.getKey()
                    + " ("
                    + bufferSize
                    + ")."
            );
        }

        this.serverSideEncryption = SERVER_SIDE_ENCRYPTION_SETTING.get(metadata.settings());

        this.storageClass = STORAGE_CLASS_SETTING.get(metadata.settings());
        this.cannedACL = CANNED_ACL_SETTING.get(metadata.settings());

        if (S3ClientSettings.checkDeprecatedCredentials(metadata.settings())) {
            // provided repository settings
            deprecationLogger.critical(
                DeprecationCategory.SECURITY,
                "s3_repository_secret_settings",
                "Using s3 access/secret key from repository settings. Instead "
                    + "store these in named clients and the elasticsearch keystore for secure settings."
            );
        }

        coolDown = COOLDOWN_PERIOD.get(metadata.settings());

        logger.debug(
            "using bucket [{}], chunk_size [{}], server_side_encryption [{}], buffer_size [{}], cannedACL [{}], storageClass [{}]",
            bucket,
            chunkSize,
            serverSideEncryption,
            bufferSize,
            cannedACL,
            storageClass
        );
    }

    private static Map<String, String> buildLocation(RepositoryMetadata metadata) {
        return Map.of("base_path", BASE_PATH_SETTING.get(metadata.settings()), "bucket", BUCKET_SETTING.get(metadata.settings()));
    }

    /**
     * Holds a reference to delayed repository operation {@link Scheduler.Cancellable} so it can be cancelled should the repository be
     * closed concurrently.
     */
    private final AtomicReference<Scheduler.Cancellable> finalizationFuture = new AtomicReference<>();

    @Override
    public void finalizeSnapshot(final FinalizeSnapshotContext finalizeSnapshotContext) {
        final FinalizeSnapshotContext wrappedFinalizeContext;
        if (SnapshotsService.useShardGenerations(finalizeSnapshotContext.repositoryMetaVersion()) == false) {
            final ListenableFuture<Void> metadataDone = new ListenableFuture<>();
            wrappedFinalizeContext = new FinalizeSnapshotContext(
                finalizeSnapshotContext.updatedShardGenerations(),
                finalizeSnapshotContext.repositoryStateId(),
                finalizeSnapshotContext.clusterMetadata(),
                finalizeSnapshotContext.snapshotInfo(),
                finalizeSnapshotContext.repositoryMetaVersion(),
                wrapWithWeakConsistencyProtection(ActionListener.runAfter(finalizeSnapshotContext, () -> metadataDone.onResponse(null))),
                info -> metadataDone.addListener(new ActionListener<>() {
                    @Override
                    public void onResponse(Void unused) {
                        finalizeSnapshotContext.onDone(info);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        assert false : e; // never fails
                    }
                })
            );
        } else {
            wrappedFinalizeContext = finalizeSnapshotContext;
        }
        super.finalizeSnapshot(wrappedFinalizeContext);
    }

    /**
     * Wraps given listener such that it is executed with a delay of {@link #coolDown} on the snapshot thread-pool after being invoked.
     * See {@link #COOLDOWN_PERIOD} for details.
     */
    @Override
    protected ActionListener<RepositoryData> wrapWithWeakConsistencyProtection(ActionListener<RepositoryData> listener) {
        final ActionListener<RepositoryData> wrappedListener = ActionListener.runBefore(listener, () -> {
            final Scheduler.Cancellable cancellable = finalizationFuture.getAndSet(null);
            assert cancellable != null;
        });
        return new ActionListener<>() {
            @Override
            public void onResponse(RepositoryData response) {
                logCooldownInfo();
                final Scheduler.Cancellable existing = finalizationFuture.getAndSet(
                    threadPool.schedule(ActionRunnable.wrap(wrappedListener, l -> l.onResponse(response)), coolDown, snapshotExecutor)
                );
                assert existing == null : "Already have an ongoing finalization " + finalizationFuture;
            }

            @Override
            public void onFailure(Exception e) {
                logCooldownInfo();
                final Scheduler.Cancellable existing = finalizationFuture.getAndSet(
                    threadPool.schedule(ActionRunnable.wrap(wrappedListener, l -> l.onFailure(e)), coolDown, snapshotExecutor)
                );
                assert existing == null : "Already have an ongoing finalization " + finalizationFuture;
            }
        };
    }

    private void logCooldownInfo() {
        logger.info(
            "Sleeping for [{}] after modifying repository [{}] because it contains snapshots older than version [{}]"
                + " and therefore is using a backwards compatible metadata format that requires this cooldown period to avoid "
                + "repository corruption. To get rid of this message and move to the new repository metadata format, either remove "
                + "all snapshots older than version [{}] from the repository or create a new repository at an empty location.",
            coolDown,
            metadata.name(),
            SnapshotsService.SHARD_GEN_IN_REPO_DATA_VERSION,
            SnapshotsService.SHARD_GEN_IN_REPO_DATA_VERSION
        );
    }

    private static BlobPath buildBasePath(RepositoryMetadata metadata) {
        final String basePath = BASE_PATH_SETTING.get(metadata.settings());
        if (Strings.hasLength(basePath)) {
            return BlobPath.EMPTY.add(basePath);
        } else {
            return BlobPath.EMPTY;
        }
    }

    @Override
    protected S3BlobStore createBlobStore() {
        return new S3BlobStore(
            service,
            bucket,
            serverSideEncryption,
            bufferSize,
            cannedACL,
            storageClass,
            metadata,
            bigArrays,
            threadPool,
            s3RepositoriesMetrics
        );
    }

    // only use for testing
    @Override
    protected BlobStore getBlobStore() {
        return super.getBlobStore();
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    @Override
    protected void doClose() {
        final Scheduler.Cancellable cancellable = finalizationFuture.getAndSet(null);
        if (cancellable != null) {
            logger.debug("Repository [{}] closed during cool-down period", metadata.name());
            cancellable.cancel();
        }
        super.doClose();
    }

    @Override
    public String getAnalysisFailureExtraDetail() {
        return Strings.format(
            """
                Elasticsearch observed the storage system underneath this repository behaved incorrectly which indicates it is not \
                suitable for use with Elasticsearch snapshots. Typically this happens when using storage other than AWS S3 which \
                incorrectly claims to be S3-compatible. If so, please report this incompatibility to your storage supplier. Do not report \
                Elasticsearch issues involving storage systems which claim to be S3-compatible unless you can demonstrate that the same \
                issue exists when using a genuine AWS S3 repository. See [%s] for further information about repository analysis, and [%s] \
                for further information about support for S3-compatible repository implementations.""",
            ReferenceDocs.SNAPSHOT_REPOSITORY_ANALYSIS,
            ReferenceDocs.S3_COMPATIBLE_REPOSITORIES
        );
    }

    // only one multipart cleanup process running at once
    private final AtomicBoolean multipartCleanupInProgress = new AtomicBoolean();

    @Override
    public void deleteSnapshots(
        Collection<SnapshotId> snapshotIds,
        long repositoryDataGeneration,
        IndexVersion minimumNodeVersion,
        ActionListener<RepositoryData> repositoryDataUpdateListener,
        Runnable onCompletion
    ) {
        getMultipartUploadCleanupListener(
            isReadOnly() ? 0 : MAX_MULTIPART_UPLOAD_CLEANUP_SIZE.get(getMetadata().settings()),
            new ActionListener<>() {
                @Override
                public void onResponse(ActionListener<Void> multipartUploadCleanupListener) {
                    S3Repository.super.deleteSnapshots(snapshotIds, repositoryDataGeneration, minimumNodeVersion, new ActionListener<>() {
                        @Override
                        public void onResponse(RepositoryData repositoryData) {
                            multipartUploadCleanupListener.onResponse(null);
                            repositoryDataUpdateListener.onResponse(repositoryData);
                        }

                        @Override
                        public void onFailure(Exception e) {
                            multipartUploadCleanupListener.onFailure(e);
                            repositoryDataUpdateListener.onFailure(e);
                        }
                    }, onCompletion);
                }

                @Override
                public void onFailure(Exception e) {
                    logger.warn("failed to get multipart uploads for cleanup during snapshot delete", e);
                    assert false : e; // getMultipartUploadCleanupListener doesn't throw and snapshotExecutor doesn't reject anything
                    repositoryDataUpdateListener.onFailure(e);
                }
            }
        );
    }

    /**
     * Capture the current list of multipart uploads, and (asynchronously) return a listener which, if completed successfully, aborts those
     * uploads. Called at the start of a snapshot delete operation, at which point there should be no ongoing uploads (except in the case of
     * a master failover). We protect against the master failover case by waiting until the delete operation successfully updates the root
     * index-N blob before aborting any uploads.
     */
    void getMultipartUploadCleanupListener(int maxUploads, ActionListener<ActionListener<Void>> listener) {
        if (maxUploads == 0) {
            listener.onResponse(ActionListener.noop());
            return;
        }

        if (multipartCleanupInProgress.compareAndSet(false, true) == false) {
            logger.info("multipart upload cleanup already in progress");
            listener.onResponse(ActionListener.noop());
            return;
        }

        try (var refs = new RefCountingRunnable(() -> multipartCleanupInProgress.set(false))) {
            snapshotExecutor.execute(
                ActionRunnable.supply(
                    ActionListener.releaseAfter(listener, refs.acquire()),
                    () -> blobContainer() instanceof S3BlobContainer s3BlobContainer
                        ? s3BlobContainer.getMultipartUploadCleanupListener(maxUploads, refs)
                        : ActionListener.noop()
                )
            );
        }
    }
}
