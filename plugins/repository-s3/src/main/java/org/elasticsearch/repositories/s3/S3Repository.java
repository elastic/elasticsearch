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
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
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

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
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
    private static final ByteSizeValue DEFAULT_BUFFER_SIZE = new ByteSizeValue(
        Math.max(
            ByteSizeUnit.MB.toBytes(5), // minimum value
            Math.min(
                ByteSizeUnit.MB.toBytes(100),
                JvmInfo.jvmInfo().getMem().getHeapMax().getBytes() / 20)),
        ByteSizeUnit.BYTES);


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
    static final Setting<ByteSizeValue> BUFFER_SIZE_SETTING =
        Setting.byteSizeSetting("buffer_size", DEFAULT_BUFFER_SIZE, MIN_PART_SIZE_USING_MULTIPART, MAX_PART_SIZE_USING_MULTIPART);

    /**
     * Big files can be broken down into chunks during snapshotting if needed. Defaults to 5tb.
     */
    static final Setting<ByteSizeValue> CHUNK_SIZE_SETTING = Setting.byteSizeSetting("chunk_size", MAX_FILE_SIZE_USING_MULTIPART,
            new ByteSizeValue(5, ByteSizeUnit.MB), MAX_FILE_SIZE_USING_MULTIPART);

    /**
     * When set to true metadata files are stored in compressed format. This setting doesn’t affect index
     * files that are already compressed by default. Defaults to false.
     */
    static final Setting<Boolean> COMPRESS_SETTING = Setting.boolSetting("compress", false);

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
     * {@link org.elasticsearch.snapshots.SnapshotsService#SHARD_GEN_IN_REPO_DATA_VERSION} ({@link org.elasticsearch.Version#V_7_6_0}).
     * This delay is necessary so that the eventually consistent nature of AWS S3 does not randomly result in repository corruption when
     * doing repository operations in rapid succession on a repository in the old metadata format.
     * This setting should not be adjusted in production when working with an AWS S3 backed repository. Doing so risks the repository
     * becoming silently corrupted. To get rid of this waiting period, either create a new S3 repository or remove all snapshots older than
     * {@link org.elasticsearch.Version#V_7_6_0} from the repository which will trigger an upgrade of the repository metadata to the new
     * format and disable the cooldown period.
     */
    static final Setting<TimeValue> COOLDOWN_PERIOD = Setting.timeSetting(
        "cooldown_period",
        new TimeValue(3, TimeUnit.MINUTES),
        new TimeValue(0, TimeUnit.MILLISECONDS),
        Setting.Property.Dynamic);

    /**
     * Specifies the path within bucket to repository data. Defaults to root directory.
     */
    static final Setting<String> BASE_PATH_SETTING = Setting.simpleString("base_path");

    private final S3Service service;

    private final String bucket;

    private final ByteSizeValue bufferSize;

    private final ByteSizeValue chunkSize;

    private final BlobPath basePath;

    private final boolean serverSideEncryption;

    private final String storageClass;

    private final String cannedACL;

    private final RepositoryMetadata repositoryMetadata;

    /**
     * Time period to delay repository operations by after finalizing or deleting a snapshot.
     * See {@link #COOLDOWN_PERIOD} for details.
     */
    private final TimeValue coolDown;

    /**
     * Constructs an s3 backed repository
     */
    S3Repository(
            final RepositoryMetadata metadata,
            final NamedXContentRegistry namedXContentRegistry,
            final S3Service service,
            final ClusterService clusterService,
            final BigArrays bigArrays,
            final RecoverySettings recoverySettings) {
        super(metadata,
            COMPRESS_SETTING.get(metadata.settings()),
            namedXContentRegistry,
            clusterService,
            bigArrays,
            recoverySettings,
            buildLocation(metadata));
        this.service = service;

        this.repositoryMetadata = metadata;

        // Parse and validate the user's S3 Storage Class setting
        this.bucket = BUCKET_SETTING.get(metadata.settings());
        if (bucket == null) {
            throw new RepositoryException(metadata.name(), "No bucket defined for s3 repository");
        }

        this.bufferSize = BUFFER_SIZE_SETTING.get(metadata.settings());
        this.chunkSize = CHUNK_SIZE_SETTING.get(metadata.settings());

        // We make sure that chunkSize is bigger or equal than/to bufferSize
        if (this.chunkSize.getBytes() < bufferSize.getBytes()) {
            throw new RepositoryException(metadata.name(), CHUNK_SIZE_SETTING.getKey() + " (" + this.chunkSize +
                ") can't be lower than " + BUFFER_SIZE_SETTING.getKey() + " (" + bufferSize + ").");
        }

        final String basePath = BASE_PATH_SETTING.get(metadata.settings());
        if (Strings.hasLength(basePath)) {
            this.basePath = BlobPath.EMPTY.add(basePath);
        } else {
            this.basePath = BlobPath.EMPTY;
        }

        this.serverSideEncryption = SERVER_SIDE_ENCRYPTION_SETTING.get(metadata.settings());

        this.storageClass = STORAGE_CLASS_SETTING.get(metadata.settings());
        this.cannedACL = CANNED_ACL_SETTING.get(metadata.settings());

        if (S3ClientSettings.checkDeprecatedCredentials(metadata.settings())) {
            // provided repository settings
            deprecationLogger.deprecate(DeprecationCategory.SECURITY, "s3_repository_secret_settings",
                    "Using s3 access/secret key from repository settings. Instead "
                    + "store these in named clients and the elasticsearch keystore for secure settings.");
        }

        coolDown = COOLDOWN_PERIOD.get(metadata.settings());

        logger.debug(
                "using bucket [{}], chunk_size [{}], server_side_encryption [{}], buffer_size [{}], cannedACL [{}], storageClass [{}]",
                bucket,
                chunkSize,
                serverSideEncryption,
                bufferSize,
                cannedACL,
                storageClass);
    }

    private static Map<String, String> buildLocation(RepositoryMetadata metadata) {
        return org.elasticsearch.core.Map.of("base_path", BASE_PATH_SETTING.get(metadata.settings()),
            "bucket", BUCKET_SETTING.get(metadata.settings()));
    }

    /**
     * Holds a reference to delayed repository operation {@link Scheduler.Cancellable} so it can be cancelled should the repository be
     * closed concurrently.
     */
    private final AtomicReference<Scheduler.Cancellable> finalizationFuture = new AtomicReference<>();

    @Override
    public void finalizeSnapshot(FinalizeSnapshotContext finalizeSnapshotContext) {
        if (SnapshotsService.useShardGenerations(finalizeSnapshotContext.repositoryMetaVersion()) == false) {
            finalizeSnapshotContext = new FinalizeSnapshotContext(
                finalizeSnapshotContext.updatedShardGenerations(),
                finalizeSnapshotContext.repositoryStateId(),
                finalizeSnapshotContext.clusterMetadata(),
                finalizeSnapshotContext.snapshotInfo(),
                finalizeSnapshotContext.repositoryMetaVersion(),
                delayedListener(finalizeSnapshotContext)
            );
        }
        super.finalizeSnapshot(finalizeSnapshotContext);
    }

    @Override
    public void deleteSnapshots(Collection<SnapshotId> snapshotIds, long repositoryStateId, Version repositoryMetaVersion,
                                ActionListener<RepositoryData> listener) {
        if (SnapshotsService.useShardGenerations(repositoryMetaVersion) == false) {
            listener = delayedListener(listener);
        }
        super.deleteSnapshots(snapshotIds, repositoryStateId, repositoryMetaVersion, listener);
    }

    /**
     * Wraps given listener such that it is executed with a delay of {@link #coolDown} on the snapshot thread-pool after being invoked.
     * See {@link #COOLDOWN_PERIOD} for details.
     */
    private <T> ActionListener<T> delayedListener(ActionListener<T> listener) {
        final ActionListener<T> wrappedListener = ActionListener.runBefore(listener, () -> {
            final Scheduler.Cancellable cancellable = finalizationFuture.getAndSet(null);
            assert cancellable != null;
        });
        return new ActionListener<T>() {
            @Override
            public void onResponse(T response) {
                logCooldownInfo();
                final Scheduler.Cancellable existing = finalizationFuture.getAndSet(
                    threadPool.schedule(ActionRunnable.wrap(wrappedListener, l -> l.onResponse(response)),
                        coolDown, ThreadPool.Names.SNAPSHOT));
                assert existing == null : "Already have an ongoing finalization " + finalizationFuture;
            }

            @Override
            public void onFailure(Exception e) {
                logCooldownInfo();
                final Scheduler.Cancellable existing = finalizationFuture.getAndSet(
                    threadPool.schedule(ActionRunnable.wrap(wrappedListener, l -> l.onFailure(e)), coolDown, ThreadPool.Names.SNAPSHOT));
                assert existing == null : "Already have an ongoing finalization " + finalizationFuture;
            }
        };
    }

    private void logCooldownInfo() {
        logger.info("Sleeping for [{}] after modifying repository [{}] because it contains snapshots older than version [{}]" +
                " and therefore is using a backwards compatible metadata format that requires this cooldown period to avoid " +
                "repository corruption. To get rid of this message and move to the new repository metadata format, either remove " +
                "all snapshots older than version [{}] from the repository or create a new repository at an empty location.",
            coolDown, metadata.name(), SnapshotsService.SHARD_GEN_IN_REPO_DATA_VERSION,
            SnapshotsService.SHARD_GEN_IN_REPO_DATA_VERSION);
    }

    @Override
    protected S3BlobStore createBlobStore() {
        return new S3BlobStore(service, bucket, serverSideEncryption, bufferSize, cannedACL, storageClass, metadata, bigArrays);
    }

    // only use for testing
    @Override
    protected BlobStore getBlobStore() {
        return super.getBlobStore();
    }

    @Override
    public BlobPath basePath() {
        return basePath;
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
}
