/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.gcs;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.repositories.blobstore.MeteredBlobStoreRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.util.Map;

import static org.elasticsearch.common.settings.Setting.Property;
import static org.elasticsearch.common.settings.Setting.byteSizeSetting;
import static org.elasticsearch.common.settings.Setting.simpleString;

class GoogleCloudStorageRepository extends MeteredBlobStoreRepository {
    private static final Logger logger = LogManager.getLogger(GoogleCloudStorageRepository.class);

    // package private for testing
    static final ByteSizeValue MIN_CHUNK_SIZE = ByteSizeValue.ONE;

    /**
     * Maximum allowed object size in GCS.
     * @see <a href="https://cloud.google.com/storage/quotas#objects">GCS documentation</a> for details.
     */
    static final ByteSizeValue MAX_CHUNK_SIZE = ByteSizeValue.of(5, ByteSizeUnit.TB);

    static final String TYPE = "gcs";

    static final Setting<String> BUCKET = simpleString("bucket", Property.NodeScope, Property.Dynamic);
    static final Setting<String> BASE_PATH = simpleString("base_path", Property.NodeScope, Property.Dynamic);
    static final Setting<ByteSizeValue> CHUNK_SIZE = byteSizeSetting(
        "chunk_size",
        MAX_CHUNK_SIZE,
        MIN_CHUNK_SIZE,
        MAX_CHUNK_SIZE,
        Property.NodeScope,
        Property.Dynamic
    );
    static final Setting<String> CLIENT_NAME = Setting.simpleString("client", "default");

    /**
     * Storage class applied to uploads with {@link org.elasticsearch.common.blobstore.OperationPurpose#SNAPSHOT_DATA}.
     */
    static final Setting<String> DATA_STORAGE_CLASS = simpleString("data_storage_class", Property.NodeScope, Property.Dynamic);

    /**
     * Storage class applied to uploads with {@link org.elasticsearch.common.blobstore.OperationPurpose#SNAPSHOT_METADATA}.
     */
    static final Setting<String> METADATA_STORAGE_CLASS = simpleString("metadata_storage_class", Property.NodeScope, Property.Dynamic);

    /**
     * We will retry CASes that fail due to throttling. We use an {@link BackoffPolicy#linearBackoff(TimeValue, int, TimeValue)}
     * with the following parameters
     */
    static final Setting<TimeValue> RETRY_THROTTLED_CAS_DELAY_INCREMENT = Setting.timeSetting(
        "throttled_cas_retry.delay_increment",
        TimeValue.timeValueMillis(100),
        TimeValue.ZERO
    );
    static final Setting<Integer> RETRY_THROTTLED_CAS_MAX_NUMBER_OF_RETRIES = Setting.intSetting(
        "throttled_cas_retry.maximum_number_of_retries",
        2,
        0
    );
    static final Setting<TimeValue> RETRY_THROTTLED_CAS_MAXIMUM_DELAY = Setting.timeSetting(
        "throttled_cas_retry.maximum_delay",
        TimeValue.timeValueSeconds(5),
        TimeValue.ZERO
    );

    // When the JVM property that overrides LARGE_BLOB_THRESHOLD_BYTE_SIZE is set (typically in tests to
    // exercise the resumable-upload path with small blobs), lower the minimum to that value so that it can
    // also be set explicitly as a repository setting.
    private static final ByteSizeValue MULTIPART_UPLOAD_SIZE_THRESHOLD_MIN = ByteSizeValue.of(
        Math.min((long) GoogleCloudStorageBlobStore.LARGE_BLOB_THRESHOLD_BYTE_SIZE, ByteSizeUnit.MB.toBytes(5)),
        ByteSizeUnit.BYTES
    );

    static final Setting<ByteSizeValue> MULTIPART_UPLOAD_SIZE_THRESHOLD = byteSizeSetting(
        "multipart_upload_size_threshold",
        ByteSizeValue.of(GoogleCloudStorageBlobStore.LARGE_BLOB_THRESHOLD_BYTE_SIZE, ByteSizeUnit.BYTES),
        MULTIPART_UPLOAD_SIZE_THRESHOLD_MIN,
        ByteSizeValue.of(5, ByteSizeUnit.TB)
    );

    /**
     * Maximum number of blobs per GCS batch delete request. The GCS XML API splits batches at 100
     * items and loops sub-batches serially on the calling thread, so raising this above 100 would
     * silently serialize work inside a single worker and defeat the concurrency setting.
     */
    static final Setting<Integer> DELETE_OBJECTS_MAX_SIZE = Setting.intSetting(
        "delete_objects_max_size",
        GoogleCloudStorageBlobStore.MAX_DELETES_PER_BATCH,
        1,
        GoogleCloudStorageBlobStore.MAX_DELETES_PER_BATCH
    );

    /**
     * Maximum number of concurrent GCS batch delete requests. Each worker runs on the SNAPSHOT
     * thread pool; the calling thread also participates so progress is guaranteed even when the
     * pool is saturated. The default of 5 matches the SNAPSHOT pool size on nodes with a heap
     * below 750 MB ({@link org.elasticsearch.threadpool.ThreadPool#halfAllocatedProcessorsMaxFive})
     * and leaves headroom on larger nodes whose pool size is 10.
     */
    static final Setting<Integer> MAX_CONCURRENT_BATCH_DELETES = Setting.intSetting("max_concurrent_batch_deletes", 5, 1, 100);

    private final GoogleCloudStorageService storageService;
    private final ByteSizeValue chunkSize;
    private final String bucket;
    private final String clientName;
    private final TimeValue retryThrottledCasDelayIncrement;
    private final int retryThrottledCasMaxNumberOfRetries;
    private final TimeValue retryThrottledCasMaxDelay;
    private final GcsRepositoryStatsCollector statsCollector;
    private final String dataStorageClass;
    private final String metadataStorageClass;
    private final long largeBlobThreshold;

    GoogleCloudStorageRepository(
        @Nullable final ProjectId projectId,
        final RepositoryMetadata metadata,
        final NamedXContentRegistry namedXContentRegistry,
        final GoogleCloudStorageService storageService,
        final ClusterService clusterService,
        final BigArrays bigArrays,
        final RecoverySettings recoverySettings,
        final GcsRepositoryStatsCollector statsCollector,
        final SnapshotMetrics snapshotMetrics
    ) {
        super(
            projectId,
            metadata,
            namedXContentRegistry,
            clusterService,
            bigArrays,
            recoverySettings,
            buildBasePath(metadata),
            buildLocation(metadata),
            snapshotMetrics
        );
        this.storageService = storageService;
        this.chunkSize = getSetting(CHUNK_SIZE, metadata);
        this.bucket = getSetting(BUCKET, metadata);
        this.clientName = CLIENT_NAME.get(metadata.settings());
        this.retryThrottledCasDelayIncrement = RETRY_THROTTLED_CAS_DELAY_INCREMENT.get(metadata.settings());
        this.retryThrottledCasMaxNumberOfRetries = RETRY_THROTTLED_CAS_MAX_NUMBER_OF_RETRIES.get(metadata.settings());
        this.retryThrottledCasMaxDelay = RETRY_THROTTLED_CAS_MAXIMUM_DELAY.get(metadata.settings());
        this.statsCollector = statsCollector;
        this.dataStorageClass = DATA_STORAGE_CLASS.get(metadata.settings());
        this.metadataStorageClass = METADATA_STORAGE_CLASS.get(metadata.settings());
        this.largeBlobThreshold = MULTIPART_UPLOAD_SIZE_THRESHOLD.get(metadata.settings()).getBytes();
        validateStorageClassIfSpecified(metadata.name(), DATA_STORAGE_CLASS.getKey(), this.dataStorageClass);
        validateStorageClassIfSpecified(metadata.name(), METADATA_STORAGE_CLASS.getKey(), this.metadataStorageClass);
        logger.debug("using bucket [{}], base_path [{}], chunk_size [{}], compress [{}]", bucket, basePath(), chunkSize, isCompress());
    }

    /**
     * Validates explicit {@link #DATA_STORAGE_CLASS} / {@link #METADATA_STORAGE_CLASS} values during repository construction so
     * misconfiguration surfaces when the repository is registered rather than on first blob store access.
     */
    private static void validateStorageClassIfSpecified(String repositoryName, String settingKey, String value) {
        if (Strings.hasText(value) == false) {
            return;
        }
        try {
            GoogleCloudStorageBlobStore.initStorageClass(value);
        } catch (BlobStoreException e) {
            throw new RepositoryException(repositoryName, settingKey + ": " + e.getMessage(), e);
        }
    }

    private static BlobPath buildBasePath(RepositoryMetadata metadata) {
        String basePath = BASE_PATH.get(metadata.settings());
        if (Strings.hasLength(basePath)) {
            BlobPath path = BlobPath.EMPTY;
            for (String elem : basePath.split("/")) {
                path = path.add(elem);
            }
            return path;
        } else {
            return BlobPath.EMPTY;
        }
    }

    private static Map<String, String> buildLocation(RepositoryMetadata metadata) {
        return Map.of("base_path", BASE_PATH.get(metadata.settings()), "bucket", getSetting(BUCKET, metadata));
    }

    @Override
    protected GoogleCloudStorageBlobStore createBlobStore() {
        return new GoogleCloudStorageBlobStore(
            getProjectId(),
            bucket,
            clientName,
            metadata.name(),
            storageService,
            bigArrays,
            bufferSize,
            largeBlobThreshold,
            BackoffPolicy.linearBackoff(retryThrottledCasDelayIncrement, retryThrottledCasMaxNumberOfRetries, retryThrottledCasMaxDelay),
            statsCollector,
            threadPool().executor(ThreadPool.Names.SNAPSHOT),
            DELETE_OBJECTS_MAX_SIZE.get(metadata.settings()),
            MAX_CONCURRENT_BATCH_DELETES.get(metadata.settings()),
            dataStorageClass,
            metadataStorageClass
        );
    }

    @Override
    protected ByteSizeValue chunkSize() {
        return chunkSize;
    }

    GcsRepositoryStatsCollector statsCollector() {
        return statsCollector;
    }

    /**
     * Get a given setting from the repository settings, throwing a {@link RepositoryException} if the setting does not exist or is empty.
     */
    static <T> T getSetting(Setting<T> setting, RepositoryMetadata metadata) {
        T value = setting.get(metadata.settings());
        if (value == null) {
            throw new RepositoryException(metadata.name(), "Setting [" + setting.getKey() + "] is not defined for repository");
        }
        if (value instanceof String string && Strings.hasText(string) == false) {
            throw new RepositoryException(metadata.name(), "Setting [" + setting.getKey() + "] is empty for repository");
        }
        return value;
    }
}
