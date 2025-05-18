/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.awscore.AwsRequest;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.metrics.CoreMetric;
import software.amazon.awssdk.core.retry.RetryUtils;
import software.amazon.awssdk.http.HttpMetric;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Error;
import software.amazon.awssdk.services.s3.model.StorageClass;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.BlobStoreActionStats;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Predicate;
import java.util.stream.Collectors;

class S3BlobStore implements BlobStore {

    public static final String CUSTOM_QUERY_PARAMETER_COPY_SOURCE = "x-amz-copy-source";
    public static final String CUSTOM_QUERY_PARAMETER_PURPOSE = "x-purpose";

    /**
     * Maximum number of deletes in a {@link DeleteObjectsRequest}.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html">S3 Documentation</a>.
     */
    static final int MAX_BULK_DELETES = 1000;

    static final int MAX_DELETE_EXCEPTIONS = 10;

    private static final Logger logger = LogManager.getLogger(S3BlobStore.class);

    private final S3Service service;

    private final BigArrays bigArrays;

    private final String bucket;

    private final ByteSizeValue bufferSize;

    private final ByteSizeValue maxCopySizeBeforeMultipart;

    private final boolean serverSideEncryption;

    private final ObjectCannedACL cannedACL;

    private final StorageClass storageClass;

    private final RepositoryMetadata repositoryMetadata;

    private final ThreadPool threadPool;
    private final Executor snapshotExecutor;
    private final S3RepositoriesMetrics s3RepositoriesMetrics;

    private final StatsCollectors statsCollectors = new StatsCollectors();

    private final int bulkDeletionBatchSize;
    private final BackoffPolicy retryThrottledDeleteBackoffPolicy;

    private final TimeValue getRegisterRetryDelay;

    S3BlobStore(
        S3Service service,
        String bucket,
        boolean serverSideEncryption,
        ByteSizeValue bufferSize,
        ByteSizeValue maxCopySizeBeforeMultipart,
        String cannedACL,
        String storageClass,
        RepositoryMetadata repositoryMetadata,
        BigArrays bigArrays,
        ThreadPool threadPool,
        S3RepositoriesMetrics s3RepositoriesMetrics,
        BackoffPolicy retryThrottledDeleteBackoffPolicy
    ) {
        this.service = service;
        this.bigArrays = bigArrays;
        this.bucket = bucket;
        this.serverSideEncryption = serverSideEncryption;
        this.bufferSize = bufferSize;
        this.maxCopySizeBeforeMultipart = maxCopySizeBeforeMultipart;
        this.cannedACL = initCannedACL(cannedACL);
        this.storageClass = initStorageClass(storageClass);
        this.repositoryMetadata = repositoryMetadata;
        this.threadPool = threadPool;
        this.snapshotExecutor = threadPool.executor(ThreadPool.Names.SNAPSHOT);
        this.s3RepositoriesMetrics = s3RepositoriesMetrics;
        this.bulkDeletionBatchSize = S3Repository.DELETION_BATCH_SIZE_SETTING.get(repositoryMetadata.settings());
        this.retryThrottledDeleteBackoffPolicy = retryThrottledDeleteBackoffPolicy;
        this.getRegisterRetryDelay = S3Repository.GET_REGISTER_RETRY_DELAY.get(repositoryMetadata.settings());
    }

    MetricPublisher getMetricPublisher(Operation operation, OperationPurpose purpose) {
        return statsCollectors.getMetricPublisher(operation, purpose);
    }

    public Executor getSnapshotExecutor() {
        return snapshotExecutor;
    }

    public TimeValue getCompareAndExchangeTimeToLive() {
        return service.compareAndExchangeTimeToLive;
    }

    public TimeValue getCompareAndExchangeAntiContentionDelay() {
        return service.compareAndExchangeAntiContentionDelay;
    }

    /**
     * A {@link MetricPublisher} that processes the metrics related to each API invocation attempt according to Elasticsearch's needs
     */
    class ElasticsearchS3MetricsCollector implements MetricPublisher {

        final LongAdder requests = new LongAdder();
        final LongAdder operations = new LongAdder();
        private final Operation operation;
        private final Map<String, Object> attributes;

        private ElasticsearchS3MetricsCollector(Operation operation, OperationPurpose purpose) {
            this.operation = operation;
            this.attributes = RepositoriesMetrics.createAttributesMap(repositoryMetadata, purpose, operation.getKey());
        }

        BlobStoreActionStats getEndpointStats() {
            return new BlobStoreActionStats(operations.sum(), requests.sum());
        }

        @Override
        public void publish(MetricCollection metricCollection) {
            assert operation.assertConsistentOperationName(metricCollection);

            boolean overallSuccess = false;
            for (final var successMetricValue : metricCollection.metricValues(CoreMetric.API_CALL_SUCCESSFUL)) {
                // The API allows for multiple success flags but in practice there should be only one; check they're all true for safety:
                if (Boolean.TRUE.equals(successMetricValue)) {
                    overallSuccess = true; // but keep checking just in case
                } else {
                    overallSuccess = false;
                    break;
                }
            }

            long totalTimeNanoseconds = 0;
            for (final var durationMetricValue : metricCollection.metricValues(CoreMetric.API_CALL_DURATION)) {
                totalTimeNanoseconds += durationMetricValue.toNanos();
            }

            long requestCount = 0;
            long responseCount = 0;
            long awsErrorCount = 0;
            long throttleCount = 0;
            long http416ResponseCount = 0;
            for (final var apiCallAttemptMetrics : metricCollection.children()) {
                if ("ApiCallAttempt".equals(apiCallAttemptMetrics.name()) == false) {
                    continue;
                }
                requestCount += 1;
                final var errorTypes = apiCallAttemptMetrics.metricValues(CoreMetric.ERROR_TYPE);
                if (errorTypes != null && errorTypes.size() > 0) {
                    awsErrorCount += 1;
                    if (errorTypes.contains("Throttling")) {
                        throttleCount += 1;
                    }
                }

                final var httpResponses = apiCallAttemptMetrics.metricValues(HttpMetric.HTTP_STATUS_CODE);
                if (httpResponses != null && httpResponses.size() > 0) {
                    responseCount += 1;
                    if (httpResponses.contains(RestStatus.REQUESTED_RANGE_NOT_SATISFIED.getStatus())) {
                        http416ResponseCount += 1;
                    }
                }
            }

            // See https://github.com/elastic/elasticsearch/pull/71406 and https://elasticco.atlassian.net/browse/ES-10223
            requests.add(responseCount); // requests that didn't get a HTTP status code assumed not to have reached S3 at all
            s3RepositoriesMetrics.common().operationCounter().incrementBy(1, attributes);
            operations.increment();
            if (overallSuccess == false) {
                s3RepositoriesMetrics.common().unsuccessfulOperationCounter().incrementBy(1, attributes);
            }

            s3RepositoriesMetrics.common().requestCounter().incrementBy(requestCount, attributes);
            if (awsErrorCount > 0) {
                s3RepositoriesMetrics.common().exceptionCounter().incrementBy(awsErrorCount, attributes);
                s3RepositoriesMetrics.common().exceptionHistogram().record(awsErrorCount, attributes);
            }
            if (throttleCount > 0) {
                s3RepositoriesMetrics.common().throttleCounter().incrementBy(throttleCount, attributes);
                s3RepositoriesMetrics.common().throttleHistogram().record(throttleCount, attributes);
            }
            if (http416ResponseCount > 0) {
                s3RepositoriesMetrics.common().requestRangeNotSatisfiedExceptionCounter().incrementBy(http416ResponseCount, attributes);
            }

            if (totalTimeNanoseconds > 0) {
                s3RepositoriesMetrics.common()
                    .httpRequestTimeInMillisHistogram()
                    .record(TimeUnit.NANOSECONDS.toMillis(totalTimeNanoseconds), attributes);
            }
        }

        @Override
        public void close() {}
    }

    @Override
    public String toString() {
        return bucket;
    }

    public AmazonS3Reference clientReference() {
        return service.client(repositoryMetadata);
    }

    final int getMaxRetries() {
        return service.settings(repositoryMetadata).maxRetries;
    }

    public String bucket() {
        return bucket;
    }

    public BigArrays bigArrays() {
        return bigArrays;
    }

    public boolean serverSideEncryption() {
        return serverSideEncryption;
    }

    public long bufferSizeInBytes() {
        return bufferSize.getBytes();
    }

    public long maxCopySizeBeforeMultipart() {
        return maxCopySizeBeforeMultipart.getBytes();
    }

    public RepositoryMetadata getRepositoryMetadata() {
        return repositoryMetadata;
    }

    public S3RepositoriesMetrics getS3RepositoriesMetrics() {
        return s3RepositoriesMetrics;
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new S3BlobContainer(path, this);
    }

    private static class DeletionExceptions {
        Exception exception = null;
        private int count = 0;

        void useOrMaybeSuppress(Exception e) {
            if (count < MAX_DELETE_EXCEPTIONS) {
                exception = ExceptionsHelper.useOrSuppress(exception, e);
                count++;
            }
        }
    }

    void deleteBlobs(OperationPurpose purpose, Iterator<String> blobNames) throws IOException {
        if (blobNames.hasNext() == false) {
            return;
        }

        final List<ObjectIdentifier> partition = new ArrayList<>();
        try {
            // S3 API only allows 1k blobs per delete so we split up the given blobs into requests of max. 1k deletes
            final var deletionExceptions = new DeletionExceptions();
            blobNames.forEachRemaining(key -> {
                partition.add(ObjectIdentifier.builder().key(key).build());
                if (partition.size() == bulkDeletionBatchSize) {
                    deletePartition(purpose, partition, deletionExceptions);
                    partition.clear();
                }
            });
            if (partition.isEmpty() == false) {
                deletePartition(purpose, partition, deletionExceptions);
            }
            if (deletionExceptions.exception != null) {
                throw deletionExceptions.exception;
            }
        } catch (Exception e) {
            throw new IOException("Failed to delete blobs " + partition.stream().limit(10).toList(), e);
        }
    }

    /**
     * Delete one partition of a batch of blobs
     *
     * @param purpose The {@link OperationPurpose} of the deletion
     * @param partition The list of blobs to delete
     * @param deletionExceptions A holder for any exception(s) thrown during the deletion
     */
    private void deletePartition(OperationPurpose purpose, List<ObjectIdentifier> partition, DeletionExceptions deletionExceptions) {
        final Iterator<TimeValue> retries = retryThrottledDeleteBackoffPolicy.iterator();
        int retryCounter = 0;
        while (true) {
            try (AmazonS3Reference clientReference = clientReference()) {
                final var response = clientReference.client().deleteObjects(bulkDelete(purpose, this, partition));
                if (response.hasErrors()) {
                    final var exception = new ElasticsearchException(buildDeletionErrorMessage(response.errors()));
                    logger.warn(exception.getMessage(), exception);
                    deletionExceptions.useOrMaybeSuppress(exception);
                    return;
                }
                s3RepositoriesMetrics.retryDeletesHistogram().record(retryCounter);
                return;
            } catch (SdkException e) {
                if (shouldRetryDelete(purpose) && RetryUtils.isThrottlingException(e)) {
                    // S3 is asking us to slow down. Pause for a bit and retry
                    if (maybeDelayAndRetryDelete(retries)) {
                        retryCounter++;
                    } else {
                        s3RepositoriesMetrics.retryDeletesHistogram().record(retryCounter);
                        deletionExceptions.useOrMaybeSuppress(e);
                        return;
                    }
                } else {
                    // The AWS client threw any unexpected exception and did not execute the request at all so we do not
                    // remove any keys from the outstanding deletes set.
                    deletionExceptions.useOrMaybeSuppress(e);
                    return;
                }
            }
        }
    }

    private String buildDeletionErrorMessage(List<S3Error> errors) {
        final var sb = new StringBuilder("Failed to delete some blobs ");
        for (int i = 0; i < errors.size() && i < MAX_DELETE_EXCEPTIONS; i++) {
            final var err = errors.get(i);
            sb.append("[").append(err.key()).append("][").append(err.code()).append("][").append(err.message()).append("]");
            if (i < errors.size() - 1) {
                sb.append(",");
            }
        }
        if (errors.size() > MAX_DELETE_EXCEPTIONS) {
            sb.append("... (")
                .append(errors.size())
                .append(" in total, ")
                .append(errors.size() - MAX_DELETE_EXCEPTIONS)
                .append(" omitted)");
        }
        return sb.toString();
    }

    /**
     * If there are remaining retries, pause for the configured interval then return true
     *
     * @param retries The retries iterator
     * @return true to try the deletion again, false otherwise
     */
    private boolean maybeDelayAndRetryDelete(Iterator<TimeValue> retries) {
        if (retries.hasNext()) {
            try {
                Thread.sleep(retries.next().millis());
                return true;
            } catch (InterruptedException iex) {
                Thread.currentThread().interrupt();
                // If we're interrupted, record the exception and abort retries
                logger.warn("Aborting tenacious snapshot delete retries due to interrupt");
            }
        } else {
            logger.warn(
                "Exceeded maximum tenacious snapshot delete retries, aborting. Using back-off policy "
                    + retryThrottledDeleteBackoffPolicy
                    + ", see the throttled_delete_retry.* S3 repository properties to configure the back-off parameters"
            );
        }
        return false;
    }

    private boolean shouldRetryDelete(OperationPurpose operationPurpose) {
        return operationPurpose == OperationPurpose.SNAPSHOT_DATA || operationPurpose == OperationPurpose.SNAPSHOT_METADATA;
    }

    private static DeleteObjectsRequest bulkDelete(OperationPurpose purpose, S3BlobStore blobStore, List<ObjectIdentifier> blobs) {
        final var requestBuilder = DeleteObjectsRequest.builder().bucket(blobStore.bucket()).delete(b -> b.quiet(true).objects(blobs));
        configureRequestForMetrics(requestBuilder, blobStore, Operation.DELETE_OBJECTS, purpose);
        return requestBuilder.build();
    }

    @Override
    public void close() throws IOException {
        service.onBlobStoreClose();
    }

    @Override
    public Map<String, BlobStoreActionStats> stats() {
        return statsCollectors.statsMap(service.isStateless);
    }

    // Package private for testing
    StatsCollectors getStatsCollectors() {
        return statsCollectors;
    }

    public ObjectCannedACL getCannedACL() {
        return cannedACL;
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public TimeValue getGetRegisterRetryDelay() {
        return getRegisterRetryDelay;
    }

    public static StorageClass initStorageClass(String storageClassName) {
        if ((storageClassName == null) || storageClassName.equals("")) {
            return StorageClass.STANDARD;
        }

        final StorageClass storageClass;
        try {
            storageClass = StorageClass.fromValue(storageClassName.toUpperCase(Locale.ENGLISH));
        } catch (final Exception e) {
            throw new BlobStoreException("`" + storageClassName + "` is not a valid S3 Storage Class.", e);
        }
        if (storageClass.equals(StorageClass.GLACIER)) {
            throw new BlobStoreException("Glacier storage class is not supported");
        }
        if (storageClass.equals(StorageClass.UNKNOWN_TO_SDK_VERSION)) {
            throw new BlobStoreException("`" + storageClassName + "` is not a known S3 Storage Class.");
        }

        return storageClass;
    }

    /**
     * Constructs canned acl from string
     */
    public static ObjectCannedACL initCannedACL(String cannedACL) {
        if ((cannedACL == null) || cannedACL.equals("")) {
            return ObjectCannedACL.PRIVATE;
        }

        for (final ObjectCannedACL cur : ObjectCannedACL.values()) {
            if (cur.toString().equalsIgnoreCase(cannedACL)) {
                return cur;
            }
        }

        throw new BlobStoreException("cannedACL is not valid: [" + cannedACL + "]");
    }

    ThreadPool getThreadPool() {
        return threadPool;
    }

    enum Operation {
        HEAD_OBJECT("HeadObject"),
        GET_OBJECT("GetObject"),
        LIST_OBJECTS("ListObjects"),
        PUT_OBJECT("PutObject"),
        PUT_MULTIPART_OBJECT("PutMultipartObject"),
        DELETE_OBJECTS("DeleteObjects"),
        ABORT_MULTIPART_OBJECT("AbortMultipartObject"),
        COPY_OBJECT("CopyObject"),
        COPY_MULTIPART_OBJECT("CopyMultipartObject");

        private final String key;

        String getKey() {
            return key;
        }

        Operation(String key) {
            this.key = Objects.requireNonNull(key);
        }

        static Operation parse(String s) {
            for (Operation operation : Operation.values()) {
                if (operation.key.equals(s)) {
                    return operation;
                }
            }
            throw new IllegalArgumentException(
                Strings.format("invalid operation [%s] expected one of [%s]", s, Strings.arrayToCommaDelimitedString(Operation.values()))
            );
        }

        private static final Predicate<String> IS_PUT_MULTIPART_OPERATION = Set.of(
            "CreateMultipartUpload",
            "UploadPart",
            "CompleteMultipartUpload"
        )::contains;

        private static final Predicate<String> IS_COPY_MULTIPART_OPERATION = Set.of(
            "CreateMultipartUpload",
            "UploadPartCopy",
            "CompleteMultipartUpload"
        )::contains;

        private static final Predicate<String> IS_LIST_OPERATION = Set.of("ListObjects", "ListObjectsV2", "ListMultipartUploads")::contains;

        boolean assertConsistentOperationName(MetricCollection metricCollection) {
            final var operationNameMetrics = metricCollection.metricValues(CoreMetric.OPERATION_NAME);
            assert operationNameMetrics.size() == 1 : operationNameMetrics;
            final Predicate<String> expectedOperationPredicate = switch (this) {
                case LIST_OBJECTS -> IS_LIST_OPERATION;
                case PUT_MULTIPART_OBJECT -> IS_PUT_MULTIPART_OPERATION;
                case ABORT_MULTIPART_OBJECT -> "AbortMultipartUpload"::equals;
                case COPY_MULTIPART_OBJECT -> IS_COPY_MULTIPART_OPERATION;
                default -> key::equals;
            };
            assert expectedOperationPredicate.test(operationNameMetrics.get(0)) : this + " vs " + operationNameMetrics;
            return true;
        }
    }

    record StatsKey(Operation operation, OperationPurpose purpose) {
        @Override
        public String toString() {
            return purpose.getKey() + "_" + operation.getKey();
        }
    }

    class StatsCollectors {
        final Map<StatsKey, ElasticsearchS3MetricsCollector> collectors = new ConcurrentHashMap<>();

        MetricPublisher getMetricPublisher(Operation operation, OperationPurpose purpose) {
            return collectors.computeIfAbsent(new StatsKey(operation, purpose), k -> buildMetricPublisher(k.operation(), k.purpose()));
        }

        Map<String, BlobStoreActionStats> statsMap(boolean isStateless) {
            if (isStateless) {
                return collectors.entrySet()
                    .stream()
                    .collect(
                        Collectors.toUnmodifiableMap(entry -> entry.getKey().toString(), entry -> entry.getValue().getEndpointStats())
                    );
            } else {
                final Map<String, BlobStoreActionStats> m = Arrays.stream(Operation.values())
                    .collect(Collectors.toMap(Operation::getKey, e -> BlobStoreActionStats.ZERO));
                collectors.forEach(
                    (sk, v) -> m.compute(sk.operation().getKey(), (k, c) -> Objects.requireNonNull(c).add(v.getEndpointStats()))
                );
                return Map.copyOf(m);
            }
        }

        ElasticsearchS3MetricsCollector buildMetricPublisher(Operation operation, OperationPurpose purpose) {
            return new ElasticsearchS3MetricsCollector(operation, purpose);
        }
    }

    static void configureRequestForMetrics(
        AwsRequest.Builder request,
        S3BlobStore blobStore,
        Operation operation,
        OperationPurpose purpose
    ) {
        request.overrideConfiguration(
            builder -> builder.metricPublishers(List.of(blobStore.getMetricPublisher(operation, purpose)))
                .putRawQueryParameter(CUSTOM_QUERY_PARAMETER_PURPOSE, purpose.getKey())
        );
    }
}
