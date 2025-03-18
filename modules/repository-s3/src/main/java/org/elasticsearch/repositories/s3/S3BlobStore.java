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
import software.amazon.awssdk.core.retry.RetryUtils;
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
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;

class S3BlobStore implements BlobStore {

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

    // metrics collector that ignores null responses that we interpret as the request not reaching the S3 endpoint due to a network
    // issue
    class IgnoreNoResponseMetricsPublisher implements MetricPublisher {

        final LongAdder requests = new LongAdder();
        final LongAdder operations = new LongAdder();
        private final Operation operation;
        private final Map<String, Object> attributes;

        private IgnoreNoResponseMetricsPublisher(Operation operation, OperationPurpose purpose) {
            this.operation = operation;
            this.attributes = RepositoriesMetrics.createAttributesMap(repositoryMetadata, purpose, operation.getKey());
        }

        BlobStoreActionStats getEndpointStats() {
            return new BlobStoreActionStats(operations.sum(), requests.sum());
        }

        @Override
        public void publish(MetricCollection metricCollection) {
            // TODO NOMERGE metrics collection
        }

        // TODO NOMERGE metrics collection
        // @Override
        // public final void collectMetrics(Request<?> request, Response<?> response) {
        // assert assertConsistencyBetweenHttpRequestAndOperation(request, operation);
        // final AWSRequestMetrics awsRequestMetrics = request.getAWSRequestMetrics();
        // final TimingInfo timingInfo = awsRequestMetrics.getTimingInfo();
        // final long requestCount = getCountForMetric(timingInfo, AWSRequestMetrics.Field.RequestCount);
        // final long exceptionCount = getCountForMetric(timingInfo, AWSRequestMetrics.Field.Exception);
        // final long throttleCount = getCountForMetric(timingInfo, AWSRequestMetrics.Field.ThrottleException);
        //
        // // For stats reported by API, do not collect stats for null response for BWC.
        // // See https://github.com/elastic/elasticsearch/pull/71406
        // // TODO Is this BWC really necessary?
        // // This behaviour needs to be updated, see https://elasticco.atlassian.net/browse/ES-10223
        // if (response != null) {
        // requests.add(requestCount);
        // }
        //
        // // We collect all metrics regardless whether response is null
        // // There are many situations other than network where a null response can be returned.
        // // In addition, we are interested in the stats when there is a network outage.
        // final int numberOfAwsErrors = Optional.ofNullable(awsRequestMetrics.getProperty(AWSRequestMetrics.Field.AWSErrorCode))
        // .map(List::size)
        // .orElse(0);
        //
        // if (exceptionCount > 0) {
        // final List<Object> statusCodes = Objects.requireNonNullElse(
        // awsRequestMetrics.getProperty(AWSRequestMetrics.Field.StatusCode),
        // List.of()
        // );
        // // REQUESTED_RANGE_NOT_SATISFIED errors are expected errors due to RCO
        // // TODO Add more expected client error codes?
        // final long amountOfRequestRangeNotSatisfiedErrors = statusCodes.stream()
        // .filter(e -> (Integer) e == REQUESTED_RANGE_NOT_SATISFIED.getStatus())
        // .count();
        // if (amountOfRequestRangeNotSatisfiedErrors > 0) {
        // s3RepositoriesMetrics.common()
        // .requestRangeNotSatisfiedExceptionCounter()
        // .incrementBy(amountOfRequestRangeNotSatisfiedErrors, attributes);
        // }
        // }
        //
        // s3RepositoriesMetrics.common().operationCounter().incrementBy(1, attributes);
        // operations.increment();
        // if (numberOfAwsErrors == requestCount) {
        // s3RepositoriesMetrics.common().unsuccessfulOperationCounter().incrementBy(1, attributes);
        // }
        //
        // s3RepositoriesMetrics.common().requestCounter().incrementBy(requestCount, attributes);
        // if (exceptionCount > 0) {
        // s3RepositoriesMetrics.common().exceptionCounter().incrementBy(exceptionCount, attributes);
        // s3RepositoriesMetrics.common().exceptionHistogram().record(exceptionCount, attributes);
        // }
        // if (throttleCount > 0) {
        // s3RepositoriesMetrics.common().throttleCounter().incrementBy(throttleCount, attributes);
        // s3RepositoriesMetrics.common().throttleHistogram().record(throttleCount, attributes);
        // }
        // maybeRecordHttpRequestTime(request);
        // }
        //
        // /**
        // * Used for APM style metrics to measure statics about performance. This is not for billing.
        // */
        // private void maybeRecordHttpRequestTime(Request<?> request) {
        // final List<TimingInfo> requestTimesIncludingRetries = request.getAWSRequestMetrics()
        // .getTimingInfo()
        // .getAllSubMeasurements(AWSRequestMetrics.Field.HttpRequestTime.name());
        // // It can be null if the request did not reach the server for some reason
        // if (requestTimesIncludingRetries == null) {
        // return;
        // }
        //
        // final long totalTimeInNanos = getTotalTimeInNanos(requestTimesIncludingRetries);
        // if (totalTimeInNanos == 0) {
        // logger.warn("Expected HttpRequestTime to be tracked for request [{}] but found no count.", request);
        // } else {
        // s3RepositoriesMetrics.common()
        // .httpRequestTimeInMillisHistogram()
        // .record(TimeUnit.NANOSECONDS.toMillis(totalTimeInNanos), attributes);
        // }
        // }
        //
        // private boolean assertConsistencyBetweenHttpRequestAndOperation(S3Request request, Operation operation) {
        // switch (operation) {
        // case HEAD_OBJECT -> {
        // return request.getHttpMethod().name().equals("HEAD");
        // }
        // case GET_OBJECT, LIST_OBJECTS -> {
        // return request.getHttpMethod().name().equals("GET");
        // }
        // case PUT_OBJECT -> {
        // return request.getHttpMethod().name().equals("PUT");
        // }
        // case PUT_MULTIPART_OBJECT -> {
        // return request.getHttpMethod().name().equals("PUT") || request.getHttpMethod().name().equals("POST");
        // }
        // case DELETE_OBJECTS -> {
        // return request.getHttpMethod().name().equals("POST");
        // }
        // case ABORT_MULTIPART_OBJECT -> {
        // return request.getHttpMethod().name().equals("DELETE");
        // }
        // default -> throw new AssertionError("unknown operation [" + operation + "]");
        // }
        // }

        @Override
        public void close() {}
    }

    // TODO NOMERGE metrics collection
    // private static long getCountForMetric(TimingInfo info, AWSRequestMetrics.Field field) {
    // var count = info.getCounter(field.name());
    // if (count == null) {
    // // This can be null if the thread was interrupted
    // if (field == AWSRequestMetrics.Field.RequestCount && Thread.currentThread().isInterrupted() == false) {
    // final String message = "Expected request count to be tracked but found not count.";
    // assert false : message;
    // logger.warn(message);
    // }
    // return 0L;
    // } else {
    // return count.longValue();
    // }
    // }
    //
    // private static long getTotalTimeInNanos(List<TimingInfo> requestTimesIncludingRetries) {
    // // Here we calculate the timing in Nanoseconds for the sum of the individual subMeasurements with the goal of deriving the TTFB
    // // (time to first byte). We use high precision time here to tell from the case when request time metric is missing (0).
    // // The time is converted to milliseconds for later use with an APM style counter (exposed as a long), rather than using the
    // // default double exposed by getTimeTakenMillisIfKnown().
    // // We don't need sub-millisecond precision. So no need perform the data type castings.
    // long totalTimeInNanos = 0;
    // for (TimingInfo timingInfo : requestTimesIncludingRetries) {
    // var endTimeInNanos = timingInfo.getEndTimeNanoIfKnown();
    // if (endTimeInNanos != null) {
    // totalTimeInNanos += endTimeInNanos - timingInfo.getStartTimeNano();
    // }
    // }
    // return totalTimeInNanos;
    // }

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
                final var response = SocketAccess.doPrivileged(
                    () -> clientReference.client().deleteObjects(bulkDelete(purpose, this, partition))
                );
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

    public static StorageClass initStorageClass(String storageClass) {
        if ((storageClass == null) || storageClass.equals("")) {
            return StorageClass.STANDARD;
        }

        try {
            final StorageClass _storageClass = StorageClass.fromValue(storageClass.toUpperCase(Locale.ENGLISH));
            if (_storageClass.equals(StorageClass.GLACIER)) {
                throw new BlobStoreException("Glacier storage class is not supported");
            }

            return _storageClass;
        } catch (final IllegalArgumentException illegalArgumentException) {
            throw new BlobStoreException("`" + storageClass + "` is not a valid S3 Storage Class.");
        }
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
        ABORT_MULTIPART_OBJECT("AbortMultipartObject");

        private final String key;

        String getKey() {
            return key;
        }

        Operation(String key) {
            this.key = key;
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
    }

    record StatsKey(Operation operation, OperationPurpose purpose) {
        @Override
        public String toString() {
            return purpose.getKey() + "_" + operation.getKey();
        }
    }

    class StatsCollectors {
        final Map<StatsKey, IgnoreNoResponseMetricsPublisher> collectors = new ConcurrentHashMap<>();

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

        IgnoreNoResponseMetricsPublisher buildMetricPublisher(Operation operation, OperationPurpose purpose) {
            return new IgnoreNoResponseMetricsPublisher(operation, purpose);
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
