/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.AbortMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompletedPart;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.ListMultipartUploadsRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.MultipartUpload;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.SdkPartType;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.blobstore.OptionalBytesReference;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.BlobContainerUtils;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.repositories.RepositoryException;
import org.elasticsearch.repositories.blobstore.ChunkedBlobOutputStream;
import org.elasticsearch.repositories.s3.S3BlobStore.Operation;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.NoSuchFileException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.elasticsearch.common.blobstore.support.BlobContainerUtils.getRegisterUsingConsistentRead;
import static org.elasticsearch.repositories.s3.S3Repository.MAX_FILE_SIZE;
import static org.elasticsearch.repositories.s3.S3Repository.MAX_FILE_SIZE_USING_MULTIPART;
import static org.elasticsearch.repositories.s3.S3Repository.MIN_PART_SIZE_USING_MULTIPART;

class S3BlobContainer extends AbstractBlobContainer {

    private static final Logger logger = LogManager.getLogger(S3BlobContainer.class);

    private final S3BlobStore blobStore;
    private final String keyPath;

    S3BlobContainer(BlobPath path, S3BlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        this.keyPath = path.buildAsString();
    }

    @Override
    public boolean blobExists(OperationPurpose purpose, String blobName) {
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            return doesObjectExist(purpose, clientReference, blobStore.bucket(), buildKey(blobName));
        } catch (final Exception e) {
            throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
        }
    }

    @Override
    public InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException {
        return new S3RetryingInputStream(purpose, blobStore, buildKey(blobName));
    }

    @Override
    public InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException {
        if (position < 0L) {
            throw new IllegalArgumentException("position must be non-negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        if (length == 0) {
            return new ByteArrayInputStream(new byte[0]);
        } else {
            return new S3RetryingInputStream(purpose, blobStore, buildKey(blobName), position, Math.addExact(position, length - 1));
        }
    }

    @Override
    public long readBlobPreferredLength() {
        // This container returns streams that must be fully consumed, so we tell consumers to make bounded requests.
        return new ByteSizeValue(32, ByteSizeUnit.MB).getBytes();
    }

    /**
     * This implementation ignores the failIfAlreadyExists flag as the S3 API has no way to enforce this due to its weak consistency model.
     */
    @Override
    public void writeBlob(OperationPurpose purpose, String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
        throws IOException {
        assert BlobContainer.assertPurposeConsistency(purpose, blobName);
        assert inputStream.markSupported() : "No mark support on inputStream breaks the S3 SDK's ability to retry requests";
        if (blobSize <= getLargeBlobThresholdInBytes()) {
            executeSingleUpload(purpose, blobStore, buildKey(blobName), inputStream, blobSize);
        } else {
            executeMultipartUpload(purpose, blobStore, buildKey(blobName), inputStream, blobSize);
        }
    }

    @Override
    public void writeMetadataBlob(
        OperationPurpose purpose,
        String blobName,
        boolean failIfAlreadyExists,
        boolean atomic,
        CheckedConsumer<OutputStream, IOException> writer
    ) throws IOException {
        assert purpose != OperationPurpose.SNAPSHOT_DATA && BlobContainer.assertPurposeConsistency(purpose, blobName) : purpose;
        final String absoluteBlobKey = buildKey(blobName);
        try (
            ChunkedBlobOutputStream<CompletedPart> out = new ChunkedBlobOutputStream<>(
                blobStore.bigArrays(),
                blobStore.bufferSizeInBytes()
            ) {

                private final SetOnce<String> uploadId = new SetOnce<>();

                @Override
                protected void flushBuffer() throws IOException {
                    flushBuffer(false);
                }

                private void flushBuffer(boolean lastPart) throws IOException {
                    if (buffer.size() == 0) {
                        return;
                    }
                    if (flushedBytes == 0L) {
                        assert lastPart == false : "use single part upload if there's only a single part";
                        try (var clientReference = blobStore.clientReference()) {
                            uploadId.set(
                                clientReference.client()
                                    .createMultipartUpload(createMultipartUpload(purpose, Operation.PUT_MULTIPART_OBJECT, absoluteBlobKey))
                                    .uploadId()
                            );
                        }
                        if (Strings.isEmpty(uploadId.get())) {
                            throw new IOException("Failed to initialize multipart upload " + absoluteBlobKey);
                        }
                    }
                    assert lastPart == false || successful : "must only write last part if successful";
                    final UploadPartRequest uploadRequest = createPartUploadRequest(
                        purpose,
                        uploadId.get(),
                        parts.size() + 1,
                        absoluteBlobKey,
                        buffer.size(),
                        lastPart
                    );
                    final InputStream partContentStream = buffer.bytes().streamInput();
                    final UploadPartResponse uploadResponse;
                    try (var clientReference = blobStore.clientReference()) {
                        uploadResponse = clientReference.client()
                            .uploadPart(uploadRequest, RequestBody.fromInputStream(partContentStream, buffer.size()));
                    }
                    finishPart(CompletedPart.builder().partNumber(parts.size() + 1).eTag(uploadResponse.eTag()).build());
                }

                @Override
                protected void onCompletion() throws IOException {
                    if (flushedBytes == 0L) {
                        writeBlob(purpose, blobName, buffer.bytes(), failIfAlreadyExists);
                    } else {
                        flushBuffer(true);
                        final var completeMultipartUploadRequestBuilder = CompleteMultipartUploadRequest.builder()
                            .bucket(blobStore.bucket())
                            .key(absoluteBlobKey)
                            .uploadId(uploadId.get())
                            .multipartUpload(b -> b.parts(parts));
                        S3BlobStore.configureRequestForMetrics(
                            completeMultipartUploadRequestBuilder,
                            blobStore,
                            Operation.PUT_MULTIPART_OBJECT,
                            purpose
                        );
                        final var completeMultipartUploadRequest = completeMultipartUploadRequestBuilder.build();
                        try (var clientReference = blobStore.clientReference()) {
                            clientReference.client().completeMultipartUpload(completeMultipartUploadRequest);
                        }
                    }
                }

                @Override
                protected void onFailure() {
                    if (Strings.hasText(uploadId.get())) {
                        abortMultiPartUpload(purpose, uploadId.get(), absoluteBlobKey);
                    }
                }
            }
        ) {
            writer.accept(out);
            out.markSuccess();
        }
    }

    // This method is largely copied from AmazonS3Client#doesObjectExist with the ability to instrument the getObjectMetadataRequest
    private boolean doesObjectExist(OperationPurpose purpose, AmazonS3Reference clientReference, String bucketName, String objectName) {
        try {
            final var headObjectRequestBuilder = HeadObjectRequest.builder().bucket(bucketName).key(objectName);
            S3BlobStore.configureRequestForMetrics(headObjectRequestBuilder, blobStore, Operation.HEAD_OBJECT, purpose);
            clientReference.client().headObject(headObjectRequestBuilder.build());
            return true;
        } catch (S3Exception e) {
            if (e.statusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    private UploadPartRequest createPartUploadRequest(
        OperationPurpose purpose,
        String uploadId,
        int number,
        String blobName,
        long size,
        boolean lastPart
    ) {
        final var uploadPartRequestBuilder = UploadPartRequest.builder();
        uploadPartRequestBuilder.bucket(blobStore.bucket());
        uploadPartRequestBuilder.key(blobName);
        uploadPartRequestBuilder.uploadId(uploadId);
        uploadPartRequestBuilder.partNumber(number);
        uploadPartRequestBuilder.contentLength(size);
        uploadPartRequestBuilder.sdkPartType(lastPart ? SdkPartType.LAST : SdkPartType.DEFAULT);
        S3BlobStore.configureRequestForMetrics(uploadPartRequestBuilder, blobStore, Operation.PUT_MULTIPART_OBJECT, purpose);
        return uploadPartRequestBuilder.build();
    }

    private void abortMultiPartUpload(OperationPurpose purpose, String uploadId, String blobName) {
        final var abortMultipartUploadRequestBuilder = AbortMultipartUploadRequest.builder()
            .bucket(blobStore.bucket())
            .key(blobName)
            .uploadId(uploadId);
        S3BlobStore.configureRequestForMetrics(abortMultipartUploadRequestBuilder, blobStore, Operation.ABORT_MULTIPART_OBJECT, purpose);
        final var abortMultipartUploadRequest = abortMultipartUploadRequestBuilder.build();
        try (var clientReference = blobStore.clientReference()) {
            clientReference.client().abortMultipartUpload(abortMultipartUploadRequest);
        }
    }

    private CreateMultipartUploadRequest createMultipartUpload(OperationPurpose purpose, Operation operation, String blobName) {
        final var createMultipartUploadRequestBuilder = CreateMultipartUploadRequest.builder()
            .bucket(blobStore.bucket())
            .key(blobName)
            .storageClass(blobStore.getStorageClass())
            .acl(blobStore.getCannedACL());
        if (blobStore.serverSideEncryption()) {
            createMultipartUploadRequestBuilder.serverSideEncryption(ServerSideEncryption.AES256);
        }
        S3BlobStore.configureRequestForMetrics(createMultipartUploadRequestBuilder, blobStore, operation, purpose);
        return createMultipartUploadRequestBuilder.build();
    }

    // package private for testing
    long getLargeBlobThresholdInBytes() {
        return blobStore.bufferSizeInBytes();
    }

    @Override
    public void writeBlobAtomic(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists)
        throws IOException {
        assert BlobContainer.assertPurposeConsistency(purpose, blobName);
        writeBlob(purpose, blobName, bytes, failIfAlreadyExists);
    }

    @Override
    public DeleteResult delete(OperationPurpose purpose) throws IOException {
        final AtomicLong deletedBlobs = new AtomicLong();
        final AtomicLong deletedBytes = new AtomicLong();
        try (var clientReference = blobStore.clientReference()) {
            ListObjectsV2Response prevListing = null;
            while (true) {
                final var listObjectsRequestBuilder = ListObjectsV2Request.builder().bucket(blobStore.bucket()).prefix(keyPath);
                S3BlobStore.configureRequestForMetrics(listObjectsRequestBuilder, blobStore, Operation.LIST_OBJECTS, purpose);
                if (prevListing != null) {
                    listObjectsRequestBuilder.continuationToken(prevListing.nextContinuationToken());
                }
                final var listObjectsRequest = listObjectsRequestBuilder.build();
                final var listObjectsResponse = clientReference.client().listObjectsV2(listObjectsRequest);
                final Iterator<String> blobNameIterator = Iterators.map(listObjectsResponse.contents().iterator(), s3Object -> {
                    deletedBlobs.incrementAndGet();
                    deletedBytes.addAndGet(s3Object.size());
                    return s3Object.key();
                });
                if (listObjectsResponse.isTruncated()) {
                    blobStore.deleteBlobs(purpose, blobNameIterator);
                    prevListing = listObjectsResponse;
                } else {
                    blobStore.deleteBlobs(purpose, Iterators.concat(blobNameIterator, Iterators.single(keyPath)));
                    break;
                }
            }
        } catch (final SdkException e) {
            throw new IOException("Exception when deleting blob container [" + keyPath + "]", e);
        }
        return new DeleteResult(deletedBlobs.get(), deletedBytes.get());
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames) throws IOException {
        blobStore.deleteBlobs(purpose, Iterators.map(blobNames, this::buildKey));
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, @Nullable String blobNamePrefix) throws IOException {
        try {
            final var results = new HashMap<String, BlobMetadata>();
            final var iterator = executeListing(purpose, blobNamePrefix == null ? keyPath : buildKey(blobNamePrefix));
            while (iterator.hasNext()) {
                final var currentPage = iterator.next();
                for (final var s3Object : currentPage.contents()) {
                    final var blobName = s3Object.key().substring(keyPath.length());
                    if (results.put(blobName, new BlobMetadata(blobName, s3Object.size())) != null) {
                        throw new IllegalStateException(
                            "listing objects by prefix [" + blobNamePrefix + "] yielded multiple blobs with key [" + s3Object.key() + "]"
                        );
                    }
                }
            }
            return results;
        } catch (final SdkException e) {
            throw new IOException("Exception when listing blobs by prefix [" + blobNamePrefix + "]", e);
        }
    }

    @Override
    public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException {
        return listBlobsByPrefix(purpose, null);
    }

    @Override
    public Map<String, BlobContainer> children(OperationPurpose purpose) throws IOException {
        try {
            final var results = new HashMap<String, BlobContainer>();
            final var relativePrefixStart = keyPath.length();
            final var iterator = executeListing(purpose, keyPath);
            while (iterator.hasNext()) {
                final var currentPage = iterator.next();
                for (final var commonPrefix : currentPage.commonPrefixes()) {
                    final var absolutePrefix = commonPrefix.prefix();
                    if (absolutePrefix.length() <= relativePrefixStart + 1) {
                        continue;
                    }
                    final var relativePrefix = absolutePrefix.substring(relativePrefixStart, absolutePrefix.length() - 1);
                    assert relativePrefix.isEmpty() == false;
                    assert currentPage.contents().stream().noneMatch(s3Object -> s3Object.key().startsWith(absolutePrefix))
                        : "Response contained children for listed common prefix " + absolutePrefix;
                    if (results.put(relativePrefix, blobStore.blobContainer(path().add(relativePrefix))) != null) {
                        throw new IllegalStateException(
                            "listing child containers of [" + keyPath + "] yielded multiple children with key [" + relativePrefix + "]"
                        );
                    }
                }
            }
            return results;
        } catch (final SdkException e) {
            throw new IOException("Exception when listing children of [" + path().buildAsString() + ']', e);
        }
    }

    private Iterator<ListObjectsV2Response> executeListing(OperationPurpose purpose, String pathPrefix) {
        return new Iterator<>() {
            @Nullable // if after last page
            private ListObjectsV2Response nextResponse = listNextObjects(purpose, pathPrefix, null);

            @Override
            public boolean hasNext() {
                return nextResponse != null;
            }

            @Override
            public ListObjectsV2Response next() {
                final var currentResponse = nextResponse;
                nextResponse = currentResponse.nextContinuationToken() == null
                    ? null
                    : listNextObjects(purpose, pathPrefix, currentResponse);
                return currentResponse;
            }
        };
    }

    private ListObjectsV2Response listNextObjects(
        OperationPurpose operationPurpose,
        String pathPrefix,
        @Nullable /* if requesting the first page of objects */
        ListObjectsV2Response previousResponse
    ) {
        try (var clientReference = blobStore.clientReference()) {
            final var listObjectsRequestBuilder = ListObjectsV2Request.builder()
                .bucket(blobStore.bucket())
                .prefix(pathPrefix)
                .delimiter("/");
            if (previousResponse != null) {
                if (previousResponse.nextContinuationToken() == null) {
                    throw new IllegalStateException("cannot request next page of object listing without a continuation token");
                }
                listObjectsRequestBuilder.continuationToken(previousResponse.nextContinuationToken());
            }
            S3BlobStore.configureRequestForMetrics(listObjectsRequestBuilder, blobStore, Operation.LIST_OBJECTS, operationPurpose);
            final var listObjectsRequest = listObjectsRequestBuilder.build();
            return clientReference.client().listObjectsV2(listObjectsRequest);
        }
    }

    // exposed for tests
    String buildKey(String blobName) {
        return keyPath + blobName;
    }

    /**
     * Uploads a blob using a single upload request
     */
    void executeSingleUpload(
        OperationPurpose purpose,
        final S3BlobStore s3BlobStore,
        final String blobName,
        final InputStream input,
        final long blobSize
    ) throws IOException {
        try (var clientReference = s3BlobStore.clientReference()) {
            // Extra safety checks
            if (blobSize > MAX_FILE_SIZE.getBytes()) {
                throw new IllegalArgumentException("Upload request size [" + blobSize + "] can't be larger than " + MAX_FILE_SIZE);
            }
            if (blobSize > s3BlobStore.bufferSizeInBytes()) {
                throw new IllegalArgumentException("Upload request size [" + blobSize + "] can't be larger than buffer size");
            }

            final var putRequestBuilder = PutObjectRequest.builder()
                .bucket(s3BlobStore.bucket())
                .key(blobName)
                .contentLength(blobSize)
                .storageClass(s3BlobStore.getStorageClass())
                .acl(s3BlobStore.getCannedACL());
            if (s3BlobStore.serverSideEncryption()) {
                putRequestBuilder.serverSideEncryption(ServerSideEncryption.AES256);
            }
            S3BlobStore.configureRequestForMetrics(putRequestBuilder, blobStore, Operation.PUT_OBJECT, purpose);

            final var putRequest = putRequestBuilder.build();
            clientReference.client().putObject(putRequest, RequestBody.fromInputStream(input, blobSize));
        } catch (final SdkException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using a single upload", e);
        }
    }

    private interface PartOperation {
        CompletedPart doPart(String uploadId, int partNum, long partSize, boolean lastPart);
    }

    // for copy, blobName and s3BlobStore are the destination
    private void executeMultipart(
        final OperationPurpose purpose,
        final Operation operation,
        final S3BlobStore s3BlobStore,
        final String blobName,
        final long partSize,
        final long blobSize,
        final PartOperation partOperation
    ) throws IOException {

        ensureMultiPartUploadSize(blobSize);
        final Tuple<Long, Long> multiparts = numberOfMultiparts(blobSize, partSize);

        if (multiparts.v1() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Too many multipart upload requests, maybe try a larger part size?");
        }

        final int nbParts = multiparts.v1().intValue();
        final long lastPartSize = multiparts.v2();
        assert blobSize == (((nbParts - 1) * partSize) + lastPartSize) : "blobSize does not match multipart sizes";

        final List<Runnable> cleanupOnFailureActions = new ArrayList<>(1);
        final String bucketName = s3BlobStore.bucket();
        try {
            final String uploadId;
            try (AmazonS3Reference clientReference = s3BlobStore.clientReference()) {
                uploadId = clientReference.client().createMultipartUpload(createMultipartUpload(purpose, operation, blobName)).uploadId();
                cleanupOnFailureActions.add(() -> abortMultiPartUpload(purpose, uploadId, blobName));
            }
            if (Strings.isEmpty(uploadId)) {
                throw new IOException("Failed to initialize multipart operation for " + blobName);
            }

            final List<CompletedPart> parts = new ArrayList<>();

            long bytesCount = 0;
            for (int i = 1; i <= nbParts; i++) {
                final boolean lastPart = i == nbParts;
                final var curPartSize = lastPart ? lastPartSize : partSize;
                final var partEtag = partOperation.doPart(uploadId, i, curPartSize, lastPart);
                bytesCount += curPartSize;
                parts.add(partEtag);
            }

            if (bytesCount != blobSize) {
                throw new IOException(
                    "Failed to execute multipart operation for ["
                        + blobName
                        + "], expected "
                        + blobSize
                        + "bytes sent but got "
                        + bytesCount
                );
            }

            final var completeMultipartUploadRequestBuilder = CompleteMultipartUploadRequest.builder()
                .bucket(bucketName)
                .key(blobName)
                .uploadId(uploadId)
                .multipartUpload(b -> b.parts(parts));
            S3BlobStore.configureRequestForMetrics(completeMultipartUploadRequestBuilder, blobStore, operation, purpose);
            final var completeMultipartUploadRequest = completeMultipartUploadRequestBuilder.build();
            try (var clientReference = s3BlobStore.clientReference()) {
                clientReference.client().completeMultipartUpload(completeMultipartUploadRequest);
            }
            cleanupOnFailureActions.clear();
        } catch (final SdkException e) {
            if (e instanceof SdkServiceException sse && sse.statusCode() == RestStatus.NOT_FOUND.getStatus()) {
                throw new NoSuchFileException(blobName, null, e.getMessage());
            }
            throw new IOException("Unable to upload or copy object [" + blobName + "] using multipart upload", e);
        } finally {
            cleanupOnFailureActions.forEach(Runnable::run);
        }
    }

    /**
     * Uploads a blob using multipart upload requests.
     */
    void executeMultipartUpload(
        OperationPurpose purpose,
        final S3BlobStore s3BlobStore,
        final String blobName,
        final InputStream input,
        final long blobSize
    ) throws IOException {
        executeMultipart(
            purpose,
            Operation.PUT_MULTIPART_OBJECT,
            s3BlobStore,
            blobName,
            s3BlobStore.bufferSizeInBytes(),
            blobSize,
            (uploadId, partNum, partSize, lastPart) -> {
                final UploadPartRequest uploadRequest = createPartUploadRequest(purpose, uploadId, partNum, blobName, partSize, lastPart);

                try (var clientReference = s3BlobStore.clientReference()) {
                    final UploadPartResponse uploadResponse = clientReference.client()
                        .uploadPart(uploadRequest, RequestBody.fromInputStream(input, partSize));
                    return CompletedPart.builder().partNumber(partNum).eTag(uploadResponse.eTag()).build();
                }
            }
        );
    }

    // non-static, package private for testing
    void ensureMultiPartUploadSize(final long blobSize) {
        if (blobSize > MAX_FILE_SIZE_USING_MULTIPART.getBytes()) {
            throw new IllegalArgumentException(
                "Multipart upload request size [" + blobSize + "] can't be larger than " + MAX_FILE_SIZE_USING_MULTIPART
            );
        }
        if (blobSize < MIN_PART_SIZE_USING_MULTIPART.getBytes()) {
            throw new IllegalArgumentException(
                "Multipart upload request size [" + blobSize + "] can't be smaller than " + MIN_PART_SIZE_USING_MULTIPART
            );
        }
    }

    /**
     * Returns the number parts of size of {@code partSize} needed to reach {@code totalSize},
     * along with the size of the last (or unique) part.
     *
     * @param totalSize the total size
     * @param partSize  the part size
     * @return a {@link Tuple} containing the number of parts to fill {@code totalSize} and
     * the size of the last part
     */
    static Tuple<Long, Long> numberOfMultiparts(final long totalSize, final long partSize) {
        if (partSize <= 0) {
            throw new IllegalArgumentException("Part size must be greater than zero");
        }

        if ((totalSize == 0L) || (totalSize <= partSize)) {
            return Tuple.tuple(1L, totalSize);
        }

        final long parts = totalSize / partSize;
        final long remaining = totalSize % partSize;

        if (remaining == 0) {
            return Tuple.tuple(parts, partSize);
        } else {
            return Tuple.tuple(parts + 1, remaining);
        }
    }

    private class CompareAndExchangeOperation {

        private final OperationPurpose purpose;
        private final S3Client client;
        private final String bucket;
        private final String rawKey;
        private final String blobKey;
        private final ThreadPool threadPool;

        CompareAndExchangeOperation(OperationPurpose purpose, S3Client client, String bucket, String key, ThreadPool threadPool) {
            this.purpose = purpose;
            this.client = client;
            this.bucket = bucket;
            this.rawKey = key;
            this.blobKey = buildKey(key);
            this.threadPool = threadPool;
        }

        void run(BytesReference expected, BytesReference updated, ActionListener<OptionalBytesReference> listener) throws Exception {
            BlobContainerUtils.ensureValidRegisterContent(updated);

            if (hasPreexistingUploads()) {
                // This is a small optimization to improve the liveness properties of this algorithm.
                //
                // We can safely proceed even if there are other uploads in progress, but that would add to the potential for collisions and
                // delays. Thus in this case we prefer avoid disturbing the ongoing attempts and just fail up front.
                listener.onResponse(OptionalBytesReference.MISSING);
                return;
            }

            // Step 1: Start our upload and upload the new contents as its unique part.

            final var uploadId = createMultipartUpload();
            logger.trace("[{}] initiated upload [{}]", blobKey, uploadId);
            final var partETag = uploadPartAndGetEtag(updated, uploadId);
            logger.trace("[{}] uploaded update to [{}]", blobKey, uploadId);

            // Step 2: List all uploads that are racing to complete, and compute our position in the list. This definitely includes all the
            // uploads that started before us and are still in-progress, and may include some later-started in-progress ones too.

            final var currentUploads = listMultipartUploads();
            logUploads("uploads before current", currentUploads);
            final var uploadIndex = getUploadIndex(uploadId, currentUploads);
            logger.trace("[{}] upload [{}] has index [{}]", blobKey, uploadId, uploadIndex);

            if (uploadIndex < 0) {
                // already aborted by someone else
                listener.onResponse(OptionalBytesReference.MISSING);
                return;
            }

            SubscribableListener

                // Step 3: Ensure all other uploads in currentUploads are complete (either successfully, aborted by us or by another upload)

                .<Void>newForked(l -> ensureOtherUploadsComplete(uploadId, uploadIndex, currentUploads, l))

                // Step 4: Read the current register value.

                .<OptionalBytesReference>andThen(l -> getRegister(purpose, rawKey, l))

                // Step 5: Perform the compare-and-swap by completing our upload iff the witnessed value matches the expected value.

                .andThenApply(currentValue -> {
                    if (currentValue.isPresent() && currentValue.bytesReference().equals(expected)) {
                        logger.trace("[{}] completing upload [{}]", blobKey, uploadId);
                        completeMultipartUpload(uploadId, partETag);
                    } else {
                        // Best-effort attempt to clean up after ourselves.
                        logger.trace("[{}] aborting upload [{}]", blobKey, uploadId);
                        safeAbortMultipartUpload(uploadId);
                    }
                    return currentValue;
                })

                // Step 6: Complete the listener.

                .addListener(listener.delegateResponse((l, e) -> {
                    // Best-effort attempt to clean up after ourselves.
                    logger.trace(() -> Strings.format("[%s] aborting upload [%s] on exception", blobKey, uploadId), e);
                    safeAbortMultipartUpload(uploadId);
                    l.onFailure(e);
                }));

            // No compare-and-exchange operations that started before ours can write to the register (in its step 5) after we have read the
            // current value of the register (in our step 4) because we have ensured all earlier operations have completed (in our step 3).
            // Conversely, if some other compare-and-exchange operation started after us then it will not read the register (in its step 4)
            // until it has ensured we will not do a future write to the register (in our step 5) by cancelling all the racing uploads that
            // it observed (in its step 3). Thus steps 4 and 5 can only complete successfully with no intervening writes to the register.
        }

        /**
         * @return {@code true} if there are already ongoing uploads, so we should not proceed with the operation
         */
        private boolean hasPreexistingUploads() {
            final var timeToLiveMillis = blobStore.getCompareAndExchangeTimeToLive().millis();
            if (timeToLiveMillis < 0) {
                return false; // proceed always
            }

            final var uploads = listMultipartUploads();
            logUploads("preexisting uploads", uploads);

            if (uploads.isEmpty()) {
                logger.trace("[{}] no preexisting uploads", blobKey);
                return false;
            }

            final var expiryDate = Instant.ofEpochMilli(blobStore.getThreadPool().absoluteTimeInMillis() - timeToLiveMillis);
            if (uploads.stream().anyMatch(upload -> upload.initiated().compareTo(expiryDate) > 0)) {
                logger.trace("[{}] fresh preexisting uploads vs {}", blobKey, expiryDate);
                return true;
            }

            // there are uploads, but they are all older than the TTL, so clean them up before carrying on (should be rare)
            for (final var upload : uploads) {
                logger.warn("cleaning up stale compare-and-swap upload [{}] initiated at [{}]", upload.uploadId(), upload.initiated());
                safeAbortMultipartUpload(upload.uploadId());
            }

            logger.trace("[{}] stale preexisting uploads vs {}", blobKey, expiryDate);
            return false;
        }

        private void logUploads(String description, List<MultipartUpload> uploads) {
            if (logger.isTraceEnabled()) {
                logger.trace(
                    "[{}] {}: [{}]",
                    blobKey,
                    description,
                    uploads.stream()
                        .map(multipartUpload -> multipartUpload.uploadId() + ": " + multipartUpload.initiated())
                        .collect(Collectors.joining(","))
                );
            }
        }

        private List<MultipartUpload> listMultipartUploads() {
            final var listRequestBuilder = ListMultipartUploadsRequest.builder().bucket(bucket).prefix(blobKey);
            S3BlobStore.configureRequestForMetrics(listRequestBuilder, blobStore, Operation.LIST_OBJECTS, purpose);
            final var listRequest = listRequestBuilder.build();
            try {
                return client.listMultipartUploads(listRequest).uploads();
            } catch (SdkServiceException e) {
                if (e.statusCode() == 404) {
                    return List.of();
                }
                throw e;
            }
        }

        private String createMultipartUpload() {
            final var createMultipartUploadRequestBuilder = CreateMultipartUploadRequest.builder().bucket(bucket).key(blobKey);
            S3BlobStore.configureRequestForMetrics(createMultipartUploadRequestBuilder, blobStore, Operation.PUT_MULTIPART_OBJECT, purpose);
            final var createMultipartUploadRequest = createMultipartUploadRequestBuilder.build();
            return client.createMultipartUpload(createMultipartUploadRequest).uploadId();
        }

        private String uploadPartAndGetEtag(BytesReference updated, String uploadId) throws IOException {
            final var uploadPartRequestBuilder = UploadPartRequest.builder();
            uploadPartRequestBuilder.bucket(bucket);
            uploadPartRequestBuilder.key(blobKey);
            uploadPartRequestBuilder.uploadId(uploadId);
            uploadPartRequestBuilder.partNumber(1);
            uploadPartRequestBuilder.sdkPartType(SdkPartType.LAST);
            S3BlobStore.configureRequestForMetrics(uploadPartRequestBuilder, blobStore, Operation.PUT_MULTIPART_OBJECT, purpose);
            return client.uploadPart(uploadPartRequestBuilder.build(), RequestBody.fromInputStream(updated.streamInput(), updated.length()))
                .eTag();
        }

        private int getUploadIndex(String targetUploadId, List<MultipartUpload> multipartUploads) {
            var uploadIndex = 0;
            var found = false;
            for (final var multipartUpload : multipartUploads) {
                final var observedUploadId = multipartUpload.uploadId();
                if (observedUploadId.equals(targetUploadId)) {
                    final var currentTimeMillis = blobStore.getThreadPool().absoluteTimeInMillis();
                    final var ageMillis = currentTimeMillis - multipartUpload.initiated().toEpochMilli();
                    final var expectedAgeRangeMillis = blobStore.getCompareAndExchangeTimeToLive().millis();
                    if (0 <= expectedAgeRangeMillis && (ageMillis < -expectedAgeRangeMillis || ageMillis > expectedAgeRangeMillis)) {
                        logger.warn(
                            """
                                compare-and-exchange of blob [{}:{}] was initiated at [{}={}] \
                                which deviates from local node epoch time [{}] by more than the warn threshold of [{}ms]""",
                            bucket,
                            blobKey,
                            multipartUpload.initiated(),
                            multipartUpload.initiated().toEpochMilli(),
                            currentTimeMillis,
                            expectedAgeRangeMillis
                        );
                    }
                    found = true;
                } else if (observedUploadId.compareTo(targetUploadId) < 0) {
                    uploadIndex += 1;
                }
            }

            return found ? uploadIndex : -1;
        }

        private void ensureOtherUploadsComplete(
            String uploadId,
            int uploadIndex,
            List<MultipartUpload> currentUploads,
            ActionListener<Void> listener
        ) {
            // This is a small optimization to improve the liveness properties of this algorithm.
            //
            // When there are updates racing to complete, we try and let them complete in order of their upload IDs. The one with the first
            // upload ID immediately tries to cancel the competing updates in order to make progress, but the ones with greater upload IDs
            // wait based on their position in the list before proceeding.
            //
            // Note that this does not guarantee that any of the uploads actually succeeds. Another operation could start and see a
            // different collection of racing uploads and cancel all of them while they're sleeping. In theory this whole thing is provably
            // impossible anyway [1] but in practice it'll eventually work with sufficient retries.
            //
            // [1] Michael J. Fischer, Nancy A. Lynch, and Michael S. Paterson. 1985. Impossibility of distributed consensus with one faulty
            // process. J. ACM 32, 2 (April 1985), 374–382.
            //
            // TODO should we sort these by initiation time (and then upload ID as a tiebreaker)?
            // TODO should we listMultipartUploads() while waiting, so we can fail quicker if we are concurrently cancelled?
            if (uploadIndex > 0) {
                threadPool.scheduleUnlessShuttingDown(
                    TimeValue.timeValueMillis(
                        uploadIndex * blobStore.getCompareAndExchangeAntiContentionDelay().millis() + Randomness.get().nextInt(50)
                    ),
                    blobStore.getSnapshotExecutor(),
                    ActionRunnable.wrap(listener, l -> cancelOtherUploads(uploadId, currentUploads, l))
                );
            } else {
                cancelOtherUploads(uploadId, currentUploads, listener);
            }
        }

        private void cancelOtherUploads(String uploadId, List<MultipartUpload> currentUploads, ActionListener<Void> listener) {
            logger.trace("[{}] upload [{}] cancelling other uploads", blobKey, uploadId);
            final var executor = blobStore.getSnapshotExecutor();
            try (var listeners = new RefCountingListener(listener)) {
                for (final var currentUpload : currentUploads) {
                    final var currentUploadId = currentUpload.uploadId();
                    if (uploadId.equals(currentUploadId) == false) {
                        executor.execute(ActionRunnable.run(listeners.acquire(), () -> abortMultipartUploadIfExists(currentUploadId)));
                    }
                }
            }
        }

        private void safeAbortMultipartUpload(String uploadId) {
            try {
                abortMultipartUploadIfExists(uploadId);
            } catch (Exception e) {
                // cleanup is a best-effort thing, we can't do anything better than log and fall through here
                logger.error("unexpected error cleaning up upload [" + uploadId + "] of [" + blobKey + "]", e);
            }
        }

        private void abortMultipartUploadIfExists(String uploadId) {
            try {
                final var abortMultipartUploadRequestBuilder = AbortMultipartUploadRequest.builder()
                    .bucket(bucket)
                    .key(blobKey)
                    .uploadId(uploadId);
                S3BlobStore.configureRequestForMetrics(
                    abortMultipartUploadRequestBuilder,
                    blobStore,
                    Operation.ABORT_MULTIPART_OBJECT,
                    purpose
                );
                final var abortMultipartUploadRequest = abortMultipartUploadRequestBuilder.build();
                client.abortMultipartUpload(abortMultipartUploadRequest);
            } catch (SdkServiceException e) {
                if (e.statusCode() != 404) {
                    throw e;
                }
                // else already aborted
            }
        }

        private void completeMultipartUpload(String uploadId, String partETag) {
            final var completeMultipartUploadRequestBuilder = CompleteMultipartUploadRequest.builder()
                .bucket(bucket)
                .key(blobKey)
                .uploadId(uploadId)
                .multipartUpload(b -> b.parts(CompletedPart.builder().partNumber(1).eTag(partETag).build()));
            S3BlobStore.configureRequestForMetrics(
                completeMultipartUploadRequestBuilder,
                blobStore,
                Operation.PUT_MULTIPART_OBJECT,
                purpose
            );
            final var completeMultipartUploadRequest = completeMultipartUploadRequestBuilder.build();
            client.completeMultipartUpload(completeMultipartUploadRequest);
        }
    }

    @Override
    public void compareAndExchangeRegister(
        OperationPurpose purpose,
        String key,
        BytesReference expected,
        BytesReference updated,
        ActionListener<OptionalBytesReference> listener
    ) {
        final var clientReference = blobStore.clientReference();
        ActionListener.run(ActionListener.releaseAfter(listener.delegateResponse((delegate, e) -> {
            logger.trace(() -> Strings.format("[%s]: compareAndExchangeRegister failed", key), e);
            if (e instanceof AwsServiceException awsServiceException
                && (awsServiceException.statusCode() == 404
                    || awsServiceException.statusCode() == 200
                        && "NoSuchUpload".equals(awsServiceException.awsErrorDetails().errorCode()))) {
                // An uncaught 404 means that our multipart upload was aborted by a concurrent operation before we could complete it.
                // Also (rarely) S3 can start processing the request during a concurrent abort and this can result in a 200 OK with an
                // <Error><Code>NoSuchUpload</Code>... in the response. Either way, this means that our write encountered contention:
                delegate.onResponse(OptionalBytesReference.MISSING);
            } else {
                delegate.onFailure(e);
            }
        }), clientReference),
            l -> new CompareAndExchangeOperation(purpose, clientReference.client(), blobStore.bucket(), key, blobStore.getThreadPool()).run(
                expected,
                updated,
                l
            )
        );
    }

    @Override
    public void getRegister(OperationPurpose purpose, String key, ActionListener<OptionalBytesReference> listener) {
        ActionListener.completeWith(listener, () -> {
            Exception finalException = null;
            final var getObjectRequestBuilder = GetObjectRequest.builder().bucket(blobStore.bucket()).key(buildKey(key));
            S3BlobStore.configureRequestForMetrics(getObjectRequestBuilder, blobStore, Operation.GET_OBJECT, purpose);
            final var getObjectRequest = getObjectRequestBuilder.build();
            try (var clientReference = blobStore.clientReference(); var s3Object = clientReference.client().getObject(getObjectRequest);) {
                return OptionalBytesReference.of(getRegisterUsingConsistentRead(s3Object, keyPath, key));
            } catch (Exception e) {
                logger.trace(() -> Strings.format("[%s]: getRegister failed", key), e);
                if (e instanceof AwsServiceException awsServiceException && awsServiceException.statusCode() == 404) {
                    return OptionalBytesReference.EMPTY;
                } else {
                    throw e;
                }
            }
        });
    }

    ActionListener<Void> getMultipartUploadCleanupListener(int maxUploads, RefCountingRunnable refs) {
        try (var clientReference = blobStore.clientReference()) {
            final var bucket = blobStore.bucket();
            final var listMultipartUploadsRequest = ListMultipartUploadsRequest.builder()
                .bucket(bucket)
                .prefix(keyPath)
                .maxUploads(maxUploads)
                // TODO adjust to use S3BlobStore.configureRequestForMetrics, adding metrics collection
                .overrideConfiguration(b -> blobStore.addPurposeQueryParameter(OperationPurpose.SNAPSHOT_DATA, b))
                .build();
            final var multipartUploadListing = clientReference.client().listMultipartUploads(listMultipartUploadsRequest);
            final var multipartUploads = multipartUploadListing.uploads();
            if (multipartUploads.isEmpty()) {
                logger.debug("found no multipart uploads to clean up");
                return ActionListener.noop();
            } else {
                // the uploads are only _possibly_ dangling because it's also possible we're no longer then master and the new master has
                // started some more shard snapshots
                if (multipartUploadListing.isTruncated()) {
                    logger.info("""
                        found at least [{}] possibly-dangling multipart uploads; will clean up the first [{}] after finalizing \
                        the current snapshot deletions, and will check for further possibly-dangling multipart uploads in future \
                        snapshot deletions""", multipartUploads.size(), multipartUploads.size());
                } else {
                    logger.info("""
                        found [{}] possibly-dangling multipart uploads; \
                        will clean them up after finalizing the current snapshot deletions""", multipartUploads.size());
                }
                return newMultipartUploadCleanupListener(
                    refs,
                    Iterators.map(
                        multipartUploads.iterator(),
                        u -> AbortMultipartUploadRequest.builder()
                            .bucket(bucket)
                            .key(u.key())
                            .uploadId(u.uploadId())
                            // TODO adjust to use S3BlobStore.configureRequestForMetrics, adding metrics collection
                            .overrideConfiguration(b -> blobStore.addPurposeQueryParameter(OperationPurpose.SNAPSHOT_DATA, b))
                            .build()
                    )
                );
            }
        } catch (Exception e) {
            // Cleanup is a best-effort thing, we can't do anything better than log and carry on here.
            logger.warn("failure while checking for possibly-dangling multipart uploads", e);
            return ActionListener.noop();
        }
    }

    private ActionListener<Void> newMultipartUploadCleanupListener(
        RefCountingRunnable refs,
        Iterator<AbortMultipartUploadRequest> abortMultipartUploadRequestIterator
    ) {
        return new ThreadedActionListener<>(blobStore.getSnapshotExecutor(), ActionListener.releaseAfter(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                try (var clientReference = blobStore.clientReference()) {
                    while (abortMultipartUploadRequestIterator.hasNext()) {
                        final var abortMultipartUploadRequest = abortMultipartUploadRequestIterator.next();
                        try {
                            clientReference.client().abortMultipartUpload(abortMultipartUploadRequest);
                            logger.info(
                                "cleaned up dangling multipart upload [{}] of blob [{}][{}][{}]",
                                abortMultipartUploadRequest.uploadId(),
                                blobStore.getRepositoryMetadata().name(),
                                abortMultipartUploadRequest.bucket(),
                                abortMultipartUploadRequest.key()
                            );
                        } catch (Exception e) {
                            // Cleanup is a best-effort thing, we can't do anything better than log and carry on here. Note that any failure
                            // is surprising, even a 404 means that something else aborted/completed the upload at a point where there
                            // should be no other processes interacting with the repository.
                            logger.warn(
                                Strings.format(
                                    "failed to clean up multipart upload [%s] of blob [%s][%s][%s]",
                                    abortMultipartUploadRequest.uploadId(),
                                    blobStore.getRepositoryMetadata().name(),
                                    abortMultipartUploadRequest.bucket(),
                                    abortMultipartUploadRequest.key()
                                ),
                                e
                            );
                        }
                    }
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.log(
                    MasterService.isPublishFailureException(e)
                        || (e instanceof RepositoryException repositoryException
                            && repositoryException.getCause() instanceof Exception cause
                            && MasterService.isPublishFailureException(cause)) ? Level.DEBUG : Level.WARN,
                    "failed to start cleanup of dangling multipart uploads",
                    e
                );
            }
        }, refs.acquire()));
    }
}
