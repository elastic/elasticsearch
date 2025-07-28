/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListNextBatchOfObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.util.ValidationUtils;

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
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
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
            return SocketAccess.doPrivileged(() -> doesObjectExist(purpose, clientReference, blobStore.bucket(), buildKey(blobName)));
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
        SocketAccess.doPrivilegedIOException(() -> {
            if (blobSize <= getLargeBlobThresholdInBytes()) {
                executeSingleUpload(purpose, blobStore, buildKey(blobName), inputStream, blobSize);
            } else {
                executeMultipartUpload(purpose, blobStore, buildKey(blobName), inputStream, blobSize);
            }
            return null;
        });
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
            AmazonS3Reference clientReference = blobStore.clientReference();
            ChunkedBlobOutputStream<PartETag> out = new ChunkedBlobOutputStream<>(blobStore.bigArrays(), blobStore.bufferSizeInBytes()) {

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
                        uploadId.set(
                            SocketAccess.doPrivileged(
                                () -> clientReference.client()
                                    .initiateMultipartUpload(initiateMultiPartUpload(purpose, absoluteBlobKey))
                                    .getUploadId()
                            )
                        );
                        if (Strings.isEmpty(uploadId.get())) {
                            throw new IOException("Failed to initialize multipart upload " + absoluteBlobKey);
                        }
                    }
                    assert lastPart == false || successful : "must only write last part if successful";
                    final UploadPartRequest uploadRequest = createPartUploadRequest(
                        purpose,
                        buffer.bytes().streamInput(),
                        uploadId.get(),
                        parts.size() + 1,
                        absoluteBlobKey,
                        buffer.size(),
                        lastPart
                    );
                    final UploadPartResult uploadResponse = SocketAccess.doPrivileged(
                        () -> clientReference.client().uploadPart(uploadRequest)
                    );
                    finishPart(uploadResponse.getPartETag());
                }

                @Override
                protected void onCompletion() throws IOException {
                    if (flushedBytes == 0L) {
                        writeBlob(purpose, blobName, buffer.bytes(), failIfAlreadyExists);
                    } else {
                        flushBuffer(true);
                        final CompleteMultipartUploadRequest complRequest = new CompleteMultipartUploadRequest(
                            blobStore.bucket(),
                            absoluteBlobKey,
                            uploadId.get(),
                            parts
                        );
                        S3BlobStore.configureRequestForMetrics(complRequest, blobStore, Operation.PUT_MULTIPART_OBJECT, purpose);
                        SocketAccess.doPrivilegedVoid(() -> clientReference.client().completeMultipartUpload(complRequest));
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
            ValidationUtils.assertStringNotEmpty(bucketName, "bucketName");
            ValidationUtils.assertStringNotEmpty(objectName, "objectName");
            final var getObjectMetadataRequest = new GetObjectMetadataRequest(bucketName, objectName);
            S3BlobStore.configureRequestForMetrics(getObjectMetadataRequest, blobStore, Operation.HEAD_OBJECT, purpose);
            clientReference.client().getObjectMetadata(getObjectMetadataRequest);
            return true;
        } catch (AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                return false;
            }
            throw e;
        }
    }

    private UploadPartRequest createPartUploadRequest(
        OperationPurpose purpose,
        InputStream stream,
        String uploadId,
        int number,
        String blobName,
        long size,
        boolean lastPart
    ) {
        final UploadPartRequest uploadRequest = new UploadPartRequest();
        uploadRequest.setBucketName(blobStore.bucket());
        uploadRequest.setKey(blobName);
        uploadRequest.setUploadId(uploadId);
        uploadRequest.setPartNumber(number);
        uploadRequest.setInputStream(stream);
        S3BlobStore.configureRequestForMetrics(uploadRequest, blobStore, Operation.PUT_MULTIPART_OBJECT, purpose);
        uploadRequest.setPartSize(size);
        uploadRequest.setLastPart(lastPart);
        return uploadRequest;
    }

    private void abortMultiPartUpload(OperationPurpose purpose, String uploadId, String blobName) {
        final AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(blobStore.bucket(), blobName, uploadId);
        S3BlobStore.configureRequestForMetrics(abortRequest, blobStore, Operation.ABORT_MULTIPART_OBJECT, purpose);
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            SocketAccess.doPrivilegedVoid(() -> clientReference.client().abortMultipartUpload(abortRequest));
        }
    }

    private InitiateMultipartUploadRequest initiateMultiPartUpload(OperationPurpose purpose, String blobName) {
        final InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(blobStore.bucket(), blobName);
        initRequest.setStorageClass(blobStore.getStorageClass());
        initRequest.setCannedACL(blobStore.getCannedACL());
        S3BlobStore.configureRequestForMetrics(initRequest, blobStore, Operation.PUT_MULTIPART_OBJECT, purpose);
        if (blobStore.serverSideEncryption()) {
            final ObjectMetadata md = new ObjectMetadata();
            md.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
            initRequest.setObjectMetadata(md);
        }
        return initRequest;
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
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            ObjectListing prevListing = null;
            while (true) {
                final ObjectListing list;
                if (prevListing != null) {
                    final var listNextBatchOfObjectsRequest = new ListNextBatchOfObjectsRequest(prevListing);
                    S3BlobStore.configureRequestForMetrics(listNextBatchOfObjectsRequest, blobStore, Operation.LIST_OBJECTS, purpose);
                    list = SocketAccess.doPrivileged(() -> clientReference.client().listNextBatchOfObjects(listNextBatchOfObjectsRequest));
                } else {
                    final ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
                    listObjectsRequest.setBucketName(blobStore.bucket());
                    listObjectsRequest.setPrefix(keyPath);
                    S3BlobStore.configureRequestForMetrics(listObjectsRequest, blobStore, Operation.LIST_OBJECTS, purpose);
                    list = SocketAccess.doPrivileged(() -> clientReference.client().listObjects(listObjectsRequest));
                }
                final Iterator<String> blobNameIterator = Iterators.map(list.getObjectSummaries().iterator(), summary -> {
                    deletedBlobs.incrementAndGet();
                    deletedBytes.addAndGet(summary.getSize());
                    return summary.getKey();
                });
                if (list.isTruncated()) {
                    blobStore.deleteBlobs(purpose, blobNameIterator);
                    prevListing = list;
                } else {
                    blobStore.deleteBlobs(purpose, Iterators.concat(blobNameIterator, Iterators.single(keyPath)));
                    break;
                }
            }
        } catch (final AmazonClientException e) {
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
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            return executeListing(
                purpose,
                clientReference,
                listObjectsRequest(purpose, blobNamePrefix == null ? keyPath : buildKey(blobNamePrefix))
            ).stream()
                .flatMap(listing -> listing.getObjectSummaries().stream())
                .map(summary -> new BlobMetadata(summary.getKey().substring(keyPath.length()), summary.getSize()))
                .collect(Collectors.toMap(BlobMetadata::name, Function.identity()));
        } catch (final AmazonClientException e) {
            throw new IOException("Exception when listing blobs by prefix [" + blobNamePrefix + "]", e);
        }
    }

    @Override
    public Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException {
        return listBlobsByPrefix(purpose, null);
    }

    @Override
    public Map<String, BlobContainer> children(OperationPurpose purpose) throws IOException {
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            return executeListing(purpose, clientReference, listObjectsRequest(purpose, keyPath)).stream().flatMap(listing -> {
                assert listing.getObjectSummaries().stream().noneMatch(s -> {
                    for (String commonPrefix : listing.getCommonPrefixes()) {
                        if (s.getKey().substring(keyPath.length()).startsWith(commonPrefix)) {
                            return true;
                        }
                    }
                    return false;
                }) : "Response contained children for listed common prefixes.";
                return listing.getCommonPrefixes().stream();
            })
                .map(prefix -> prefix.substring(keyPath.length()))
                .filter(name -> name.isEmpty() == false)
                // Stripping the trailing slash off of the common prefix
                .map(name -> name.substring(0, name.length() - 1))
                .collect(Collectors.toMap(Function.identity(), name -> blobStore.blobContainer(path().add(name))));
        } catch (final AmazonClientException e) {
            throw new IOException("Exception when listing children of [" + path().buildAsString() + ']', e);
        }
    }

    private List<ObjectListing> executeListing(
        OperationPurpose purpose,
        AmazonS3Reference clientReference,
        ListObjectsRequest listObjectsRequest
    ) {
        final List<ObjectListing> results = new ArrayList<>();
        ObjectListing prevListing = null;
        while (true) {
            ObjectListing list;
            if (prevListing != null) {
                final var listNextBatchOfObjectsRequest = new ListNextBatchOfObjectsRequest(prevListing);
                S3BlobStore.configureRequestForMetrics(listNextBatchOfObjectsRequest, blobStore, Operation.LIST_OBJECTS, purpose);
                list = SocketAccess.doPrivileged(() -> clientReference.client().listNextBatchOfObjects(listNextBatchOfObjectsRequest));
            } else {
                list = SocketAccess.doPrivileged(() -> clientReference.client().listObjects(listObjectsRequest));
            }
            results.add(list);
            if (list.isTruncated()) {
                prevListing = list;
            } else {
                break;
            }
        }
        return results;
    }

    private ListObjectsRequest listObjectsRequest(OperationPurpose purpose, String pathPrefix) {
        final ListObjectsRequest listObjectsRequest = new ListObjectsRequest().withBucketName(blobStore.bucket())
            .withPrefix(pathPrefix)
            .withDelimiter("/");
        S3BlobStore.configureRequestForMetrics(listObjectsRequest, blobStore, Operation.LIST_OBJECTS, purpose);
        return listObjectsRequest;
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

        // Extra safety checks
        if (blobSize > MAX_FILE_SIZE.getBytes()) {
            throw new IllegalArgumentException("Upload request size [" + blobSize + "] can't be larger than " + MAX_FILE_SIZE);
        }
        if (blobSize > s3BlobStore.bufferSizeInBytes()) {
            throw new IllegalArgumentException("Upload request size [" + blobSize + "] can't be larger than buffer size");
        }

        final ObjectMetadata md = new ObjectMetadata();
        md.setContentLength(blobSize);
        if (s3BlobStore.serverSideEncryption()) {
            md.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        }
        final PutObjectRequest putRequest = new PutObjectRequest(s3BlobStore.bucket(), blobName, input, md);
        putRequest.setStorageClass(s3BlobStore.getStorageClass());
        putRequest.setCannedAcl(s3BlobStore.getCannedACL());
        S3BlobStore.configureRequestForMetrics(putRequest, blobStore, Operation.PUT_OBJECT, purpose);

        try (AmazonS3Reference clientReference = s3BlobStore.clientReference()) {
            SocketAccess.doPrivilegedVoid(() -> { clientReference.client().putObject(putRequest); });
        } catch (final AmazonClientException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using a single upload", e);
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

        ensureMultiPartUploadSize(blobSize);
        final long partSize = s3BlobStore.bufferSizeInBytes();
        final Tuple<Long, Long> multiparts = numberOfMultiparts(blobSize, partSize);

        if (multiparts.v1() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Too many multipart upload requests, maybe try a larger buffer size?");
        }

        final int nbParts = multiparts.v1().intValue();
        final long lastPartSize = multiparts.v2();
        assert blobSize == (((nbParts - 1) * partSize) + lastPartSize) : "blobSize does not match multipart sizes";

        final SetOnce<String> uploadId = new SetOnce<>();
        final String bucketName = s3BlobStore.bucket();
        boolean success = false;
        try (AmazonS3Reference clientReference = s3BlobStore.clientReference()) {

            uploadId.set(
                SocketAccess.doPrivileged(
                    () -> clientReference.client().initiateMultipartUpload(initiateMultiPartUpload(purpose, blobName)).getUploadId()
                )
            );
            if (Strings.isEmpty(uploadId.get())) {
                throw new IOException("Failed to initialize multipart upload " + blobName);
            }

            final List<PartETag> parts = new ArrayList<>();

            long bytesCount = 0;
            for (int i = 1; i <= nbParts; i++) {
                final boolean lastPart = i == nbParts;
                final UploadPartRequest uploadRequest = createPartUploadRequest(
                    purpose,
                    input,
                    uploadId.get(),
                    i,
                    blobName,
                    lastPart ? lastPartSize : partSize,
                    lastPart
                );
                bytesCount += uploadRequest.getPartSize();

                final UploadPartResult uploadResponse = SocketAccess.doPrivileged(() -> clientReference.client().uploadPart(uploadRequest));
                parts.add(uploadResponse.getPartETag());
            }

            if (bytesCount != blobSize) {
                throw new IOException(
                    "Failed to execute multipart upload for [" + blobName + "], expected " + blobSize + "bytes sent but got " + bytesCount
                );
            }

            final CompleteMultipartUploadRequest complRequest = new CompleteMultipartUploadRequest(
                bucketName,
                blobName,
                uploadId.get(),
                parts
            );
            S3BlobStore.configureRequestForMetrics(complRequest, blobStore, Operation.PUT_MULTIPART_OBJECT, purpose);
            SocketAccess.doPrivilegedVoid(() -> clientReference.client().completeMultipartUpload(complRequest));
            success = true;

        } catch (final AmazonClientException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using multipart upload", e);
        } finally {
            if ((success == false) && Strings.hasLength(uploadId.get())) {
                abortMultiPartUpload(purpose, uploadId.get(), blobName);
            }
        }
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
        private final AmazonS3 client;
        private final String bucket;
        private final String rawKey;
        private final String blobKey;
        private final ThreadPool threadPool;

        CompareAndExchangeOperation(OperationPurpose purpose, AmazonS3 client, String bucket, String key, ThreadPool threadPool) {
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

            final var uploadId = initiateMultipartUpload();
            logger.trace("[{}] initiated upload [{}]", blobKey, uploadId);
            final var partETag = uploadPart(updated, uploadId);
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
            final var uploads = listMultipartUploads();
            logUploads("preexisting uploads", uploads);

            if (uploads.isEmpty()) {
                logger.trace("[{}] no preexisting uploads", blobKey);
                return false;
            }

            final var expiryDate = Date.from(
                Instant.ofEpochMilli(
                    blobStore.getThreadPool().absoluteTimeInMillis() - blobStore.getCompareAndExchangeTimeToLive().millis()
                )
            );
            if (uploads.stream().anyMatch(upload -> upload.getInitiated().after(expiryDate))) {
                logger.trace("[{}] fresh preexisting uploads vs {}", blobKey, expiryDate);
                return true;
            }

            // there are uploads, but they are all older than the TTL, so clean them up before carrying on (should be rare)
            for (final var upload : uploads) {
                logger.warn(
                    "cleaning up stale compare-and-swap upload [{}] initiated at [{}]",
                    upload.getUploadId(),
                    upload.getInitiated()
                );
                safeAbortMultipartUpload(upload.getUploadId());
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
                        .map(multipartUpload -> multipartUpload.getUploadId() + ": " + multipartUpload.getInitiated())
                        .collect(Collectors.joining(","))
                );
            }
        }

        private List<MultipartUpload> listMultipartUploads() {
            final var listRequest = new ListMultipartUploadsRequest(bucket);
            listRequest.setPrefix(blobKey);
            S3BlobStore.configureRequestForMetrics(listRequest, blobStore, Operation.LIST_OBJECTS, purpose);
            try {
                return SocketAccess.doPrivileged(() -> client.listMultipartUploads(listRequest)).getMultipartUploads();
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == 404) {
                    return List.of();
                }
                throw e;
            }
        }

        private String initiateMultipartUpload() {
            final var initiateRequest = new InitiateMultipartUploadRequest(bucket, blobKey);
            S3BlobStore.configureRequestForMetrics(initiateRequest, blobStore, Operation.PUT_MULTIPART_OBJECT, purpose);
            return SocketAccess.doPrivileged(() -> client.initiateMultipartUpload(initiateRequest)).getUploadId();
        }

        private PartETag uploadPart(BytesReference updated, String uploadId) throws IOException {
            final var uploadPartRequest = new UploadPartRequest();
            uploadPartRequest.setBucketName(bucket);
            uploadPartRequest.setKey(blobKey);
            uploadPartRequest.setUploadId(uploadId);
            uploadPartRequest.setPartNumber(1);
            uploadPartRequest.setLastPart(true);
            uploadPartRequest.setInputStream(updated.streamInput());
            uploadPartRequest.setPartSize(updated.length());
            S3BlobStore.configureRequestForMetrics(uploadPartRequest, blobStore, Operation.PUT_MULTIPART_OBJECT, purpose);
            return SocketAccess.doPrivileged(() -> client.uploadPart(uploadPartRequest)).getPartETag();
        }

        private int getUploadIndex(String targetUploadId, List<MultipartUpload> multipartUploads) {
            var uploadIndex = 0;
            var found = false;
            for (MultipartUpload multipartUpload : multipartUploads) {
                final var observedUploadId = multipartUpload.getUploadId();
                if (observedUploadId.equals(targetUploadId)) {
                    final var currentTimeMillis = blobStore.getThreadPool().absoluteTimeInMillis();
                    final var ageMillis = currentTimeMillis - multipartUpload.getInitiated().toInstant().toEpochMilli();
                    final var expectedAgeRangeMillis = blobStore.getCompareAndExchangeTimeToLive().millis();
                    if (ageMillis < -expectedAgeRangeMillis || ageMillis > expectedAgeRangeMillis) {
                        logger.warn(
                            """
                                compare-and-exchange of blob [{}:{}] was initiated at [{}={}] \
                                which deviates from local node epoch time [{}] by more than the warn threshold of [{}ms]""",
                            bucket,
                            blobKey,
                            multipartUpload.getInitiated(),
                            multipartUpload.getInitiated().toInstant().toEpochMilli(),
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
                    final var currentUploadId = currentUpload.getUploadId();
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
                final var request = new AbortMultipartUploadRequest(bucket, blobKey, uploadId);
                S3BlobStore.configureRequestForMetrics(request, blobStore, Operation.ABORT_MULTIPART_OBJECT, purpose);
                SocketAccess.doPrivilegedVoid(() -> client.abortMultipartUpload(request));
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() != 404) {
                    throw e;
                }
                // else already aborted
            }
        }

        private void completeMultipartUpload(String uploadId, PartETag partETag) {
            final var completeMultipartUploadRequest = new CompleteMultipartUploadRequest(bucket, blobKey, uploadId, List.of(partETag));
            S3BlobStore.configureRequestForMetrics(completeMultipartUploadRequest, blobStore, Operation.PUT_MULTIPART_OBJECT, purpose);
            SocketAccess.doPrivilegedVoid(() -> client.completeMultipartUpload(completeMultipartUploadRequest));
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
            if (e instanceof AmazonS3Exception amazonS3Exception
                && (amazonS3Exception.getStatusCode() == 404
                    || amazonS3Exception.getStatusCode() == 0 && "NoSuchUpload".equals(amazonS3Exception.getErrorCode()))) {
                // An uncaught 404 means that our multipart upload was aborted by a concurrent operation before we could complete it.
                // Also (rarely) S3 can start processing the request during a concurrent abort and this can result in a 200 OK with an
                // <Error><Code>NoSuchUpload</Code>... in the response, which the SDK translates to status code 0. Either way, this means
                // that our write encountered contention:
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
            final var getObjectRequest = new GetObjectRequest(blobStore.bucket(), buildKey(key));
            S3BlobStore.configureRequestForMetrics(getObjectRequest, blobStore, Operation.GET_OBJECT, purpose);
            try (
                var clientReference = blobStore.clientReference();
                var s3Object = SocketAccess.doPrivileged(() -> clientReference.client().getObject(getObjectRequest));
                var stream = s3Object.getObjectContent()
            ) {
                return OptionalBytesReference.of(getRegisterUsingConsistentRead(stream, keyPath, key));
            } catch (AmazonS3Exception e) {
                logger.trace(() -> Strings.format("[%s]: getRegister failed", key), e);
                if (e.getStatusCode() == 404) {
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
            final var request = new ListMultipartUploadsRequest(bucket).withPrefix(keyPath).withMaxUploads(maxUploads);
            blobStore.addPurposeQueryParameter(OperationPurpose.SNAPSHOT_DATA, request);
            final var multipartUploadListing = SocketAccess.doPrivileged(() -> clientReference.client().listMultipartUploads(request));
            final var multipartUploads = multipartUploadListing.getMultipartUploads();
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
                    multipartUploads.stream().map(u -> new AbortMultipartUploadRequest(bucket, u.getKey(), u.getUploadId())).toList()
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
        List<AbortMultipartUploadRequest> abortMultipartUploadRequests
    ) {
        return new ThreadedActionListener<>(blobStore.getSnapshotExecutor(), ActionListener.releaseAfter(new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                try (var clientReference = blobStore.clientReference()) {
                    for (final var abortMultipartUploadRequest : abortMultipartUploadRequests) {
                        blobStore.addPurposeQueryParameter(OperationPurpose.SNAPSHOT_DATA, abortMultipartUploadRequest);
                        try {
                            SocketAccess.doPrivilegedVoid(() -> clientReference.client().abortMultipartUpload(abortMultipartUploadRequest));
                            logger.info(
                                "cleaned up dangling multipart upload [{}] of blob [{}][{}][{}]",
                                abortMultipartUploadRequest.getUploadId(),
                                blobStore.getRepositoryMetadata().name(),
                                abortMultipartUploadRequest.getBucketName(),
                                abortMultipartUploadRequest.getKey()
                            );
                        } catch (Exception e) {
                            // Cleanup is a best-effort thing, we can't do anything better than log and carry on here. Note that any failure
                            // is surprising, even a 404 means that something else aborted/completed the upload at a point where there
                            // should be no other processes interacting with the repository.
                            logger.warn(
                                Strings.format(
                                    "failed to clean up multipart upload [%s] of blob [%s][%s][%s]",
                                    abortMultipartUploadRequest.getUploadId(),
                                    blobStore.getRepositoryMetadata().name(),
                                    abortMultipartUploadRequest.getBucketName(),
                                    abortMultipartUploadRequest.getKey()
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
