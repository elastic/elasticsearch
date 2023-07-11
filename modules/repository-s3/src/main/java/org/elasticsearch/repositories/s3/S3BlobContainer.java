/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.DeleteResult;
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
import org.elasticsearch.repositories.blobstore.ChunkedBlobOutputStream;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.common.blobstore.support.BlobContainerUtils.getRegisterUsingConsistentRead;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.repositories.s3.S3Repository.MAX_FILE_SIZE;
import static org.elasticsearch.repositories.s3.S3Repository.MAX_FILE_SIZE_USING_MULTIPART;
import static org.elasticsearch.repositories.s3.S3Repository.MIN_PART_SIZE_USING_MULTIPART;

class S3BlobContainer extends AbstractBlobContainer {

    private static final Logger logger = LogManager.getLogger(S3BlobContainer.class);

    /**
     * Maximum number of deletes in a {@link DeleteObjectsRequest}.
     * @see <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/multiobjectdeleteapi.html">S3 Documentation</a>.
     */
    private static final int MAX_BULK_DELETES = 1000;

    private final S3BlobStore blobStore;
    private final String keyPath;

    S3BlobContainer(BlobPath path, S3BlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        this.keyPath = path.buildAsString();
    }

    @Override
    public boolean blobExists(String blobName) {
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            return SocketAccess.doPrivileged(() -> clientReference.client().doesObjectExist(blobStore.bucket(), buildKey(blobName)));
        } catch (final Exception e) {
            throw new BlobStoreException("Failed to check if blob [" + blobName + "] exists", e);
        }
    }

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        return new S3RetryingInputStream(blobStore, buildKey(blobName));
    }

    @Override
    public InputStream readBlob(String blobName, long position, long length) throws IOException {
        if (position < 0L) {
            throw new IllegalArgumentException("position must be non-negative");
        }
        if (length < 0) {
            throw new IllegalArgumentException("length must be non-negative");
        }
        if (length == 0) {
            return new ByteArrayInputStream(new byte[0]);
        } else {
            return new S3RetryingInputStream(blobStore, buildKey(blobName), position, Math.addExact(position, length - 1));
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
    public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        assert inputStream.markSupported() : "No mark support on inputStream breaks the S3 SDK's ability to retry requests";
        SocketAccess.doPrivilegedIOException(() -> {
            if (blobSize <= getLargeBlobThresholdInBytes()) {
                executeSingleUpload(blobStore, buildKey(blobName), inputStream, blobSize);
            } else {
                executeMultipartUpload(blobStore, buildKey(blobName), inputStream, blobSize);
            }
            return null;
        });
    }

    @Override
    public void writeMetadataBlob(
        String blobName,
        boolean failIfAlreadyExists,
        boolean atomic,
        CheckedConsumer<OutputStream, IOException> writer
    ) throws IOException {
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
                                    .initiateMultipartUpload(initiateMultiPartUpload(absoluteBlobKey))
                                    .getUploadId()
                            )
                        );
                        if (Strings.isEmpty(uploadId.get())) {
                            throw new IOException("Failed to initialize multipart upload " + absoluteBlobKey);
                        }
                    }
                    assert lastPart == false || successful : "must only write last part if successful";
                    final UploadPartRequest uploadRequest = createPartUploadRequest(
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
                        writeBlob(blobName, buffer.bytes(), failIfAlreadyExists);
                    } else {
                        flushBuffer(true);
                        final CompleteMultipartUploadRequest complRequest = new CompleteMultipartUploadRequest(
                            blobStore.bucket(),
                            absoluteBlobKey,
                            uploadId.get(),
                            parts
                        );
                        complRequest.setRequestMetricCollector(blobStore.multiPartUploadMetricCollector);
                        SocketAccess.doPrivilegedVoid(() -> clientReference.client().completeMultipartUpload(complRequest));
                    }
                }

                @Override
                protected void onFailure() {
                    if (Strings.hasText(uploadId.get())) {
                        abortMultiPartUpload(uploadId.get(), absoluteBlobKey);
                    }
                }
            }
        ) {
            writer.accept(out);
            out.markSuccess();
        }
    }

    private UploadPartRequest createPartUploadRequest(
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
        uploadRequest.setRequestMetricCollector(blobStore.multiPartUploadMetricCollector);
        uploadRequest.setPartSize(size);
        uploadRequest.setLastPart(lastPart);
        return uploadRequest;
    }

    private void abortMultiPartUpload(String uploadId, String blobName) {
        final AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(blobStore.bucket(), blobName, uploadId);
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            SocketAccess.doPrivilegedVoid(() -> clientReference.client().abortMultipartUpload(abortRequest));
        }
    }

    private InitiateMultipartUploadRequest initiateMultiPartUpload(String blobName) {
        final InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(blobStore.bucket(), blobName);
        initRequest.setStorageClass(blobStore.getStorageClass());
        initRequest.setCannedACL(blobStore.getCannedACL());
        initRequest.setRequestMetricCollector(blobStore.multiPartUploadMetricCollector);
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
    public void writeBlobAtomic(String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
        writeBlob(blobName, bytes, failIfAlreadyExists);
    }

    @Override
    @SuppressWarnings("unchecked")
    public DeleteResult delete() throws IOException {
        final AtomicLong deletedBlobs = new AtomicLong();
        final AtomicLong deletedBytes = new AtomicLong();
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            ObjectListing prevListing = null;
            while (true) {
                ObjectListing list;
                if (prevListing != null) {
                    final ObjectListing finalPrevListing = prevListing;
                    list = SocketAccess.doPrivileged(() -> clientReference.client().listNextBatchOfObjects(finalPrevListing));
                } else {
                    final ListObjectsRequest listObjectsRequest = new ListObjectsRequest();
                    listObjectsRequest.setBucketName(blobStore.bucket());
                    listObjectsRequest.setPrefix(keyPath);
                    listObjectsRequest.setRequestMetricCollector(blobStore.listMetricCollector);
                    list = SocketAccess.doPrivileged(() -> clientReference.client().listObjects(listObjectsRequest));
                }
                final Iterator<S3ObjectSummary> objectSummaryIterator = list.getObjectSummaries().iterator();
                final Iterator<String> blobNameIterator = new Iterator<>() {
                    @Override
                    public boolean hasNext() {
                        return objectSummaryIterator.hasNext();
                    }

                    @Override
                    public String next() {
                        final S3ObjectSummary summary = objectSummaryIterator.next();
                        deletedBlobs.incrementAndGet();
                        deletedBytes.addAndGet(summary.getSize());
                        return summary.getKey();
                    }
                };
                if (list.isTruncated()) {
                    doDeleteBlobs(blobNameIterator, false);
                    prevListing = list;
                } else {
                    doDeleteBlobs(Iterators.concat(blobNameIterator, Collections.singletonList(keyPath).iterator()), false);
                    break;
                }
            }
        } catch (final AmazonClientException e) {
            throw new IOException("Exception when deleting blob container [" + keyPath + "]", e);
        }
        return new DeleteResult(deletedBlobs.get(), deletedBytes.get());
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(Iterator<String> blobNames) throws IOException {
        doDeleteBlobs(blobNames, true);
    }

    private void doDeleteBlobs(Iterator<String> blobNames, boolean relative) throws IOException {
        if (blobNames.hasNext() == false) {
            return;
        }
        final Iterator<String> outstanding;
        if (relative) {
            outstanding = new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return blobNames.hasNext();
                }

                @Override
                public String next() {
                    return buildKey(blobNames.next());
                }
            };
        } else {
            outstanding = blobNames;
        }

        final List<String> partition = new ArrayList<>();
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            // S3 API only allows 1k blobs per delete so we split up the given blobs into requests of max. 1k deletes
            final AtomicReference<Exception> aex = new AtomicReference<>();
            SocketAccess.doPrivilegedVoid(() -> {
                outstanding.forEachRemaining(key -> {
                    partition.add(key);
                    if (partition.size() == MAX_BULK_DELETES) {
                        deletePartition(clientReference, partition, aex);
                        partition.clear();
                    }
                });
                if (partition.isEmpty() == false) {
                    deletePartition(clientReference, partition, aex);
                }
            });
            if (aex.get() != null) {
                throw aex.get();
            }
        } catch (Exception e) {
            throw new IOException("Failed to delete blobs " + partition.stream().limit(10).toList(), e);
        }
    }

    private void deletePartition(AmazonS3Reference clientReference, List<String> partition, AtomicReference<Exception> aex) {
        try {
            clientReference.client().deleteObjects(bulkDelete(blobStore.bucket(), partition));
        } catch (MultiObjectDeleteException e) {
            // We are sending quiet mode requests so we can't use the deleted keys entry on the exception and instead
            // first remove all keys that were sent in the request and then add back those that ran into an exception.
            logger.warn(
                () -> format(
                    "Failed to delete some blobs %s",
                    e.getErrors().stream().map(err -> "[" + err.getKey() + "][" + err.getCode() + "][" + err.getMessage() + "]").toList()
                ),
                e
            );
            aex.set(ExceptionsHelper.useOrSuppress(aex.get(), e));
        } catch (AmazonClientException e) {
            // The AWS client threw any unexpected exception and did not execute the request at all so we do not
            // remove any keys from the outstanding deletes set.
            aex.set(ExceptionsHelper.useOrSuppress(aex.get(), e));
        }
    }

    private static DeleteObjectsRequest bulkDelete(String bucket, List<String> blobs) {
        return new DeleteObjectsRequest(bucket).withKeys(blobs.toArray(Strings.EMPTY_ARRAY)).withQuiet(true);
    }

    @Override
    public Map<String, BlobMetadata> listBlobsByPrefix(@Nullable String blobNamePrefix) throws IOException {
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            return executeListing(clientReference, listObjectsRequest(blobNamePrefix == null ? keyPath : buildKey(blobNamePrefix))).stream()
                .flatMap(listing -> listing.getObjectSummaries().stream())
                .map(summary -> new BlobMetadata(summary.getKey().substring(keyPath.length()), summary.getSize()))
                .collect(Collectors.toMap(BlobMetadata::name, Function.identity()));
        } catch (final AmazonClientException e) {
            throw new IOException("Exception when listing blobs by prefix [" + blobNamePrefix + "]", e);
        }
    }

    @Override
    public Map<String, BlobMetadata> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    @Override
    public Map<String, BlobContainer> children() throws IOException {
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            return executeListing(clientReference, listObjectsRequest(keyPath)).stream().flatMap(listing -> {
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

    private static List<ObjectListing> executeListing(AmazonS3Reference clientReference, ListObjectsRequest listObjectsRequest) {
        final List<ObjectListing> results = new ArrayList<>();
        ObjectListing prevListing = null;
        while (true) {
            ObjectListing list;
            if (prevListing != null) {
                final ObjectListing finalPrevListing = prevListing;
                list = SocketAccess.doPrivileged(() -> clientReference.client().listNextBatchOfObjects(finalPrevListing));
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

    private ListObjectsRequest listObjectsRequest(String pathPrefix) {
        return new ListObjectsRequest().withBucketName(blobStore.bucket())
            .withPrefix(pathPrefix)
            .withDelimiter("/")
            .withRequestMetricCollector(blobStore.listMetricCollector);
    }

    private String buildKey(String blobName) {
        return keyPath + blobName;
    }

    /**
     * Uploads a blob using a single upload request
     */
    static void executeSingleUpload(final S3BlobStore s3BlobStore, final String blobName, final InputStream input, final long blobSize)
        throws IOException {

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
        putRequest.setRequestMetricCollector(s3BlobStore.putMetricCollector);

        try (AmazonS3Reference clientReference = s3BlobStore.clientReference()) {
            SocketAccess.doPrivilegedVoid(() -> { clientReference.client().putObject(putRequest); });
        } catch (final AmazonClientException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using a single upload", e);
        }
    }

    /**
     * Uploads a blob using multipart upload requests.
     */
    void executeMultipartUpload(final S3BlobStore s3BlobStore, final String blobName, final InputStream input, final long blobSize)
        throws IOException {

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
                    () -> clientReference.client().initiateMultipartUpload(initiateMultiPartUpload(blobName)).getUploadId()
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
            complRequest.setRequestMetricCollector(s3BlobStore.multiPartUploadMetricCollector);
            SocketAccess.doPrivilegedVoid(() -> clientReference.client().completeMultipartUpload(complRequest));
            success = true;

        } catch (final AmazonClientException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using multipart upload", e);
        } finally {
            if ((success == false) && Strings.hasLength(uploadId.get())) {
                abortMultiPartUpload(uploadId.get(), blobName);
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

        private final AmazonS3 client;
        private final String bucket;
        private final String rawKey;
        private final String blobKey;
        private final ThreadPool threadPool;

        CompareAndExchangeOperation(AmazonS3 client, String bucket, String key, ThreadPool threadPool) {
            this.client = client;
            this.bucket = bucket;
            this.rawKey = key;
            this.blobKey = buildKey(key);
            this.threadPool = threadPool;
        }

        private List<MultipartUpload> listMultipartUploads() {
            final var listRequest = new ListMultipartUploadsRequest(bucket);
            listRequest.setPrefix(blobKey);
            listRequest.setRequestMetricCollector(blobStore.listMetricCollector);
            try {
                return SocketAccess.doPrivileged(() -> client.listMultipartUploads(listRequest)).getMultipartUploads();
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == 404) {
                    return List.of();
                }
                throw e;
            }
        }

        private static int getUploadIndex(String targetUploadId, List<MultipartUpload> multipartUploads) {
            var uploadIndex = 0;
            var found = false;
            for (MultipartUpload multipartUpload : multipartUploads) {
                final var observedUploadId = multipartUpload.getUploadId();
                if (observedUploadId.equals(targetUploadId)) {
                    found = true;
                } else if (observedUploadId.compareTo(targetUploadId) < 0) {
                    uploadIndex += 1;
                }
            }

            return found ? uploadIndex : -1;
        }

        void run(BytesReference expected, BytesReference updated, ActionListener<OptionalBytesReference> listener) throws Exception {

            BlobContainerUtils.ensureValidRegisterContent(updated);

            if (listMultipartUploads().isEmpty() == false) {
                // TODO What if the previous writer crashed? We should consider the age of any ongoing uploads before bailing out like this.
                listener.onResponse(OptionalBytesReference.MISSING);
                return;
            }

            final var initiateRequest = new InitiateMultipartUploadRequest(bucket, blobKey);
            initiateRequest.setRequestMetricCollector(blobStore.multiPartUploadMetricCollector);
            final var uploadId = SocketAccess.doPrivileged(() -> client.initiateMultipartUpload(initiateRequest)).getUploadId();

            final var uploadPartRequest = new UploadPartRequest();
            uploadPartRequest.setBucketName(bucket);
            uploadPartRequest.setKey(blobKey);
            uploadPartRequest.setUploadId(uploadId);
            uploadPartRequest.setPartNumber(1);
            uploadPartRequest.setLastPart(true);
            uploadPartRequest.setInputStream(updated.streamInput());
            uploadPartRequest.setPartSize(updated.length());
            uploadPartRequest.setRequestMetricCollector(blobStore.multiPartUploadMetricCollector);
            final var partETag = SocketAccess.doPrivileged(() -> client.uploadPart(uploadPartRequest)).getPartETag();

            final var currentUploads = listMultipartUploads();
            final var uploadIndex = getUploadIndex(uploadId, currentUploads);

            if (uploadIndex < 0) {
                // already aborted by someone else
                listener.onResponse(OptionalBytesReference.MISSING);
                return;
            }

            final var isComplete = new AtomicBoolean();
            final Runnable doCleanup = () -> {
                if (isComplete.compareAndSet(false, true)) {
                    try {
                        abortMultipartUploadIfExists(uploadId);
                    } catch (Exception e) {
                        // cleanup is a best-effort thing, we can't do anything better than log and fall through here
                        logger.error("unexpected error cleaning up upload [" + uploadId + "] of [" + blobKey + "]", e);
                        assert false : e;
                    }
                }
            };

            try (
                var listeners = new RefCountingListener(
                    ActionListener.runAfter(
                        listener.delegateFailure(
                            (delegate1, ignored) -> getRegister(
                                rawKey,
                                delegate1.delegateFailure((delegate2, currentValue) -> ActionListener.completeWith(delegate2, () -> {
                                    if (currentValue.isPresent() && currentValue.bytesReference().equals(expected)) {
                                        final var completeMultipartUploadRequest = new CompleteMultipartUploadRequest(
                                            bucket,
                                            blobKey,
                                            uploadId,
                                            List.of(partETag)
                                        );
                                        completeMultipartUploadRequest.setRequestMetricCollector(blobStore.multiPartUploadMetricCollector);
                                        SocketAccess.doPrivilegedVoid(() -> client.completeMultipartUpload(completeMultipartUploadRequest));
                                        isComplete.set(true);
                                    }
                                    return currentValue;
                                }))
                            )
                        ),
                        doCleanup
                    )
                )
            ) {
                if (currentUploads.size() > 1) {
                    // This is a small optimization to improve the liveness properties of this algorithm.
                    //
                    // When there are multiple competing updates, we order them by upload id and the first one tries to cancel the competing
                    // updates in order to make progress. To avoid liveness issues when the winner fails, the rest wait based on their
                    // upload_id-based position and try to make progress.

                    var delayListener = listeners.acquire();
                    final Runnable cancelConcurrentUpdates = () -> {
                        try {
                            for (MultipartUpload currentUpload : currentUploads) {
                                final var currentUploadId = currentUpload.getUploadId();
                                if (uploadId.equals(currentUploadId) == false) {
                                    threadPool.executor(ThreadPool.Names.SNAPSHOT)
                                        .execute(
                                            ActionRunnable.run(listeners.acquire(), () -> abortMultipartUploadIfExists(currentUploadId))
                                        );
                                }
                            }
                        } finally {
                            delayListener.onResponse(null);
                        }
                    };

                    if (uploadIndex > 0) {
                        threadPool.scheduleUnlessShuttingDown(
                            TimeValue.timeValueMillis(TimeValue.timeValueSeconds(uploadIndex).millis() + Randomness.get().nextInt(50)),
                            ThreadPool.Names.SNAPSHOT,
                            cancelConcurrentUpdates
                        );
                    } else {
                        cancelConcurrentUpdates.run();
                    }
                }
            }
        }

        private void abortMultipartUploadIfExists(String uploadId) {
            try {
                final var request = new AbortMultipartUploadRequest(bucket, blobKey, uploadId);
                SocketAccess.doPrivilegedVoid(() -> client.abortMultipartUpload(request));
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() != 404) {
                    throw e;
                }
                // else already aborted
            }
        }

    }

    @Override
    public void compareAndExchangeRegister(
        String key,
        BytesReference expected,
        BytesReference updated,
        ActionListener<OptionalBytesReference> listener
    ) {
        final var clientReference = blobStore.clientReference();
        ActionListener.run(ActionListener.releaseAfter(listener.delegateResponse((delegate, e) -> {
            if (e instanceof AmazonS3Exception amazonS3Exception && amazonS3Exception.getStatusCode() == 404) {
                // an uncaught 404 means that our multipart upload was aborted by a concurrent operation before we could complete it
                delegate.onResponse(OptionalBytesReference.MISSING);
            } else {
                delegate.onFailure(e);
            }
        }), clientReference),
            l -> new CompareAndExchangeOperation(clientReference.client(), blobStore.bucket(), key, blobStore.getThreadPool()).run(
                expected,
                updated,
                l
            )
        );
    }

    @Override
    public void getRegister(String key, ActionListener<OptionalBytesReference> listener) {
        ActionListener.completeWith(listener, () -> {
            final var getObjectRequest = new GetObjectRequest(blobStore.bucket(), buildKey(key));
            getObjectRequest.setRequestMetricCollector(blobStore.getMetricCollector);
            try (
                var clientReference = blobStore.clientReference();
                var s3Object = SocketAccess.doPrivileged(() -> clientReference.client().getObject(getObjectRequest));
                var stream = s3Object.getObjectContent()
            ) {
                return OptionalBytesReference.of(getRegisterUsingConsistentRead(stream, keyPath, key));
            } catch (AmazonS3Exception e) {
                if (e.getStatusCode() == 404) {
                    return OptionalBytesReference.EMPTY;
                } else {
                    throw e;
                }
            }
        });
    }
}
