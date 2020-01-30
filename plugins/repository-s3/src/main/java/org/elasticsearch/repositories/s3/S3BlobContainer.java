/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.s3;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.DeleteResult;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.Tuple;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

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
    public InputStream readBlob(String blobName) throws IOException {
        return new S3RetryingInputStream(blobStore, buildKey(blobName));
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

    // package private for testing
    long getLargeBlobThresholdInBytes() {
        return blobStore.bufferSizeInBytes();
    }

    @Override
    public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException {
        writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
    }

    @Override
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
                    list = SocketAccess.doPrivileged(() -> clientReference.client().listObjects(listObjectsRequest));
                }
                final List<String> blobsToDelete = new ArrayList<>();
                    list.getObjectSummaries().forEach(s3ObjectSummary -> {
                        deletedBlobs.incrementAndGet();
                        deletedBytes.addAndGet(s3ObjectSummary.getSize());
                        blobsToDelete.add(s3ObjectSummary.getKey());
                    });
                if (list.isTruncated()) {
                    doDeleteBlobs(blobsToDelete, false);
                    prevListing = list;
                } else {
                    final List<String> lastBlobsToDelete = new ArrayList<>(blobsToDelete);
                    lastBlobsToDelete.add(keyPath);
                    doDeleteBlobs(lastBlobsToDelete, false);
                    break;
                }
            }
        } catch (final AmazonClientException e) {
            throw new IOException("Exception when deleting blob container [" + keyPath + "]", e);
        }
        return new DeleteResult(deletedBlobs.get(), deletedBytes.get());
    }

    @Override
    public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        doDeleteBlobs(blobNames, true);
    }

    private void doDeleteBlobs(List<String> blobNames, boolean relative) throws IOException {
        if (blobNames.isEmpty()) {
            return;
        }
        final Set<String> outstanding;
        if (relative) {
            outstanding = blobNames.stream().map(this::buildKey).collect(Collectors.toSet());
        } else {
            outstanding = new HashSet<>(blobNames);
        }
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            // S3 API only allows 1k blobs per delete so we split up the given blobs into requests of max. 1k deletes
            final List<DeleteObjectsRequest> deleteRequests = new ArrayList<>();
            final List<String> partition = new ArrayList<>();
            for (String key : outstanding) {
                partition.add(key);
                if (partition.size() == MAX_BULK_DELETES ) {
                    deleteRequests.add(bulkDelete(blobStore.bucket(), partition));
                    partition.clear();
                }
            }
            if (partition.isEmpty() == false) {
                deleteRequests.add(bulkDelete(blobStore.bucket(), partition));
            }
            SocketAccess.doPrivilegedVoid(() -> {
                AmazonClientException aex = null;
                for (DeleteObjectsRequest deleteRequest : deleteRequests) {
                    List<String> keysInRequest =
                        deleteRequest.getKeys().stream().map(DeleteObjectsRequest.KeyVersion::getKey).collect(Collectors.toList());
                    try {
                        clientReference.client().deleteObjects(deleteRequest);
                        outstanding.removeAll(keysInRequest);
                    } catch (MultiObjectDeleteException e) {
                        // We are sending quiet mode requests so we can't use the deleted keys entry on the exception and instead
                        // first remove all keys that were sent in the request and then add back those that ran into an exception.
                        outstanding.removeAll(keysInRequest);
                        outstanding.addAll(
                            e.getErrors().stream().map(MultiObjectDeleteException.DeleteError::getKey).collect(Collectors.toSet()));
                        logger.warn(
                            () -> new ParameterizedMessage("Failed to delete some blobs {}", e.getErrors()
                                .stream().map(err -> "[" + err.getKey() + "][" + err.getCode() + "][" + err.getMessage() + "]")
                                .collect(Collectors.toList())), e);
                        aex = ExceptionsHelper.useOrSuppress(aex, e);
                    } catch (AmazonClientException e) {
                        // The AWS client threw any unexpected exception and did not execute the request at all so we do not
                        // remove any keys from the outstanding deletes set.
                        aex = ExceptionsHelper.useOrSuppress(aex, e);
                    }
                }
                if (aex != null) {
                    throw aex;
                }
            });
        } catch (Exception e) {
            throw new IOException("Failed to delete blobs [" + outstanding + "]", e);
        }
        assert outstanding.isEmpty();
    }

    private static DeleteObjectsRequest bulkDelete(String bucket, List<String> blobs) {
        return new DeleteObjectsRequest(bucket).withKeys(blobs.toArray(Strings.EMPTY_ARRAY)).withQuiet(true);
    }

    @Override
    public Map<String, BlobMetaData> listBlobsByPrefix(@Nullable String blobNamePrefix) throws IOException {
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            return executeListing(clientReference, listObjectsRequest(blobNamePrefix == null ? keyPath : buildKey(blobNamePrefix)))
                .stream()
                .flatMap(listing -> listing.getObjectSummaries().stream())
                .map(summary -> new PlainBlobMetaData(summary.getKey().substring(keyPath.length()), summary.getSize()))
                .collect(Collectors.toMap(PlainBlobMetaData::name, Function.identity()));
        } catch (final AmazonClientException e) {
            throw new IOException("Exception when listing blobs by prefix [" + blobNamePrefix + "]", e);
        }
    }

    @Override
    public Map<String, BlobMetaData> listBlobs() throws IOException {
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

    private ListObjectsRequest listObjectsRequest(String keyPath) {
        return new ListObjectsRequest().withBucketName(blobStore.bucket()).withPrefix(keyPath).withDelimiter("/");
    }

    private String buildKey(String blobName) {
        return keyPath + blobName;
    }

    /**
     * Uploads a blob using a single upload request
     */
    void executeSingleUpload(final S3BlobStore blobStore,
                             final String blobName,
                             final InputStream input,
                             final long blobSize) throws IOException {

        // Extra safety checks
        if (blobSize > MAX_FILE_SIZE.getBytes()) {
            throw new IllegalArgumentException("Upload request size [" + blobSize + "] can't be larger than " + MAX_FILE_SIZE);
        }
        if (blobSize > blobStore.bufferSizeInBytes()) {
            throw new IllegalArgumentException("Upload request size [" + blobSize + "] can't be larger than buffer size");
        }

        final ObjectMetadata md = new ObjectMetadata();
        md.setContentLength(blobSize);
        if (blobStore.serverSideEncryption()) {
            md.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        }
        final PutObjectRequest putRequest = new PutObjectRequest(blobStore.bucket(), blobName, input, md);
        putRequest.setStorageClass(blobStore.getStorageClass());
        putRequest.setCannedAcl(blobStore.getCannedACL());

        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            SocketAccess.doPrivilegedVoid(() -> {
                clientReference.client().putObject(putRequest);
            });
        } catch (final AmazonClientException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using a single upload", e);
        }
    }

    /**
     * Uploads a blob using multipart upload requests.
     */
    void executeMultipartUpload(final S3BlobStore blobStore,
                                final String blobName,
                                final InputStream input,
                                final long blobSize) throws IOException {

        ensureMultiPartUploadSize(blobSize);
        final long partSize = blobStore.bufferSizeInBytes();
        final Tuple<Long, Long> multiparts = numberOfMultiparts(blobSize, partSize);

        if (multiparts.v1() > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Too many multipart upload requests, maybe try a larger buffer size?");
        }

        final int nbParts = multiparts.v1().intValue();
        final long lastPartSize = multiparts.v2();
        assert blobSize == (((nbParts - 1) * partSize) + lastPartSize) : "blobSize does not match multipart sizes";

        final SetOnce<String> uploadId = new SetOnce<>();
        final String bucketName = blobStore.bucket();
        boolean success = false;

        final InitiateMultipartUploadRequest initRequest = new InitiateMultipartUploadRequest(bucketName, blobName);
        initRequest.setStorageClass(blobStore.getStorageClass());
        initRequest.setCannedACL(blobStore.getCannedACL());
        if (blobStore.serverSideEncryption()) {
            final ObjectMetadata md = new ObjectMetadata();
            md.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
            initRequest.setObjectMetadata(md);
        }
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {

            uploadId.set(SocketAccess.doPrivileged(() -> clientReference.client().initiateMultipartUpload(initRequest).getUploadId()));
            if (Strings.isEmpty(uploadId.get())) {
                throw new IOException("Failed to initialize multipart upload " + blobName);
            }

            final List<PartETag> parts = new ArrayList<>();

            long bytesCount = 0;
            for (int i = 1; i <= nbParts; i++) {
                final UploadPartRequest uploadRequest = new UploadPartRequest();
                uploadRequest.setBucketName(bucketName);
                uploadRequest.setKey(blobName);
                uploadRequest.setUploadId(uploadId.get());
                uploadRequest.setPartNumber(i);
                uploadRequest.setInputStream(input);

                if (i < nbParts) {
                    uploadRequest.setPartSize(partSize);
                    uploadRequest.setLastPart(false);
                } else {
                    uploadRequest.setPartSize(lastPartSize);
                    uploadRequest.setLastPart(true);
                }
                bytesCount += uploadRequest.getPartSize();

                final UploadPartResult uploadResponse = SocketAccess.doPrivileged(() -> clientReference.client().uploadPart(uploadRequest));
                parts.add(uploadResponse.getPartETag());
            }

            if (bytesCount != blobSize) {
                throw new IOException("Failed to execute multipart upload for [" + blobName + "], expected " + blobSize
                    + "bytes sent but got " + bytesCount);
            }

            final CompleteMultipartUploadRequest complRequest = new CompleteMultipartUploadRequest(bucketName, blobName, uploadId.get(),
                    parts);
            SocketAccess.doPrivilegedVoid(() -> clientReference.client().completeMultipartUpload(complRequest));
            success = true;

        } catch (final AmazonClientException e) {
            throw new IOException("Unable to upload object [" + blobName + "] using multipart upload", e);
        } finally {
            if ((success == false) && Strings.hasLength(uploadId.get())) {
                final AbortMultipartUploadRequest abortRequest = new AbortMultipartUploadRequest(bucketName, blobName, uploadId.get());
                try (AmazonS3Reference clientReference = blobStore.clientReference()) {
                    SocketAccess.doPrivilegedVoid(() -> clientReference.client().abortMultipartUpload(abortRequest));
                }
            }
        }
    }

    // non-static, package private for testing
    void ensureMultiPartUploadSize(final long blobSize) {
        if (blobSize > MAX_FILE_SIZE_USING_MULTIPART.getBytes()) {
            throw new IllegalArgumentException("Multipart upload request size [" + blobSize
                + "] can't be larger than " + MAX_FILE_SIZE_USING_MULTIPART);
        }
        if (blobSize < MIN_PART_SIZE_USING_MULTIPART.getBytes()) {
            throw new IllegalArgumentException("Multipart upload request size [" + blobSize
                + "] can't be smaller than " + MIN_PART_SIZE_USING_MULTIPART);
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
}
