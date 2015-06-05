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

package org.elasticsearch.cloud.aws.blobstore;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.util.Base64;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;

/**
 * DefaultS3OutputStream uploads data to the AWS S3 service using 2 modes: single and multi part.
 * <p/>
 * When the length of the chunk is lower than buffer_size, the chunk is uploaded with a single request.
 * Otherwise multiple requests are made, each of buffer_size (except the last one which can be lower than buffer_size).
 * <p/>
 * Quick facts about S3:
 * <p/>
 * Maximum object size:                 5 TB
 * Maximum number of parts per upload:  10,000
 * Part numbers:                        1 to 10,000 (inclusive)
 * Part size:                           5 MB to 5 GB, last part can be < 5 MB
 * <p/>
 * See http://docs.aws.amazon.com/AmazonS3/latest/dev/qfacts.html
 * See http://docs.aws.amazon.com/AmazonS3/latest/dev/uploadobjusingmpu.html
 */
public class DefaultS3OutputStream extends S3OutputStream {

    private static final ByteSizeValue MULTIPART_MAX_SIZE = new ByteSizeValue(5, ByteSizeUnit.GB);
    private static final ESLogger logger = Loggers.getLogger("cloud.aws");
    /**
     * Multipart Upload API data
     */
    private String multipartId;
    private int multipartChunks;
    private List<PartETag> multiparts;

    public DefaultS3OutputStream(S3BlobStore blobStore, String bucketName, String blobName, int bufferSizeInBytes, int numberOfRetries, boolean serverSideEncryption) {
        super(blobStore, bucketName, blobName, bufferSizeInBytes, numberOfRetries, serverSideEncryption);
    }

    @Override
    public void flush(byte[] bytes, int off, int len, boolean closing) throws IOException {
        if (len > MULTIPART_MAX_SIZE.getBytes()) {
            throw new IOException("Unable to upload files larger than " + MULTIPART_MAX_SIZE + " to Amazon S3");
        }

        if (!closing) {
            if (len < getBufferSize()) {
                upload(bytes, off, len);
            } else {
                if (getFlushCount() == 0) {
                    initializeMultipart();
                }
                uploadMultipart(bytes, off, len, false);
            }
        } else {
            if (multipartId != null) {
                uploadMultipart(bytes, off, len, true);
                completeMultipart();
            } else {
                upload(bytes, off, len);
            }
        }
    }

    /**
     * Upload data using a single request.
     *
     * @param bytes
     * @param off
     * @param len
     * @throws IOException
     */
    private void upload(byte[] bytes, int off, int len) throws IOException {
        try (ByteArrayInputStream is = new ByteArrayInputStream(bytes, off, len)) {
            int retry = 0;
            while (retry <= getNumberOfRetries()) {
                try {
                    doUpload(getBlobStore(), getBucketName(), getBlobName(), is, len, isServerSideEncryption());
                    break;
                } catch (AmazonClientException e) {
                    if (getBlobStore().shouldRetry(e) && retry < getNumberOfRetries()) {
                        is.reset();
                        retry++;
                    } else {
                        throw new IOException("Unable to upload object " + getBlobName(), e);
                    }
                }
            }
        }
    }

    protected void doUpload(S3BlobStore blobStore, String bucketName, String blobName, InputStream is, int length,
            boolean serverSideEncryption) throws AmazonS3Exception {
        ObjectMetadata md = new ObjectMetadata();
        if (serverSideEncryption) {
            md.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
        }
        md.setContentLength(length);

        InputStream inputStream = is;

        // We try to compute a MD5 while reading it
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("MD5");
            inputStream = new DigestInputStream(is, messageDigest);
        } catch (NoSuchAlgorithmException impossible) {
            // Every implementation of the Java platform is required to support MD5 (see MessageDigest)
            throw new RuntimeException(impossible);
        }
        PutObjectResult putObjectResult = blobStore.client().putObject(bucketName, blobName, inputStream, md);

        String localMd5 = Base64.encodeAsString(messageDigest.digest());
        String remoteMd5 = putObjectResult.getContentMd5();
        if (!localMd5.equals(remoteMd5)) {
            logger.debug("MD5 local [{}], remote [{}] are not equal...", localMd5, remoteMd5);
            throw new AmazonS3Exception("MD5 local [" + localMd5 +
                    "], remote [" + remoteMd5 +
                    "] are not equal...");
        }
    }

    private void initializeMultipart() {
        int retry = 0;
        while ((retry <= getNumberOfRetries()) && (multipartId == null)) {
            try {
                multipartId = doInitialize(getBlobStore(), getBucketName(), getBlobName(), isServerSideEncryption());
                if (multipartId != null) {
                    multipartChunks = 1;
                    multiparts = new ArrayList<>();
                }
            } catch (AmazonClientException e) {
                if (getBlobStore().shouldRetry(e) && retry < getNumberOfRetries()) {
                    retry++;
                } else {
                    throw e;
                }
            }
        }
    }

    protected String doInitialize(S3BlobStore blobStore, String bucketName, String blobName, boolean serverSideEncryption) {
        InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(bucketName, blobName);
        if (serverSideEncryption) {
            ObjectMetadata md = new ObjectMetadata();
            md.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
            request.setObjectMetadata(md);
        }
        return blobStore.client().initiateMultipartUpload(request).getUploadId();
    }

    private void uploadMultipart(byte[] bytes, int off, int len, boolean lastPart) throws IOException {
        try (ByteArrayInputStream is = new ByteArrayInputStream(bytes, off, len)) {
            int retry = 0;
            while (retry <= getNumberOfRetries()) {
                try {
                    PartETag partETag = doUploadMultipart(getBlobStore(), getBucketName(), getBlobName(), multipartId, is, len, lastPart);
                    multiparts.add(partETag);
                    multipartChunks++;
                    return;
                } catch (AmazonClientException e) {
                    if (getBlobStore().shouldRetry(e) && retry < getNumberOfRetries()) {
                        is.reset();
                        retry++;
                    } else {
                        abortMultipart();
                        throw e;
                    }
                }
            }
        }
    }

    protected PartETag doUploadMultipart(S3BlobStore blobStore, String bucketName, String blobName, String uploadId, InputStream is,
            int length, boolean lastPart) throws AmazonS3Exception {
        UploadPartRequest request = new UploadPartRequest()
        .withBucketName(bucketName)
        .withKey(blobName)
        .withUploadId(uploadId)
        .withPartNumber(multipartChunks)
        .withInputStream(is)
        .withPartSize(length)
        .withLastPart(lastPart);

        UploadPartResult response = blobStore.client().uploadPart(request);
        return response.getPartETag();

    }

    private void completeMultipart() {
        int retry = 0;
        while (retry <= getNumberOfRetries()) {
            try {
                doCompleteMultipart(getBlobStore(), getBucketName(), getBlobName(), multipartId, multiparts);
                multipartId = null;
                return;
            } catch (AmazonClientException e) {
                if (getBlobStore().shouldRetry(e) && retry < getNumberOfRetries()) {
                    retry++;
                } else {
                    abortMultipart();
                    throw e;
                }
            }
        }
    }

    protected void doCompleteMultipart(S3BlobStore blobStore, String bucketName, String blobName, String uploadId, List<PartETag> parts)
            throws AmazonS3Exception {
        CompleteMultipartUploadRequest request = new CompleteMultipartUploadRequest(bucketName, blobName, uploadId, parts);
        blobStore.client().completeMultipartUpload(request);
    }

    private void abortMultipart() {
        if (multipartId != null) {
            try {
                doAbortMultipart(getBlobStore(), getBucketName(), getBlobName(), multipartId);
            } finally {
                multipartId = null;
            }
        }
    }

    protected void doAbortMultipart(S3BlobStore blobStore, String bucketName, String blobName, String uploadId)
            throws AmazonS3Exception {
        blobStore.client().abortMultipartUpload(new AbortMultipartUploadRequest(bucketName, blobName, uploadId));
    }
}
