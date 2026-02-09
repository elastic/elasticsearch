/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package org.elasticsearch.xpack.stateless;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.MultipartUpload;
import com.amazonaws.services.s3.model.MultipartUploadListing;
import com.amazonaws.services.s3.model.PartETag;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.concurrent.Future;

class S3Register {
    private final Logger logger = LogManager.getLogger(S3Register.class);
    private final String bucket;
    private final String key;
    private final AmazonS3 client;
    private final Random random;
    private final ThreadPool threadPool;

    S3Register(AmazonS3 client, String bucket, String key, Random random, ThreadPool threadPool) {
        this.bucket = bucket;
        this.key = key;
        this.client = client;
        this.random = random;
        this.threadPool = threadPool;
    }

    CASStatus performOperation(CASOperation op) {
        try {
            var proposedValue = op.newValue();
            var expectedValue = op.expectedValue();
            var concurrentUpdates = getConcurrentUpdates();

            if (concurrentUpdates.isEmpty()) {
                var uploadId = startMultipart(key);

                var uploadPartResult = uploadPart(uploadId, Integer.toString(proposedValue));
                var partETags = List.of(uploadPartResult.getPartETag());

                var concurrentUpdatesBeforeCommit = getConcurrentUpdates().stream().map(MultipartUpload::getUploadId).sorted().toList();

                // This is a small optimization to improve the liveness properties of this
                // algorithm.

                // When there are multiple competing updates, we order them by upload id and
                // the first one tries to cancel the competing updates in order to make progress.
                // To avoid liveness issues when the winner fails, the rest wait based on their
                // upload_id based position and try to make progress.
                int position = Collections.binarySearch(concurrentUpdatesBeforeCommit, uploadId);
                if (position >= 0) {
                    sleepUninterruptibly(TimeValue.timeValueSeconds(position).millis() + RandomNumbers.randomIntBetween(random, 0, 50));
                    tryToCancelConcurrentUploads(uploadId, concurrentUpdatesBeforeCommit);
                } else {
                    return CASStatus.FAILED;
                }

                var valueBeforeCommit = getCurrentValue();

                // We've a stale version, we should retry later on
                if (Objects.equals(expectedValue, valueBeforeCommit) == false) {
                    try {
                        abortMultipartUpload(uploadId);
                    } catch (Exception e) {
                        logger.warn("Unable to abort multi part upload [{}]", uploadId, e);
                    }
                    return CASStatus.FAILED;
                }

                finishUpload(uploadId, partETags);
                return CASStatus.SUCCESSFUL;
            }

            return CASStatus.FAILED;
        } catch (AmazonS3Exception e) {
            return e.getErrorCode().equals("NoSuchUpload") ? CASStatus.FAILED : CASStatus.UNKNOWN;
        } catch (Exception e) {
            return CASStatus.UNKNOWN;
        }
    }

    private void tryToCancelConcurrentUploads(String uploadId, List<String> concurrentUpdates) {
        final List<Future<?>> cancellationFutures = new ArrayList<>();
        for (var concurrentUpdate : concurrentUpdates) {
            if (concurrentUpdate.equals(uploadId)) {
                continue;
            }
            cancellationFutures.add(abortMultipartUpload(concurrentUpdate));
        }

        cancellationFutures.forEach(FutureUtils::get);
    }

    private List<MultipartUpload> getConcurrentUpdates() {
        final var listRequest = new ListMultipartUploadsRequest(bucket);
        listRequest.setPrefix(key);
        MultipartUploadListing multipartUploadListing = client.listMultipartUploads(listRequest);
        return multipartUploadListing.getMultipartUploads();
    }

    private UploadPartResult uploadPart(String uploadId, String content) {
        byte[] data = content.getBytes(StandardCharsets.UTF_8);
        var uploadPartRequest = new UploadPartRequest();
        uploadPartRequest.setBucketName(bucket);
        uploadPartRequest.setKey(key);
        uploadPartRequest.setUploadId(uploadId);
        uploadPartRequest.setPartNumber(1);
        uploadPartRequest.setLastPart(true);
        uploadPartRequest.setInputStream(new ByteArrayInputStream(data));
        uploadPartRequest.setPartSize(data.length);
        return client.uploadPart(uploadPartRequest);
    }

    @Nullable
    Integer getCurrentValue() throws IOException {
        try {
            var response = Streams.readFully(client.getObject(bucket, key).getObjectContent());
            return Integer.parseInt(response.utf8ToString());
        } catch (AmazonS3Exception e) {
            if (e.getErrorCode().equals("NoSuchKey")) {
                return null;
            }

            throw e;
        }
    }

    private String startMultipart(String key) {
        var request = new InitiateMultipartUploadRequest(bucket, key);
        return client.initiateMultipartUpload(request).getUploadId();
    }

    private Future<?> abortMultipartUpload(String uploadId) {
        return threadPool.executor(ThreadPool.Names.GENERIC).submit(() -> {
            var abortUploadRequest = new AbortMultipartUploadRequest(bucket, key, uploadId);
            client.abortMultipartUpload(abortUploadRequest);
        });
    }

    private void finishUpload(String uploadId, List<PartETag> partETags) {
        var request = new CompleteMultipartUploadRequest();
        request.setBucketName(bucket);
        request.setUploadId(uploadId);
        request.setKey(key);
        request.setPartETags(partETags);
        client.completeMultipartUpload(request);
    }

    private void sleepUninterruptibly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    enum CASStatus {
        SUCCESSFUL,
        UNKNOWN,
        FAILED
    }

    record CASOperation(@Nullable Integer expectedValue, int newValue) {}
}
