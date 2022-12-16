package co.elastic.elasticsearch.stateless;

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
import org.elasticsearch.core.Nullable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;

class S3Register {
    private final Logger logger = LogManager.getLogger(S3Register.class);
    private final String bucket;
    private final String key;
    private final AmazonS3 client;
    private final Random random;

    S3Register(AmazonS3 client, String bucket, String key, Random random) {
        this.bucket = bucket;
        this.key = key;
        this.client = client;
        this.random = random;
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

                var concurrentUploadsBeforeCommit = getConcurrentUpdatesExcluding(uploadId);

                if (concurrentUploadsBeforeCommit.isEmpty() == false) {
                    tryToCancelConcurrentUploads(uploadId);
                }

                var valueBeforeCommit = getCurrentValue();

                // We've a stale version, we should retry later on
                if (expectedValue.equals(valueBeforeCommit) == false) {
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

    private void tryToCancelConcurrentUploads(String uploadId) {
        sleepUninterruptibly(RandomNumbers.randomIntBetween(random, 0, 200));

        for (var concurrentUpdate : getConcurrentUpdatesExcluding(uploadId)) {
            abortMultipartUpload(concurrentUpdate.getUploadId());
        }
    }

    private List<MultipartUpload> getConcurrentUpdates() {
        final var listRequest = new ListMultipartUploadsRequest(bucket);
        listRequest.setPrefix(key);
        MultipartUploadListing multipartUploadListing = client.listMultipartUploads(listRequest);
        return multipartUploadListing.getMultipartUploads();
    }

    private List<MultipartUpload> getConcurrentUpdatesExcluding(String uploadId) {
        return getConcurrentUpdates().stream().filter(multipartUpload -> multipartUpload.getUploadId().equals(uploadId) == false).toList();
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

    private void abortMultipartUpload(String uploadId) {
        var abortUploadRequest = new AbortMultipartUploadRequest(bucket, key, uploadId);
        client.abortMultipartUpload(abortUploadRequest);
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
