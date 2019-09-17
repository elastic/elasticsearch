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
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.Version;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;

/**
 * Wrapper around an S3 object that will retry the {@link GetObjectRequest} if the download fails part-way through, resuming from where
 * the failure occurred. This should be handled by the SDK but it isn't today. This should be revisited in the future (e.g. before removing
 * the {@link Version#V_7_0_0} version constant) and removed when the SDK handles retries itself.
 *
 * See https://github.com/aws/aws-sdk-java/issues/856 for the related SDK issue
 */
class S3RetryingInputStream extends InputStream {

    private static final Logger logger = LogManager.getLogger(S3RetryingInputStream.class);

    static final int MAX_SUPPRESSED_EXCEPTIONS = 10;

    private final S3BlobStore blobStore;
    private final String blobKey;
    private final int maxAttempts;

    private InputStream currentStream;
    private int attempt = 1;
    private List<IOException> failures = new ArrayList<>(MAX_SUPPRESSED_EXCEPTIONS);
    private long currentOffset;
    private boolean closed;

    S3RetryingInputStream(S3BlobStore blobStore, String blobKey) throws IOException {
        this.blobStore = blobStore;
        this.blobKey = blobKey;
        this.maxAttempts = blobStore.getMaxRetries() + 1;
        currentStream = openStream();
    }

    private InputStream openStream() throws IOException {
        try (AmazonS3Reference clientReference = blobStore.clientReference()) {
            final GetObjectRequest getObjectRequest = new GetObjectRequest(blobStore.bucket(), blobKey);
            if (currentOffset > 0) {
                getObjectRequest.setRange(currentOffset);
            }
            final S3Object s3Object = SocketAccess.doPrivileged(() -> clientReference.client().getObject(getObjectRequest));
            return s3Object.getObjectContent();
        } catch (final AmazonClientException e) {
            if (e instanceof AmazonS3Exception) {
                if (404 == ((AmazonS3Exception) e).getStatusCode()) {
                    throw addSuppressedExceptions(new NoSuchFileException("Blob object [" + blobKey + "] not found: " + e.getMessage()));
                }
            }
            throw addSuppressedExceptions(e);
        }
    }

    @Override
    public int read() throws IOException {
        ensureOpen();
        while (true) {
            try {
                final int result = currentStream.read();
                currentOffset += 1;
                return result;
            } catch (IOException e) {
                reopenStreamOrFail(e);
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        ensureOpen();
        while (true) {
            try {
                final int bytesRead = currentStream.read(b, off, len);
                if (bytesRead == -1) {
                    return -1;
                }
                currentOffset += bytesRead;
                return bytesRead;
            } catch (IOException e) {
                reopenStreamOrFail(e);
            }
        }
    }

    private void ensureOpen() {
        if (closed) {
            assert false : "using S3RetryingInputStream after close";
            throw new IllegalStateException("using S3RetryingInputStream after close");
        }
    }

    private void reopenStreamOrFail(IOException e) throws IOException {
        if (attempt >= maxAttempts) {
            throw addSuppressedExceptions(e);
        }
        logger.debug(new ParameterizedMessage("failed reading [{}/{}] at offset [{}], attempt [{}] of [{}], retrying",
            blobStore.bucket(), blobKey, currentOffset, attempt, maxAttempts), e);
        attempt += 1;
        if (failures.size() < MAX_SUPPRESSED_EXCEPTIONS) {
            failures.add(e);
        }
        IOUtils.closeWhileHandlingException(currentStream);
        currentStream = openStream();
    }

    @Override
    public void close() throws IOException {
        currentStream.close();
        closed = true;
    }

    @Override
    public long skip(long n) {
        throw new UnsupportedOperationException("S3RetryingInputStream does not support seeking");
    }

    @Override
    public void reset() {
        throw new UnsupportedOperationException("S3RetryingInputStream does not support seeking");
    }

    private <T extends Exception> T addSuppressedExceptions(T e) {
        for (IOException failure : failures) {
            e.addSuppressed(failure);
        }
        return e;
    }
}
