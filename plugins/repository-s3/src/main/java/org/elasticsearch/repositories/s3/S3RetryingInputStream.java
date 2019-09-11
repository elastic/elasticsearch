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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;

class S3RetryingInputStream extends InputStream {

    private static final Logger logger = LogManager.getLogger(S3RetryingInputStream.class);

    private final S3BlobStore blobStore;
    private final String blobKey;
    private final int maxAttempts;

    private InputStream currentStream;
    private long currentOffset;

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
                    throw new NoSuchFileException("Blob object [" + blobKey + "] not found: " + e.getMessage());
                }
            }
            throw e;
        }
    }

    @Override
    public int read() throws IOException {
        int attempt = 0;
        while (true) {
            attempt += 1;
            try {
                final int result = currentStream.read();
                currentOffset += 1;
                return result;
            } catch (IOException e) {
                if (attempt >= maxAttempts) {
                    throw e;
                }
                logger.debug(new ParameterizedMessage("failed reading [{}/{}] at offset [{}], attempt [{}] of [{}], retrying",
                    blobStore.bucket(), blobKey, currentOffset, attempt, maxAttempts), e);
                currentStream.close();
                currentStream = openStream();
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int attempt = 0;
        while (true) {
            attempt += 1;
            try {
                final int bytesRead = currentStream.read(b, off, len);
                if (bytesRead == -1) {
                    return -1;
                }
                currentOffset += bytesRead;
                return bytesRead;
            } catch (IOException e) {
                if (attempt >= maxAttempts) {
                    throw e;
                }
                logger.debug(new ParameterizedMessage("failed reading [{}/{}] at offset [{}], attempt [{}] of [{}], retrying",
                    blobStore.bucket(), blobKey, currentOffset, attempt, maxAttempts), e);
                currentStream.close();
                currentStream = openStream();
            }
        }
    }

    @Override
    public void close() throws IOException {
        currentStream.close();
    }

    @Override
    public long skip(long n) {
        throw new UnsupportedOperationException("S3RetryingInputStream does not support seeking");
    }

    @Override
    public synchronized void reset() {
        throw new UnsupportedOperationException("S3RetryingInputStream does not support seeking");
    }
}
