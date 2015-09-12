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

import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.io.OutputStream;

/**
 * S3OutputStream buffers data before flushing it to an underlying S3OutputStream.
 */
public abstract class S3OutputStream extends OutputStream {

    /**
     * Limit of upload allowed by AWS S3.
     */
    protected static final ByteSizeValue MULTIPART_MAX_SIZE = new ByteSizeValue(5, ByteSizeUnit.GB);
    protected static final ByteSizeValue MULTIPART_MIN_SIZE = new ByteSizeValue(5, ByteSizeUnit.MB);

    private S3BlobStore blobStore;
    private String bucketName;
    private String blobName;
    private int numberOfRetries;
    private boolean serverSideEncryption;

    private byte[] buffer;
    private int count;
    private long length;

    private int flushCount = 0;

    public S3OutputStream(S3BlobStore blobStore, String bucketName, String blobName, int bufferSizeInBytes, int numberOfRetries, boolean serverSideEncryption) {
        this.blobStore = blobStore;
        this.bucketName = bucketName;
        this.blobName = blobName;
        this.numberOfRetries = numberOfRetries;
        this.serverSideEncryption = serverSideEncryption;

        if (bufferSizeInBytes < MULTIPART_MIN_SIZE.getBytes()) {
            throw new IllegalArgumentException("Buffer size can't be smaller than " + MULTIPART_MIN_SIZE);
        }
        if (bufferSizeInBytes > MULTIPART_MAX_SIZE.getBytes()) {
            throw new IllegalArgumentException("Buffer size can't be larger than " + MULTIPART_MAX_SIZE);
        }

        this.buffer = new byte[bufferSizeInBytes];
    }

    public abstract void flush(byte[] bytes, int off, int len, boolean closing) throws IOException;

    private void flushBuffer(boolean closing) throws IOException {
        flush(buffer, 0, count, closing);
        flushCount++;
        count = 0;
    }

    @Override
    public void write(int b) throws IOException {
        if (count >= buffer.length) {
            flushBuffer(false);
        }

        buffer[count++] = (byte) b;
        length++;
    }

    @Override
    public void close() throws IOException {
        if (count > 0) {
            flushBuffer(true);
            count = 0;
        }
    }

    public S3BlobStore getBlobStore() {
        return blobStore;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getBlobName() {
        return blobName;
    }

    public int getBufferSize() {
        return buffer.length;
    }

    public int getNumberOfRetries() {
        return numberOfRetries;
    }

    public boolean isServerSideEncryption() {
        return serverSideEncryption;
    }

    public long getLength() {
        return length;
    }

    public int getFlushCount() {
        return flushCount;
    }
}
