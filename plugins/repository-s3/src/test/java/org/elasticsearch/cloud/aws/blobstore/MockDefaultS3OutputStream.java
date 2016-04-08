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

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.PartETag;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.elasticsearch.common.io.Streams;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class MockDefaultS3OutputStream extends DefaultS3OutputStream {

    private ByteArrayOutputStream out = new ByteArrayOutputStream();

    private boolean initialized = false;
    private boolean completed = false;
    private boolean aborted = false;

    private int numberOfUploadRequests = 0;

    public MockDefaultS3OutputStream(int bufferSizeInBytes) {
        super(null, "test-bucket", "test-blobname", bufferSizeInBytes, 3, false);
    }

    @Override
    protected void doUpload(S3BlobStore blobStore, String bucketName, String blobName, InputStream is, int length, boolean serverSideEncryption) throws AmazonS3Exception {
        try {
            long copied = Streams.copy(is, out);
            if (copied != length) {
                throw new AmazonS3Exception("Not all the bytes were copied");
            }
            numberOfUploadRequests++;
        } catch (IOException e) {
            throw new AmazonS3Exception(e.getMessage());
        }
    }

    @Override
    protected String doInitialize(S3BlobStore blobStore, String bucketName, String blobName, boolean serverSideEncryption) {
        initialized = true;
        return RandomizedTest.randomAsciiOfLength(50);
    }

    @Override
    protected PartETag doUploadMultipart(S3BlobStore blobStore, String bucketName, String blobName, String uploadId, InputStream is, int length, boolean lastPart) throws AmazonS3Exception {
        try {
            long copied = Streams.copy(is, out);
            if (copied != length) {
                throw new AmazonS3Exception("Not all the bytes were copied");
            }
            return new PartETag(numberOfUploadRequests++, RandomizedTest.randomAsciiOfLength(50));
        } catch (IOException e) {
            throw new AmazonS3Exception(e.getMessage());
        }
    }

    @Override
    protected void doCompleteMultipart(S3BlobStore blobStore, String bucketName, String blobName, String uploadId, List<PartETag> parts) throws AmazonS3Exception {
        completed = true;
    }

    @Override
    protected void doAbortMultipart(S3BlobStore blobStore, String bucketName, String blobName, String uploadId) throws AmazonS3Exception {
        aborted = true;
    }

    public int getNumberOfUploadRequests() {
        return numberOfUploadRequests;
    }

    public boolean isMultipart() {
        return (numberOfUploadRequests > 1) && initialized && completed && !aborted;
    }

    public byte[] toByteArray() {
        return out.toByteArray();
    }
}
