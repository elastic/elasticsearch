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
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsResult;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.Streams;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

class MockAmazonS3 extends AbstractAmazonS3 {

    private final ConcurrentMap<String, byte[]> blobs;
    private final String bucket;
    private final boolean serverSideEncryption;
    private final String cannedACL;
    private final String storageClass;

    MockAmazonS3(final ConcurrentMap<String, byte[]> blobs,
                 final String bucket,
                 final boolean serverSideEncryption,
                 final String cannedACL,
                 final String storageClass) {
        this.blobs = Objects.requireNonNull(blobs);
        this.bucket = Objects.requireNonNull(bucket);
        this.serverSideEncryption = serverSideEncryption;
        this.cannedACL = cannedACL;
        this.storageClass = storageClass;
    }

    @Override
    public boolean doesObjectExist(final String bucketName, final String objectName) throws SdkClientException {
        assertThat(bucketName, equalTo(bucket));
        return blobs.containsKey(objectName);
    }

    @Override
    public PutObjectResult putObject(final PutObjectRequest request) throws AmazonClientException {
        assertThat(request.getBucketName(), equalTo(bucket));
        assertThat(request.getMetadata().getSSEAlgorithm(), serverSideEncryption ? equalTo("AES256") : nullValue());
        assertThat(request.getCannedAcl(), notNullValue());
        assertThat(request.getCannedAcl().toString(), cannedACL != null ? equalTo(cannedACL) : equalTo("private"));
        assertThat(request.getStorageClass(), storageClass != null ? equalTo(storageClass) : equalTo("STANDARD"));


        final String blobName = request.getKey();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            Streams.copy(request.getInputStream(), out);
            blobs.put(blobName, out.toByteArray());
        } catch (IOException e) {
            throw new AmazonClientException(e);
        }
        return new PutObjectResult();
    }

    @Override
    public S3Object getObject(final GetObjectRequest request) throws AmazonClientException {
        assertThat(request.getBucketName(), equalTo(bucket));

        final String blobName = request.getKey();
        final byte[] content = blobs.get(blobName);
        if (content == null) {
            AmazonS3Exception exception = new AmazonS3Exception("[" + blobName + "] does not exist.");
            exception.setStatusCode(404);
            throw exception;
        }

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(content.length);

        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(new S3ObjectInputStream(new ByteArrayInputStream(content), null, false));
        s3Object.setKey(blobName);
        s3Object.setObjectMetadata(metadata);

        return s3Object;
    }

    @Override
    public ObjectListing listObjects(final ListObjectsRequest request) throws AmazonClientException {
        assertThat(request.getBucketName(), equalTo(bucket));

        final ObjectListing listing = new ObjectListing();
        listing.setBucketName(request.getBucketName());
        listing.setPrefix(request.getPrefix());

        for (Map.Entry<String, byte[]> blob : blobs.entrySet()) {
            if (Strings.isEmpty(request.getPrefix()) || blob.getKey().startsWith(request.getPrefix())) {
                S3ObjectSummary summary = new S3ObjectSummary();
                summary.setBucketName(request.getBucketName());
                summary.setKey(blob.getKey());
                summary.setSize(blob.getValue().length);
                listing.getObjectSummaries().add(summary);
            }
        }
        return listing;
    }

    @Override
    public void deleteObject(final DeleteObjectRequest request) throws AmazonClientException {
        assertThat(request.getBucketName(), equalTo(bucket));
        blobs.remove(request.getKey());
    }

    @Override
    public void shutdown() {
        // TODO check close
    }

    @Override
    public DeleteObjectsResult deleteObjects(DeleteObjectsRequest request) throws SdkClientException {
        assertThat(request.getBucketName(), equalTo(bucket));

        final List<DeleteObjectsResult.DeletedObject> deletions = new ArrayList<>();
        for (DeleteObjectsRequest.KeyVersion key : request.getKeys()) {
            if (blobs.remove(key.getKey()) != null) {
                DeleteObjectsResult.DeletedObject deletion = new DeleteObjectsResult.DeletedObject();
                deletion.setKey(key.getKey());
                deletions.add(deletion);
            }
        }
        return new DeleteObjectsResult(deletions);
    }
}
