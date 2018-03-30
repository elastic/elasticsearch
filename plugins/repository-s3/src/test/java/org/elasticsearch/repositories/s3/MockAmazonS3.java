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

import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.elasticsearch.common.io.Streams;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MockAmazonS3 extends AbstractAmazonS3 {

    static final Map<String, Map<String, byte[]>> BUCKETS = new ConcurrentHashMap<>();

    private MockAmazonS3() {
    }

    public static AmazonS3 createClient(final String bucket,
                                        final boolean serverSideEncryption,
                                        final String cannedACL,
                                        final String storageClass) {

        final AmazonS3 mockedClient = mock(AmazonS3.class);

        // Create or erase the bucket
        BUCKETS.put(bucket, new ConcurrentHashMap<>());

        // Bucket exists?
        when(mockedClient.doesBucketExist(any(String.class))).thenAnswer(invocation -> {
            String bucketName = (String) invocation.getArguments()[0];
            assertThat(bucketName, equalTo(bucket));
            return BUCKETS.containsKey(bucketName);
        });

        // Blob exists?
        when(mockedClient.doesObjectExist(any(String.class), any(String.class))).thenAnswer(invocation -> {
            String bucketName = (String) invocation.getArguments()[0];
            String objectName = (String) invocation.getArguments()[1];
            assertThat(bucketName, equalTo(bucket));
            return BUCKETS.getOrDefault(bucketName, emptyMap()).containsKey(objectName);
        });

        // Write blob
        when(mockedClient.putObject(any(PutObjectRequest.class))).thenAnswer(invocation -> {
            PutObjectRequest request = (PutObjectRequest) invocation.getArguments()[0];
            assertThat(request.getBucketName(), equalTo(bucket));
            assertThat(request.getMetadata().getSSEAlgorithm(), serverSideEncryption ? equalTo("AES256") : nullValue());
            assertThat(request.getCannedAcl(), notNullValue());
            assertThat(request.getCannedAcl().toString(), cannedACL != null ? equalTo(cannedACL) : equalTo("private"));
            assertThat(request.getStorageClass(), storageClass != null ? equalTo(storageClass) : equalTo("STANDARD"));

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            Streams.copy(request.getInputStream(), out);
            assertThat((long) out.size(), equalTo(request.getMetadata().getContentLength()));

            BUCKETS.computeIfAbsent(request.getBucketName(), s -> new ConcurrentHashMap<>()).put(request.getKey(), out.toByteArray());
            return null;
        });

        // Read blob
        when(mockedClient.getObject(any(String.class), any(String.class))).thenAnswer(invocation -> {
            String bucketName = (String) invocation.getArguments()[0];
            String objectName = (String) invocation.getArguments()[1];
            assertThat(bucketName, equalTo(bucket));

            byte[] blob = BUCKETS.getOrDefault(bucketName, emptyMap()).get(objectName);
            if(blob == null){
                AmazonS3Exception exception = new AmazonS3Exception("Blob not found");
                exception.setStatusCode(404);
                throw exception;
            }

            S3Object response = new S3Object();
            response.setObjectContent(new S3ObjectInputStream(new ByteArrayInputStream(blob), null, false));
            return response;
        });

        // Copy blob
        when(mockedClient.copyObject(any(CopyObjectRequest.class))).thenAnswer(invocation -> {
            CopyObjectRequest request = (CopyObjectRequest) invocation.getArguments()[0];
            assertThat(request.getSourceBucketName(), equalTo(bucket));
            assertThat(request.getDestinationBucketName(), equalTo(bucket));

            Map<String, byte[]> blobsInBucket = BUCKETS.getOrDefault(bucket, emptyMap());
            byte[] blob = blobsInBucket.get(request.getSourceKey());
            if(blob != null) {
                blobsInBucket.put(request.getDestinationKey(), blob);
            } else {
                AmazonS3Exception exception = new AmazonS3Exception("Blob not found");
                exception.setStatusCode(404);
                throw exception;
            }
            return null;
        });

        // List BUCKETS
        when(mockedClient.listObjects(any(String.class), any(String.class))).thenAnswer(invocation -> {
            String bucketName = (String) invocation.getArguments()[0];
            String prefix = (String) invocation.getArguments()[1];

            assertThat(bucketName, equalTo(bucket));
            ObjectListing listing = new ObjectListing();
            listing.setBucketName(bucketName);
            listing.setPrefix(prefix);
            for (Map.Entry<String, byte[]> blob : BUCKETS.getOrDefault(bucketName, emptyMap()).entrySet()) {
                if (blob.getKey().startsWith(prefix)) {
                    S3ObjectSummary summary = new S3ObjectSummary();
                    summary.setBucketName(bucketName);
                    summary.setKey(blob.getKey());
                    summary.setSize(blob.getValue().length);
                    listing.getObjectSummaries().add(summary);
                }
            }
            return listing;
        });

        // List next batch of BUCKETS
        when(mockedClient.listNextBatchOfObjects(any(ObjectListing.class))).thenAnswer(invocation -> {
            ObjectListing objectListing = (ObjectListing) invocation.getArguments()[0];
            assertThat(objectListing.getBucketName(), equalTo(bucket));
            return new ObjectListing();
        });

        // Delete blob
        doAnswer(invocation -> {
            String bucketName = (String) invocation.getArguments()[0];
            String objectName = (String) invocation.getArguments()[1];
            assertThat(bucketName, equalTo(bucket));

            Map<String, byte[]> blobsInBucket = BUCKETS.getOrDefault(bucketName, emptyMap());
            if(blobsInBucket.remove(objectName) == null){
                AmazonS3Exception exception = new AmazonS3Exception("Blob not found");
                exception.setStatusCode(404);
                throw exception;
            }
            return null;
        }).when(mockedClient).deleteObject(any(String.class), any(String.class));

        // Delete multiple BUCKETS
        doAnswer(invocation -> {
            DeleteObjectsRequest deleteObjectsRequest = (DeleteObjectsRequest) invocation.getArguments()[0];
            assertThat(deleteObjectsRequest.getBucketName(), equalTo(bucket));

            Map<String, byte[]> blobsInBucket = BUCKETS.getOrDefault(deleteObjectsRequest.getBucketName(), emptyMap());
            for (DeleteObjectsRequest.KeyVersion key : deleteObjectsRequest.getKeys()) {
                if(blobsInBucket.remove(key.getKey()) == null){
                    AmazonS3Exception exception = new AmazonS3Exception("Blob not found");
                    exception.setStatusCode(404);
                    throw exception;
                }
            }
            return null;
        }).when(mockedClient).deleteObjects(any(DeleteObjectsRequest.class));

        return mockedClient;
    }
}
