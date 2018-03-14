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
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.AbstractAmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CopyObjectRequest;
import com.amazonaws.services.s3.model.CopyObjectResult;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectMetadataRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.assertTrue;

class MockAmazonS3 extends AbstractAmazonS3 {

    private final int mockSocketPort;

    private Map<String, InputStream> blobs = new ConcurrentHashMap<>();

    // in ESBlobStoreContainerTestCase.java, the maximum
    // length of the input data is 100 bytes
    private byte[] byteCounter = new byte[100];


    MockAmazonS3(int mockSocketPort) {
        this.mockSocketPort = mockSocketPort;
    }

    // Simulate a socket connection to check that SocketAccess.doPrivileged() is used correctly.
    // Any method of AmazonS3 might potentially open a socket to the S3 service. Firstly, a call
    // to any method of AmazonS3 has to be wrapped by SocketAccess.doPrivileged().
    // Secondly, each method on the stack from doPrivileged to opening the socket has to be
    // located in a jar that is provided by the plugin.
    // Thirdly, a SocketPermission has to be configured in plugin-security.policy.
    // By opening a socket in each method of MockAmazonS3 it is ensured that in production AmazonS3
    // is able to to open a socket to the S3 Service without causing a SecurityException
    private void simulateS3SocketConnection() {
        try (Socket socket = new Socket(InetAddress.getByName("127.0.0.1"), mockSocketPort)) {
            assertTrue(socket.isConnected()); // NOOP to keep static analysis happy
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }


    @Override
    public boolean doesBucketExist(String bucket) {
        return true;
    }

    @Override
    public boolean doesObjectExist(String bucketName, String objectName) throws AmazonServiceException, SdkClientException {
        simulateS3SocketConnection();
        return blobs.containsKey(objectName);
    }

    @Override
    public ObjectMetadata getObjectMetadata(
            GetObjectMetadataRequest getObjectMetadataRequest)
            throws AmazonClientException, AmazonServiceException {
        simulateS3SocketConnection();
        String blobName = getObjectMetadataRequest.getKey();

        if (!blobs.containsKey(blobName)) {
            throw new AmazonS3Exception("[" + blobName + "] does not exist.");
        }

        return new ObjectMetadata(); // nothing is done with it
    }

    @Override
    public PutObjectResult putObject(PutObjectRequest putObjectRequest)
            throws AmazonClientException, AmazonServiceException {
        simulateS3SocketConnection();
        String blobName = putObjectRequest.getKey();

        if (blobs.containsKey(blobName)) {
            throw new AmazonS3Exception("[" + blobName + "] already exists.");
        }

        blobs.put(blobName, putObjectRequest.getInputStream());
        return new PutObjectResult();
    }

    @Override
    public S3Object getObject(GetObjectRequest getObjectRequest)
            throws AmazonClientException, AmazonServiceException {
        simulateS3SocketConnection();
        // in ESBlobStoreContainerTestCase.java, the prefix is empty,
        // so the key and blobName are equivalent to each other
        String blobName = getObjectRequest.getKey();

        if (!blobs.containsKey(blobName)) {
            throw new AmazonS3Exception("[" + blobName + "] does not exist.");
        }

        // the HTTP request attribute is irrelevant for reading
        S3ObjectInputStream stream = new S3ObjectInputStream(
                blobs.get(blobName), null, false);
        S3Object s3Object = new S3Object();
        s3Object.setObjectContent(stream);
        return s3Object;
    }

    @Override
    public ObjectListing listObjects(ListObjectsRequest listObjectsRequest)
            throws AmazonClientException, AmazonServiceException {
        simulateS3SocketConnection();
        MockObjectListing list = new MockObjectListing();
        list.setTruncated(false);

        String blobName;
        String prefix = listObjectsRequest.getPrefix();

        ArrayList<S3ObjectSummary> mockObjectSummaries = new ArrayList<>();

        for (Map.Entry<String, InputStream> blob : blobs.entrySet()) {
            blobName = blob.getKey();
            S3ObjectSummary objectSummary = new S3ObjectSummary();

            if (prefix.isEmpty() || blobName.startsWith(prefix)) {
                objectSummary.setKey(blobName);

                try {
                    objectSummary.setSize(getSize(blob.getValue()));
                } catch (IOException e) {
                    throw new AmazonS3Exception("Object listing " +
                            "failed for blob [" + blob.getKey() + "]");
                }

                mockObjectSummaries.add(objectSummary);
            }
        }

        list.setObjectSummaries(mockObjectSummaries);
        return list;
    }

    @Override
    public CopyObjectResult copyObject(CopyObjectRequest copyObjectRequest)
            throws AmazonClientException, AmazonServiceException {
        simulateS3SocketConnection();
        String sourceBlobName = copyObjectRequest.getSourceKey();
        String targetBlobName = copyObjectRequest.getDestinationKey();

        if (!blobs.containsKey(sourceBlobName)) {
            throw new AmazonS3Exception("Source blob [" +
                    sourceBlobName + "] does not exist.");
        }

        if (blobs.containsKey(targetBlobName)) {
            throw new AmazonS3Exception("Target blob [" +
                    targetBlobName + "] already exists.");
        }

        blobs.put(targetBlobName, blobs.get(sourceBlobName));
        return new CopyObjectResult(); // nothing is done with it
    }

    @Override
    public void deleteObject(DeleteObjectRequest deleteObjectRequest)
            throws AmazonClientException, AmazonServiceException {
        simulateS3SocketConnection();
        String blobName = deleteObjectRequest.getKey();

        if (!blobs.containsKey(blobName)) {
            throw new AmazonS3Exception("[" + blobName + "] does not exist.");
        }

        blobs.remove(blobName);
    }

    private int getSize(InputStream stream) throws IOException {
        int size = stream.read(byteCounter);
        stream.reset(); // in case we ever need the size again
        return size;
    }

    private class MockObjectListing extends ObjectListing {
        // the objectSummaries attribute in ObjectListing.java
        // is read-only, but we need to be able to write to it,
        // so we create a mock of it to work around this
        private List<S3ObjectSummary> mockObjectSummaries;

        @Override
        public List<S3ObjectSummary> getObjectSummaries() {
            return mockObjectSummaries;
        }

        private void setObjectSummaries(List<S3ObjectSummary> objectSummaries) {
            mockObjectSummaries = objectSummaries;
        }
    }
}
