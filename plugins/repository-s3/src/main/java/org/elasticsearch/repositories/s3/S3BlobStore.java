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

import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.unit.ByteSizeValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Locale;

class S3BlobStore implements BlobStore {

    private final S3Service service;

    private final String clientName;

    private final String bucket;

    private final ByteSizeValue bufferSize;

    private final boolean serverSideEncryption;

    private final CannedAccessControlList cannedACL;

    private final StorageClass storageClass;

    S3BlobStore(S3Service service, String clientName, String bucket, boolean serverSideEncryption,
                ByteSizeValue bufferSize, String cannedACL, String storageClass) {
        this.service = service;
        this.clientName = clientName;
        this.bucket = bucket;
        this.serverSideEncryption = serverSideEncryption;
        this.bufferSize = bufferSize;
        this.cannedACL = initCannedACL(cannedACL);
        this.storageClass = initStorageClass(storageClass);
    }

    @Override
    public String toString() {
        return bucket;
    }

    public AmazonS3Reference clientReference() {
        return service.client(clientName);
    }

    public String bucket() {
        return bucket;
    }

    public boolean serverSideEncryption() {
        return serverSideEncryption;
    }

    public long bufferSizeInBytes() {
        return bufferSize.getBytes();
    }

    @Override
    public BlobContainer blobContainer(BlobPath path) {
        return new S3BlobContainer(path, this);
    }

    @Override
    public void delete(BlobPath path) {
        try (AmazonS3Reference clientReference = clientReference()) {
            ObjectListing prevListing = null;
            // From
            // http://docs.amazonwebservices.com/AmazonS3/latest/dev/DeletingMultipleObjectsUsingJava.html
            // we can do at most 1K objects per delete
            // We don't know the bucket name until first object listing
            DeleteObjectsRequest multiObjectDeleteRequest = null;
            final ArrayList<KeyVersion> keys = new ArrayList<>();
            while (true) {
                ObjectListing list;
                if (prevListing != null) {
                    final ObjectListing finalPrevListing = prevListing;
                    list = SocketAccess.doPrivileged(() -> clientReference.client().listNextBatchOfObjects(finalPrevListing));
                } else {
                    list = SocketAccess.doPrivileged(() -> clientReference.client().listObjects(bucket, path.buildAsString()));
                    multiObjectDeleteRequest = new DeleteObjectsRequest(list.getBucketName());
                }
                for (final S3ObjectSummary summary : list.getObjectSummaries()) {
                    keys.add(new KeyVersion(summary.getKey()));
                    // Every 500 objects batch the delete request
                    if (keys.size() > 500) {
                        multiObjectDeleteRequest.setKeys(keys);
                        final DeleteObjectsRequest finalMultiObjectDeleteRequest = multiObjectDeleteRequest;
                        SocketAccess.doPrivilegedVoid(() -> clientReference.client().deleteObjects(finalMultiObjectDeleteRequest));
                        multiObjectDeleteRequest = new DeleteObjectsRequest(list.getBucketName());
                        keys.clear();
                    }
                }
                if (list.isTruncated()) {
                    prevListing = list;
                } else {
                    break;
                }
            }
            if (!keys.isEmpty()) {
                multiObjectDeleteRequest.setKeys(keys);
                final DeleteObjectsRequest finalMultiObjectDeleteRequest = multiObjectDeleteRequest;
                SocketAccess.doPrivilegedVoid(() -> clientReference.client().deleteObjects(finalMultiObjectDeleteRequest));
            }
        }
    }

    @Override
    public void close() throws IOException {
        this.service.close();
    }

    public CannedAccessControlList getCannedACL() {
        return cannedACL;
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public static StorageClass initStorageClass(String storageClass) {
        if ((storageClass == null) || storageClass.equals("")) {
            return StorageClass.Standard;
        }

        try {
            final StorageClass _storageClass = StorageClass.fromValue(storageClass.toUpperCase(Locale.ENGLISH));
            if (_storageClass.equals(StorageClass.Glacier)) {
                throw new BlobStoreException("Glacier storage class is not supported");
            }

            return _storageClass;
        } catch (final IllegalArgumentException illegalArgumentException) {
            throw new BlobStoreException("`" + storageClass + "` is not a valid S3 Storage Class.");
        }
    }

    /**
     * Constructs canned acl from string
     */
    public static CannedAccessControlList initCannedACL(String cannedACL) {
        if ((cannedACL == null) || cannedACL.equals("")) {
            return CannedAccessControlList.Private;
        }

        for (final CannedAccessControlList cur : CannedAccessControlList.values()) {
            if (cur.toString().equalsIgnoreCase(cannedACL)) {
                return cur;
            }
        }

        throw new BlobStoreException("cannedACL is not valid: [" + cannedACL + "]");
    }
}
