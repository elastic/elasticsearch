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

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.DeleteObjectsRequest.KeyVersion;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.ImmutableBlobContainer;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.concurrent.Executor;

/**
 *
 */
public class S3BlobStore extends AbstractComponent implements BlobStore {

    private final AmazonS3 client;

    private final String bucket;

    private final String region;

    private final ThreadPool threadPool;

    private final int bufferSizeInBytes;

    public S3BlobStore(Settings settings, AmazonS3 client, String bucket, @Nullable String region, ThreadPool threadPool) {
        super(settings);
        this.client = client;
        this.bucket = bucket;
        this.region = region;
        this.threadPool = threadPool;

        this.bufferSizeInBytes = (int) settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).bytes();

        if (!client.doesBucketExist(bucket)) {
            if (region != null) {
                client.createBucket(bucket, region);
            } else {
                client.createBucket(bucket);
            }
        }
    }

    @Override
    public String toString() {
        return (region == null ? "" : region + "/") + bucket;
    }

    public AmazonS3 client() {
        return client;
    }

    public String bucket() {
        return bucket;
    }

    public Executor executor() {
        return threadPool.executor(ThreadPool.Names.SNAPSHOT_DATA);
    }

    public int bufferSizeInBytes() {
        return bufferSizeInBytes;
    }

    @Override
    public ImmutableBlobContainer immutableBlobContainer(BlobPath path) {
        return new S3ImmutableBlobContainer(path, this);
    }

    @Override
    public void delete(BlobPath path) {
        ObjectListing prevListing = null;
        //From http://docs.amazonwebservices.com/AmazonS3/latest/dev/DeletingMultipleObjectsUsingJava.html
        //we can do at most 1K objects per delete
        //We don't know the bucket name until first object listing
        DeleteObjectsRequest multiObjectDeleteRequest = null;
        ArrayList<KeyVersion> keys = new ArrayList<KeyVersion>();
        while (true) {
            ObjectListing list;
            if (prevListing != null) {
                list = client.listNextBatchOfObjects(prevListing);
            } else {
                String keyPath = path.buildAsString("/");
                if (!keyPath.isEmpty()) {
                    keyPath = keyPath + "/";
                }
                list = client.listObjects(bucket, keyPath);
                multiObjectDeleteRequest = new DeleteObjectsRequest(list.getBucketName());
            }
            for (S3ObjectSummary summary : list.getObjectSummaries()) {
                keys.add(new KeyVersion(summary.getKey()));
                //Every 500 objects batch the delete request
                if (keys.size() > 500) {
                    multiObjectDeleteRequest.setKeys(keys);
                    client.deleteObjects(multiObjectDeleteRequest);
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
            client.deleteObjects(multiObjectDeleteRequest);
        }
    }

    @Override
    public void close() {
    }
}
