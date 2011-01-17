/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.util.concurrent.Executor;

/**
 * @author kimchy (shay.banon)
 */
public class S3BlobStore extends AbstractComponent implements BlobStore {

    private final AmazonS3 client;

    private final String bucket;

    private final String region;

    private final Executor executor;

    private final int bufferSizeInBytes;

    public S3BlobStore(Settings settings, AmazonS3 client, String bucket, @Nullable String region, Executor executor) {
        super(settings);
        this.client = client;
        this.bucket = bucket;
        this.region = region;
        this.executor = executor;

        this.bufferSizeInBytes = (int) settings.getAsBytesSize("buffer_size", new ByteSizeValue(100, ByteSizeUnit.KB)).bytes();

        if (!client.doesBucketExist(bucket)) {
            if (region != null) {
                client.createBucket(bucket, region);
            } else {
                client.createBucket(bucket);
            }
        }
    }

    @Override public String toString() {
        return (region == null ? "" : region + "/") + bucket;
    }

    public AmazonS3 client() {
        return client;
    }

    public String bucket() {
        return bucket;
    }

    public Executor executor() {
        return executor;
    }

    public int bufferSizeInBytes() {
        return bufferSizeInBytes;
    }

    @Override public ImmutableBlobContainer immutableBlobContainer(BlobPath path) {
        return new S3ImmutableBlobContainer(path, this);
    }

    @Override public void delete(BlobPath path) {
        ObjectListing prevListing = null;
        while (true) {
            ObjectListing list;
            if (prevListing != null) {
                list = client.listNextBatchOfObjects(prevListing);
            } else {
                list = client.listObjects(bucket, path.buildAsString("/"));
            }
            for (S3ObjectSummary summary : list.getObjectSummaries()) {
                client.deleteObject(summary.getBucketName(), summary.getKey());
            }
            if (list.isTruncated()) {
                prevListing = list;
            } else {
                break;
            }
        }
    }

    @Override public void close() {
    }
}
