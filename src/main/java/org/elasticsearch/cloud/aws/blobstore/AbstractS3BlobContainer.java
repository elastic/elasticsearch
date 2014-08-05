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
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStoreException;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.ImmutableMap;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class AbstractS3BlobContainer extends AbstractBlobContainer {

    protected final S3BlobStore blobStore;

    protected final String keyPath;

    public AbstractS3BlobContainer(BlobPath path, S3BlobStore blobStore) {
        super(path);
        this.blobStore = blobStore;
        String keyPath = path.buildAsString("/");
        if (!keyPath.isEmpty()) {
            keyPath = keyPath + "/";
        }
        this.keyPath = keyPath;
    }

    @Override
    public boolean blobExists(String blobName) {
        try {
            blobStore.client().getObjectMetadata(blobStore.bucket(), buildKey(blobName));
            return true;
        } catch (AmazonS3Exception e) {
            return false;
        } catch (Throwable e) {
            throw new BlobStoreException("failed to check if blob exists", e);
        }
    }

    @Override
    public boolean deleteBlob(String blobName) throws IOException {
        blobStore.client().deleteObject(blobStore.bucket(), buildKey(blobName));
        return true;
    }

    @Override
    public void readBlob(final String blobName, final ReadBlobListener listener) {
        blobStore.executor().execute(new Runnable() {
            @Override
            public void run() {
                InputStream is;
                try {
                    S3Object object = blobStore.client().getObject(blobStore.bucket(), buildKey(blobName));
                    is = object.getObjectContent();
                } catch (AmazonS3Exception e) {
                    if (e.getStatusCode() == 404) {
                        listener.onFailure(new FileNotFoundException(e.getMessage()));
                    } else {
                        listener.onFailure(e);
                    }
                    return;
                } catch (Throwable e) {
                    listener.onFailure(e);
                    return;
                }
                byte[] buffer = new byte[blobStore.bufferSizeInBytes()];
                try {
                    int bytesRead;
                    while ((bytesRead = is.read(buffer)) != -1) {
                        listener.onPartial(buffer, 0, bytesRead);
                    }
                    listener.onCompleted();
                } catch (Throwable e) {
                    IOUtils.closeWhileHandlingException(is);
                    listener.onFailure(e);
                }
            }
        });
    }

    @Override
    public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(@Nullable String blobNamePrefix) throws IOException {
        ImmutableMap.Builder<String, BlobMetaData> blobsBuilder = ImmutableMap.builder();
        ObjectListing prevListing = null;
        while (true) {
            ObjectListing list;
            if (prevListing != null) {
                list = blobStore.client().listNextBatchOfObjects(prevListing);
            } else {
                if (blobNamePrefix != null) {
                    list = blobStore.client().listObjects(blobStore.bucket(), buildKey(blobNamePrefix));
                } else {
                    list = blobStore.client().listObjects(blobStore.bucket(), keyPath);
                }
            }
            for (S3ObjectSummary summary : list.getObjectSummaries()) {
                String name = summary.getKey().substring(keyPath.length());
                blobsBuilder.put(name, new PlainBlobMetaData(name, summary.getSize()));
            }
            if (list.isTruncated()) {
                prevListing = list;
            } else {
                break;
            }
        }
        return blobsBuilder.build();
    }

    @Override
    public ImmutableMap<String, BlobMetaData> listBlobs() throws IOException {
        return listBlobsByPrefix(null);
    }

    protected String buildKey(String blobName) {
        return keyPath + blobName;
    }

    protected boolean shouldRetry(AmazonS3Exception e) {
        return e.getStatusCode() == 400 && "RequestTimeout".equals(e.getErrorCode());
    }

}
