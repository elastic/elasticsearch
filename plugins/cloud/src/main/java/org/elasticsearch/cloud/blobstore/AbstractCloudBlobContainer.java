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

package org.elasticsearch.cloud.blobstore;

import com.google.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.support.AbstractBlobContainer;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Maps;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.PageSet;
import org.jclouds.blobstore.domain.StorageMetadata;
import org.jclouds.blobstore.options.ListContainerOptions;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author kimchy (shay.banon)
 */
public class AbstractCloudBlobContainer extends AbstractBlobContainer {

    protected final CloudBlobStore cloudBlobStore;

    protected final String cloudPath;

    public AbstractCloudBlobContainer(BlobPath path, CloudBlobStore cloudBlobStore) {
        super(path);
        this.cloudBlobStore = cloudBlobStore;
        this.cloudPath = path.buildAsString("/");
    }

    @Override public boolean deleteBlob(String blobName) throws IOException {
        cloudBlobStore.sync().removeBlob(cloudBlobStore.container(), buildBlobPath(blobName));
        return true;
    }

    @Override public boolean blobExists(String blobName) {
        return cloudBlobStore.sync().blobExists(cloudBlobStore.container(), buildBlobPath(blobName));
    }

    @Override public void readBlob(String blobName, final ReadBlobListener listener) {
        final ListenableFuture<? extends Blob> future = cloudBlobStore.async().getBlob(cloudBlobStore.container(), buildBlobPath(blobName));
        future.addListener(new Runnable() {
            @Override public void run() {
                Blob blob;
                try {
                    blob = future.get();
                } catch (InterruptedException e) {
                    listener.onFailure(e);
                    return;
                } catch (ExecutionException e) {
                    listener.onFailure(e.getCause());
                    return;
                }
                byte[] buffer = new byte[cloudBlobStore.bufferSizeInBytes()];
                InputStream is = blob.getContent();
                try {
                    int bytesRead;
                    while ((bytesRead = is.read(buffer)) != -1) {
                        listener.onPartial(buffer, 0, bytesRead);
                    }
                    listener.onCompleted();
                } catch (Exception e) {
                    try {
                        is.close();
                    } catch (IOException e1) {
                        // ignore
                    }
                    listener.onFailure(e);
                }
            }
        }, cloudBlobStore.executorService());
    }

    @Override public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        PageSet<? extends StorageMetadata> list = cloudBlobStore.sync().list(cloudBlobStore.container(), ListContainerOptions.Builder.recursive().inDirectory(blobNamePrefix));
        ImmutableMap.Builder<String, BlobMetaData> blobs = ImmutableMap.builder();
        for (StorageMetadata storageMetadata : list) {
            blobs.put(storageMetadata.getName(), new PlainBlobMetaData(storageMetadata.getName(), storageMetadata.getSize(), null));
        }
        return blobs.build();
    }

    @Override public ImmutableMap<String, BlobMetaData> listBlobs() throws IOException {
        PageSet<? extends StorageMetadata> list = cloudBlobStore.sync().list(cloudBlobStore.container(), ListContainerOptions.Builder.recursive());
        ImmutableMap.Builder<String, BlobMetaData> blobs = ImmutableMap.builder();
        for (StorageMetadata storageMetadata : list) {
            blobs.put(storageMetadata.getName(), new PlainBlobMetaData(storageMetadata.getName(), storageMetadata.getSize(), null));
        }
        return blobs.build();
    }

    protected String buildBlobPath(String blobName) {
        return cloudPath + "/" + blobName;
    }

    private Map<String, StorageMetadata> list(String container, String prefix) {
        final Map<String, StorageMetadata> allMetaDatas = Maps.newHashMap();

        String nextMarker = null;
        while (true) {
            ListContainerOptions options = ListContainerOptions.Builder.recursive();
            if (prefix != null) {
                options = options.inDirectory(prefix);
            }
            if (nextMarker != null) {
                options.afterMarker(nextMarker);
            }
            PageSet<? extends StorageMetadata> pageSet = cloudBlobStore.sync().list(container, options);
            for (StorageMetadata metadata : pageSet) {
                allMetaDatas.put(metadata.getName(), metadata);
            }
            nextMarker = pageSet.getNextMarker();
            if (nextMarker == null) {
                break;
            }
        }
        return allMetaDatas;
    }
}
