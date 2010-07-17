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
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.ImmutableBlobContainer;
import org.elasticsearch.common.blobstore.support.BlobStores;
import org.jclouds.blobstore.domain.Blob;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutionException;

/**
 * @author kimchy (shay.banon)
 */
public class CloudImmutableBlobContainer extends AbstractCloudBlobContainer implements ImmutableBlobContainer {

    public CloudImmutableBlobContainer(BlobPath path, CloudBlobStore cloudBlobStore) {
        super(path, cloudBlobStore);
    }

    @Override public void writeBlob(String blobName, InputStream is, long sizeInBytes, final WriterListener listener) {
        Blob blob = cloudBlobStore.sync().newBlob(buildBlobPath(blobName));
        blob.setPayload(is);
        blob.setContentLength(sizeInBytes);
        final ListenableFuture<String> future = cloudBlobStore.async().putBlob(cloudBlobStore.container(), blob);
        future.addListener(new Runnable() {
            @Override public void run() {
                try {
                    future.get();
                    listener.onCompleted();
                } catch (InterruptedException e) {
                    listener.onFailure(e);
                } catch (ExecutionException e) {
                    listener.onFailure(e.getCause());
                } catch (Throwable t) {
                    listener.onFailure(t);
                }
            }
        }, cloudBlobStore.executorService());
    }

    @Override public void writeBlob(String blobName, InputStream is, long sizeInBytes) throws IOException {
        BlobStores.syncWriteBlob(this, blobName, is, sizeInBytes);
    }
}
