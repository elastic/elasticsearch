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
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.ImmutableBlobContainer;
import org.elasticsearch.common.blobstore.support.BlobStores;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public class S3ImmutableBlobContainer extends AbstractS3BlobContainer implements ImmutableBlobContainer {

    public S3ImmutableBlobContainer(BlobPath path, S3BlobStore blobStore) {
        super(path, blobStore);
    }

    @Override
    public void writeBlob(final String blobName, final InputStream is, final long sizeInBytes, final WriterListener listener) {
        blobStore.executor().execute(new Runnable() {
            @Override
            public void run() {
                int retry = 0;
                // Read limit is ignored by InputStreamIndexInput, but we will set it anyway in case
                // implementation will change
                is.mark(Integer.MAX_VALUE);
                while (true) {
                    try {
                        ObjectMetadata md = new ObjectMetadata();
                        if (blobStore.serverSideEncryption()) {
                            md.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                        }
                        md.setContentLength(sizeInBytes);
                        blobStore.client().putObject(blobStore.bucket(), buildKey(blobName), is, md);
                        listener.onCompleted();
                        return;
                    } catch (AmazonS3Exception e) {
                        if (shouldRetry(e) && retry < blobStore.numberOfRetries()) {
                            try {
                                is.reset();
                            } catch (IOException ex) {
                                listener.onFailure(e);
                                return;
                            }
                            retry++;
                        } else {
                            listener.onFailure(e);
                            return;
                        }
                    } catch (Throwable e) {
                        listener.onFailure(e);
                        return;
                    }
                }
            }
        });
    }

    @Override
    public void writeBlob(String blobName, InputStream is, long sizeInBytes) throws IOException {
        BlobStores.syncWriteBlob(this, blobName, is, sizeInBytes);
    }
}
