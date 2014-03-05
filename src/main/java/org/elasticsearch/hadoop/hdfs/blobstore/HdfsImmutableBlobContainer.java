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
package org.elasticsearch.hadoop.hdfs.blobstore;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.ImmutableBlobContainer;
import org.elasticsearch.common.blobstore.support.BlobStores;

public class HdfsImmutableBlobContainer extends AbstractHdfsBlobContainer implements ImmutableBlobContainer {

    public HdfsImmutableBlobContainer(BlobPath blobPath, HdfsBlobStore blobStore, Path path) {
        super(blobPath, blobStore, path);
    }

    @Override
    public void writeBlob(final String blobName, final InputStream is, final long sizeInBytes, final WriterListener listener) {
        blobStore.executor().execute(new Runnable() {
            @Override
            public void run() {
                Path file;
                FSDataOutputStream fileStream;

                try {
                    file = new Path(path, blobName);
                    fileStream = blobStore.fileSystem().create(file, true);
                } catch (Throwable th) {
                    listener.onFailure(th);
                    return;
                }
                try {
                    try {
                        byte[] buffer = new byte[blobStore.bufferSizeInBytes()];
                        int bytesRead;
                        while ((bytesRead = is.read(buffer)) != -1) {
                            fileStream.write(buffer, 0, bytesRead);
                        }
                    } finally {
                        IOUtils.closeStream(is);
                        IOUtils.closeStream(fileStream);
                    }
                    listener.onCompleted();
                } catch (Throwable th) {
                    // just on the safe size, try and delete it on failure
                    try {
                        if (blobStore.fileSystem().exists(file)) {
                            blobStore.fileSystem().delete(file, true);
                        }
                    } catch (Throwable t) {
                        // ignore
                    }
                    listener.onFailure(th);
                }
            }
        });
    }

    @Override
    public void writeBlob(String blobName, InputStream is, long sizeInBytes) throws IOException {
        BlobStores.syncWriteBlob(this, blobName, is, sizeInBytes);
    }
}