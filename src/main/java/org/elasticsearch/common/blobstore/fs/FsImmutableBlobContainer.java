/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

package org.elasticsearch.common.blobstore.fs;

import org.elasticsearch.ElasticSearchIllegalStateException;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.ImmutableBlobContainer;
import org.elasticsearch.common.blobstore.support.BlobStores;
import org.elasticsearch.common.io.FileSystemUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;

/**
 *
 */
public class FsImmutableBlobContainer extends AbstractFsBlobContainer implements ImmutableBlobContainer {

    public FsImmutableBlobContainer(FsBlobStore blobStore, BlobPath blobPath, File path) {
        super(blobStore, blobPath, path);
    }

    @Override
    public void writeBlob(final String blobName, final InputStream is, final long sizeInBytes, final WriterListener listener) {
        blobStore.executor().execute(new Runnable() {
            @Override
            public void run() {
                File file = new File(path, blobName);
                RandomAccessFile raf;
                try {
                    raf = new RandomAccessFile(file, "rw");
                    // clean the file if it exists
                    raf.setLength(0);
                } catch (Exception e) {
                    listener.onFailure(e);
                    return;
                }
                try {
                    try {
                        long bytesWritten = 0;
                        byte[] buffer = new byte[blobStore.bufferSizeInBytes()];
                        int bytesRead;
                        while ((bytesRead = is.read(buffer)) != -1) {
                            raf.write(buffer, 0, bytesRead);
                            bytesWritten += bytesRead;
                        }
                        if (bytesWritten != sizeInBytes) {
                            listener.onFailure(new ElasticSearchIllegalStateException("[" + blobName + "]: wrote [" + bytesWritten + "], expected to write [" + sizeInBytes + "]"));
                            return;
                        }
                    } finally {
                        try {
                            is.close();
                        } catch (IOException ex) {
                            // do nothing
                        }
                        try {
                            raf.close();
                        } catch (IOException ex) {
                            // do nothing
                        }
                    }
                    FileSystemUtils.syncFile(file);
                    listener.onCompleted();
                } catch (Exception e) {
                    // just on the safe size, try and delete it on failure
                    try {
                        if (file.exists()) {
                            file.delete();
                        }
                    } catch (Exception e1) {
                        // ignore
                    }
                    listener.onFailure(e);
                }
            }
        });
    }

    @Override
    public void writeBlob(String blobName, InputStream is, long sizeInBytes) throws IOException {
        BlobStores.syncWriteBlob(this, blobName, is, sizeInBytes);
    }
}
