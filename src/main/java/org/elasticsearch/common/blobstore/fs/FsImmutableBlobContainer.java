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

package org.elasticsearch.common.blobstore.fs;

import org.elasticsearch.ElasticsearchIllegalStateException;
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
    public void writeBlob(final String blobName, final InputStream stream, final long sizeInBytes, final WriterListener listener) {
        blobStore.executor().execute(new Runnable() {
            @Override
            public void run() {
                final File file = new File(path, blobName);
                boolean success = false;
                try {
                    try (final RandomAccessFile raf = new RandomAccessFile(file, "rw");
                         final InputStream is = stream) {
                        // clean the file if it exists
                        raf.setLength(0);
                        long bytesWritten = 0;
                        final byte[] buffer = new byte[blobStore.bufferSizeInBytes()];
                        int bytesRead;
                        while ((bytesRead = is.read(buffer)) != -1) {
                            raf.write(buffer, 0, bytesRead);
                            bytesWritten += bytesRead;
                        }
                        if (bytesWritten != sizeInBytes) {
                            throw new ElasticsearchIllegalStateException("[" + blobName + "]: wrote [" + bytesWritten + "], expected to write [" + sizeInBytes + "]");
                        }
                        // fsync the FD we are done with writing
                        raf.getFD().sync();
                        // try to fsync the directory to make sure all metadata is written to
                        // the storage device - NOTE: if it's a dir it will not throw any exception
                        FileSystemUtils.syncFile(path, true);
                    }
                    success = true;
                } catch (Throwable e) {
                    listener.onFailure(e);
                    // just on the safe size, try and delete it on failure
                    FileSystemUtils.tryDeleteFile(file);
                } finally {
                   if (success) {
                       listener.onCompleted();
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