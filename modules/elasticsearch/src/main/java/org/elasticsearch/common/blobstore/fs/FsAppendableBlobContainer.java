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

package org.elasticsearch.common.blobstore.fs;

import org.elasticsearch.common.blobstore.AppendableBlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.DataOutputStreamOutput;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author kimchy (shay.banon)
 */
public class FsAppendableBlobContainer extends AbstractFsBlobContainer implements AppendableBlobContainer {

    public FsAppendableBlobContainer(FsBlobStore blobStore, BlobPath blobPath, File path) {
        super(blobStore, blobPath, path);
    }

    @Override public AppendableBlob appendBlob(String blobName) throws IOException {
        return new FsAppendableBlob(new File(path, blobName));
    }

    private class FsAppendableBlob implements AppendableBlob {

        private final File file;

        public FsAppendableBlob(File file) throws IOException {
            this.file = file;
        }

        @Override public void append(final AppendBlobListener listener) {
            blobStore.executor().execute(new Runnable() {
                @Override public void run() {
                    RandomAccessFile raf = null;
                    try {
                        raf = new RandomAccessFile(file, "rw");
                        raf.seek(raf.length());
                        listener.withStream(new DataOutputStreamOutput(raf));
                        raf.close();
                        FileSystemUtils.syncFile(file);
                        listener.onCompleted();
                    } catch (IOException e) {
                        listener.onFailure(e);
                    } finally {
                        if (raf != null) {
                            try {
                                raf.close();
                            } catch (IOException e) {
                                // ignore
                            }
                        }
                    }
                }
            });
        }

        @Override public void close() {
            // nothing to do there
        }
    }
}