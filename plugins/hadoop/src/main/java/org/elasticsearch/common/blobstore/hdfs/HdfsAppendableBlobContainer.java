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

package org.elasticsearch.common.blobstore.hdfs;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.elasticsearch.common.blobstore.AppendableBlobContainer;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.io.stream.DataOutputStreamOutput;

import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * @author kimchy (shay.banon)
 */
public class HdfsAppendableBlobContainer extends AbstractHdfsBlobContainer implements AppendableBlobContainer {

    public HdfsAppendableBlobContainer(HdfsBlobStore blobStore, BlobPath blobPath, Path path) {
        super(blobStore, blobPath, path);
    }

    @Override public AppendableBlob appendBlob(String blobName) throws IOException {
        return new HdfsAppendableBlob(new Path(path, blobName));
    }

    @Override public boolean canAppendToExistingBlob() {
        return false;
    }

    private class HdfsAppendableBlob implements AppendableBlob {

        private final Path file;

        private final FSDataOutputStream fsDataStream;

        private final DataOutputStreamOutput out;

        public HdfsAppendableBlob(Path file) throws IOException {
            this.file = file;
            this.fsDataStream = blobStore.fileSystem().create(file, true);
            this.out = new DataOutputStreamOutput(fsDataStream);
        }

        @Override public void append(final AppendBlobListener listener) {
            blobStore.executorService().execute(new Runnable() {
                @Override public void run() {
                    RandomAccessFile raf = null;
                    try {
                        listener.withStream(out);
                        out.flush();
                        fsDataStream.flush();
                        fsDataStream.sync();
                        listener.onCompleted();
                    } catch (IOException e) {
                        listener.onFailure(e);
                    }
                }
            });
        }

        @Override public void close() {
            try {
                fsDataStream.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}