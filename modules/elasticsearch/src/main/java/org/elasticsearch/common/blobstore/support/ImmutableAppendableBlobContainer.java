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

package org.elasticsearch.common.blobstore.support;

import org.elasticsearch.common.blobstore.AppendableBlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.ImmutableBlobContainer;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.common.io.stream.BytesStreamOutput;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Set;

/**
 * An appendable container that uses an immutable container to implement an appendable one.
 *
 * @author kimchy (shay.banon)
 */
public class ImmutableAppendableBlobContainer extends AbstractBlobContainer implements AppendableBlobContainer {

    private final ImmutableBlobContainer container;

    public ImmutableAppendableBlobContainer(ImmutableBlobContainer container) {
        super(container.path());
        this.container = container;
    }

    @Override public AppendableBlob appendBlob(final String blobName) throws IOException {
        return new AppendableBlob() {
            int part = 0;

            @Override public void append(final AppendBlobListener listener) {
                BytesStreamOutput out = new BytesStreamOutput();
                try {
                    listener.withStream(out);
                } catch (Exception e) {
                    listener.onFailure(e);
                    return;
                }
                if (out.size() == 0) {
                    // nothing to write, bail
                    listener.onCompleted();
                    return;
                }
                String partBlobName = blobName + ".a" + (part++);
                // use teh sync one
                ByteArrayInputStream is = new ByteArrayInputStream(out.unsafeByteArray(), 0, out.size());
                container.writeBlob(partBlobName, is, out.size(), new ImmutableBlobContainer.WriterListener() {
                    @Override public void onCompleted() {
                        listener.onCompleted();
                    }

                    @Override public void onFailure(Throwable t) {
                        listener.onFailure(t);
                    }
                });
            }

            @Override public void close() {

            }
        };
    }

    @Override public void readBlob(final String blobName, final ReadBlobListener listener) {
        container.readBlob(blobName + ".a0", new ReadBlobListener() {
            int part = 0;

            @Override public void onPartial(byte[] data, int offset, int size) throws IOException {
                listener.onPartial(data, offset, size);
            }

            @Override public void onCompleted() {
                part++;
                if (container.blobExists(blobName + ".a" + part)) {
                    container.readBlob(blobName + ".a" + part, this);
                } else {
                    listener.onCompleted();
                }
            }

            @Override public void onFailure(Throwable t) {
                listener.onFailure(t);
            }
        });
    }

    @Override public ImmutableMap<String, BlobMetaData> listBlobs() throws IOException {
        return buildVirtualBlobs(container.listBlobs());
    }

    @Override public ImmutableMap<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException {
        return buildVirtualBlobs(container.listBlobsByPrefix(blobNamePrefix));
    }

    @Override public boolean blobExists(String blobName) {
        return container.blobExists(blobName + ".a0");
    }

    @Override public boolean deleteBlob(String blobName) throws IOException {
        container.deleteBlobsByPrefix(blobName + ".");
        return true;
    }

    @Override public void deleteBlobsByFilter(BlobNameFilter filter) throws IOException {
        ImmutableMap<String, BlobMetaData> blobs = buildVirtualBlobs(container.listBlobs());
        for (String blobName : blobs.keySet()) {
            if (filter.accept(blobName)) {
                deleteBlob(blobName);
            }
        }
    }

    @Override public void deleteBlobsByPrefix(String blobNamePrefix) throws IOException {
        container.deleteBlobsByPrefix(blobNamePrefix);
    }

    private ImmutableMap<String, BlobMetaData> buildVirtualBlobs(ImmutableMap<String, BlobMetaData> blobs) {
        Set<String> names = Sets.newHashSet();
        for (BlobMetaData blob : blobs.values()) {
            if (blob.name().endsWith(".a0")) {
                names.add(blob.name().substring(0, blob.name().lastIndexOf(".a0")));
            }
        }
        ImmutableMap.Builder<String, BlobMetaData> builder = ImmutableMap.builder();
        for (String name : names) {
            long sizeInBytes = 0;
            if (blobs.containsKey(name)) {
                // no chunking
                sizeInBytes = blobs.get(name).sizeInBytes();
            } else {
                // chunking...
                int part = 0;
                while (true) {
                    BlobMetaData md = blobs.get(name + ".a" + part);
                    if (md == null) {
                        break;
                    }
                    sizeInBytes += md.sizeInBytes();
                    part++;
                }
            }
            builder.put(name, new PlainBlobMetaData(name, sizeInBytes, null));
        }
        return builder.build();
    }
}
