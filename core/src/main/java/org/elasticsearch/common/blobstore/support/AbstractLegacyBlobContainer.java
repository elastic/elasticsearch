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

package org.elasticsearch.common.blobstore.support;

import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.Streams;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Temporary compatibility interface.
 *
 * This class should be removed after S3 and Azure containers migrate to the new model
 */
@Deprecated
public abstract class AbstractLegacyBlobContainer extends AbstractBlobContainer {

    protected AbstractLegacyBlobContainer(BlobPath path) {
        super(path);
    }

    /**
     * Creates a new {@link InputStream} for the given blob name
     * <p/>
     * This method is deprecated and is used only for compatibility with older blob containers
     * The new blob containers should use readBlob/writeBlob methods instead
     */
    @Deprecated
    protected abstract InputStream openInput(String blobName) throws IOException;

    /**
     * Creates a new OutputStream for the given blob name
     * <p/>
     * This method is deprecated and is used only for compatibility with older blob containers
     * The new blob containers should override readBlob/writeBlob methods instead
     */
    @Deprecated
    protected abstract OutputStream createOutput(String blobName) throws IOException;

    @Override
    public InputStream readBlob(String blobName) throws IOException {
        return openInput(blobName);
    }

    @Override
    public void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException {
        try (OutputStream stream = createOutput(blobName)) {
            Streams.copy(inputStream, stream);
        }
    }

    @Override
    public void writeBlob(String blobName, BytesReference data) throws IOException {
        try (OutputStream stream = createOutput(blobName)) {
            data.writeTo(stream);
        }
    }
}
