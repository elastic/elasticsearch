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

package org.elasticsearch.common.blobstore;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 *
 */
public interface BlobContainer {

    BlobPath path();

    boolean blobExists(String blobName);

    /**
     * Creates a new {@link InputStream} for the given blob name
     */
    InputStream openInput(String blobName) throws IOException;

    /**
     * Creates a new OutputStream for the given blob name
     */
    OutputStream createOutput(String blobName) throws IOException;

    /**
     * Deletes a blob with giving name.
     *
     * If blob exist but cannot be deleted an exception has to be thrown.
     */
    void deleteBlob(String blobName) throws IOException;

    /**
     * Deletes all blobs in the container that match the specified prefix.
     */
    void deleteBlobsByPrefix(String blobNamePrefix) throws IOException;

    /**
     * Lists all blobs in the container
     */
    ImmutableMap<String, BlobMetaData> listBlobs() throws IOException;

    /**
     * Lists all blobs in the container that match specified prefix
     */
    ImmutableMap<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException;

    /**
     * Atomically renames source blob into target blob
     */
    void move(String sourceBlobName, String targetBlobName) throws IOException;
}
