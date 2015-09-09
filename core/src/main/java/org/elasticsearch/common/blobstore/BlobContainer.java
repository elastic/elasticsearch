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

import org.elasticsearch.common.bytes.BytesReference;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public interface BlobContainer {

    BlobPath path();

    boolean blobExists(String blobName);

    /**
     * Creates a new InputStream for the given blob name
     */
    InputStream readBlob(String blobName) throws IOException;

    /**
     * Reads blob content from the input stream and writes it to the blob store
     */
    void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException;

    /**
     * Writes bytes to the blob
     */
    void writeBlob(String blobName, BytesReference bytes) throws IOException;

    /**
     * Deletes a blob with giving name.
     *
     * If a blob exists but cannot be deleted an exception has to be thrown.
     */
    void deleteBlob(String blobName) throws IOException;

    /**
     * Deletes blobs with giving names.
     *
     * If a blob exists but cannot be deleted an exception has to be thrown.
     */
    void deleteBlobs(Collection<String> blobNames) throws IOException;

    /**
     * Deletes all blobs in the container that match the specified prefix.
     */
    void deleteBlobsByPrefix(String blobNamePrefix) throws IOException;

    /**
     * Lists all blobs in the container
     */
    Map<String, BlobMetaData> listBlobs() throws IOException;

    /**
     * Lists all blobs in the container that match specified prefix
     */
    Map<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException;

    /**
     * Atomically renames source blob into target blob
     */
    void move(String sourceBlobName, String targetBlobName) throws IOException;
}
