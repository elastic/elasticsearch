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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Map;

/**
 * An interface for managing a repository of blob entries, where each blob entry is just a named group of bytes.
 */
public interface BlobContainer {

    /**
     * Gets the {@link BlobPath} that defines the implementation specific paths to where the blobs are contained.
     *
     * @return  the BlobPath where the blobs are contained
     */
    BlobPath path();

    /**
     * Tests whether a blob with the given blob name exists in the container.
     *
     * @param   blobName
     *          The name of the blob whose existence is to be determined.
     * @return  {@code true} if a blob exists in the {@link BlobContainer} with the given name, and {@code false} otherwise.
     */
    boolean blobExists(String blobName);

    /**
     * Creates a new {@link InputStream} for the given blob name.
     *
     * @param   blobName
     *          The name of the blob to get an {@link InputStream} for.
     * @return  The {@code InputStream} to read the blob.
     * @throws  NoSuchFileException if the blob does not exist
     * @throws  IOException if the blob can not be read.
     */
    InputStream readBlob(String blobName) throws IOException;

    /**
     * Reads blob content from the input stream and writes it to the container in a new blob with the given name.
     * This method assumes the container does not already contain a blob of the same blobName.  If a blob by the
     * same name already exists, the operation will fail and an {@link IOException} will be thrown.
     *
     * @param   blobName
     *          The name of the blob to write the contents of the input stream to.
     * @param   inputStream
     *          The input stream from which to retrieve the bytes to write to the blob.
     * @param   blobSize
     *          The size of the blob to be written, in bytes.  It is implementation dependent whether
     *          this value is used in writing the blob to the repository.
     * @throws  FileAlreadyExistsException if a blob by the same name already exists
     * @throws  IOException if the input stream could not be read, or the target blob could not be written to.
     */
    void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException;

    /**
     * Deletes a blob with giving name, if the blob exists.  If the blob does not exist, this method throws an IOException.
     *
     * @param   blobName
     *          The name of the blob to delete.
     * @throws  NoSuchFileException if the blob does not exist
     * @throws  IOException if the blob exists but could not be deleted.
     */
    void deleteBlob(String blobName) throws IOException;

    /**
     * Lists all blobs in the container.
     *
     * @return  A map of all the blobs in the container.  The keys in the map are the names of the blobs and
     *          the values are {@link BlobMetaData}, containing basic information about each blob.
     * @throws  IOException if there were any failures in reading from the blob container.
     */
    Map<String, BlobMetaData> listBlobs() throws IOException;

    /**
     * Lists all blobs in the container that match the specified prefix.
     *
     * @param   blobNamePrefix
     *          The prefix to match against blob names in the container.
     * @return  A map of the matching blobs in the container.  The keys in the map are the names of the blobs
     *          and the values are {@link BlobMetaData}, containing basic information about each blob.
     * @throws  IOException if there were any failures in reading from the blob container.
     */
    Map<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException;

    /**
     * Renames the source blob into the target blob.  If the source blob does not exist or the
     * target blob already exists, an exception is thrown.  Atomicity of the move operation
     * can only be guaranteed on an implementation-by-implementation basis.  The only current
     * implementation of {@link BlobContainer} for which atomicity can be guaranteed is the
     * {@link org.elasticsearch.common.blobstore.fs.FsBlobContainer}.
     *
     * @param   sourceBlobName
     *          The blob to rename.
     * @param   targetBlobName
     *          The name of the blob after the renaming.
     * @throws  IOException if the source blob does not exist, the target blob already exists,
     *          or there were any failures in reading from the blob container.
     */
    void move(String sourceBlobName, String targetBlobName) throws IOException;
}
