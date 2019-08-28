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
import java.util.List;
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
     * @param   failIfAlreadyExists
     *          whether to throw a FileAlreadyExistsException if the given blob already exists
     * @throws  FileAlreadyExistsException if failIfAlreadyExists is true and a blob by the same name already exists
     * @throws  IOException if the input stream could not be read, or the target blob could not be written to.
     */
    void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException;

    /**
     * Reads blob content from the input stream and writes it to the container in a new blob with the given name,
     * using an atomic write operation if the implementation supports it.
     *
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
     * @param   failIfAlreadyExists
     *          whether to throw a FileAlreadyExistsException if the given blob already exists
     * @throws  FileAlreadyExistsException if failIfAlreadyExists is true and a blob by the same name already exists
     * @throws  IOException if the input stream could not be read, or the target blob could not be written to.
     */
    void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists) throws IOException;

    /**
     * Deletes the blob with the given name, if the blob exists. If the blob does not exist,
     * this method may throw a {@link NoSuchFileException} if the underlying implementation supports an existence check before delete.
     *
     * @param   blobName
     *          The name of the blob to delete.
     * @throws  NoSuchFileException if the blob does not exist
     * @throws  IOException if the blob exists but could not be deleted.
     */
    void deleteBlob(String blobName) throws IOException;

    /**
     * Deletes this container and all its contents from the repository.
     *
     * @return delete result
     * @throws IOException on failure
     */
    DeleteResult delete() throws IOException;

    /**
     * Deletes the blobs with given names. Unlike {@link #deleteBlob(String)} this method will not throw an exception
     * when one or multiple of the given blobs don't exist and simply ignore this case.
     *
     * @param   blobNames  The names of the blob to delete.
     * @throws  IOException if a subset of blob exists but could not be deleted.
     */
    default void deleteBlobsIgnoringIfNotExists(List<String> blobNames) throws IOException {
        IOException ioe = null;
        for (String blobName : blobNames) {
            try {
                deleteBlobIgnoringIfNotExists(blobName);
            } catch (IOException e) {
                if (ioe == null) {
                    ioe = e;
                } else {
                    ioe.addSuppressed(e);
                }
            }
        }
        if (ioe != null) {
            throw ioe;
        }
    }

    /**
     * Deletes a blob with giving name, ignoring if the blob does not exist.
     *
     * @param   blobName
     *          The name of the blob to delete.
     * @throws  IOException if the blob exists but could not be deleted.
     */
    default void deleteBlobIgnoringIfNotExists(String blobName) throws IOException {
        try {
            deleteBlob(blobName);
        } catch (final NoSuchFileException ignored) {
            // This exception is ignored
        }
    }

    /**
     * Lists all blobs in the container.
     *
     * @return  A map of all the blobs in the container.  The keys in the map are the names of the blobs and
     *          the values are {@link BlobMetaData}, containing basic information about each blob.
     * @throws  IOException if there were any failures in reading from the blob container.
     */
    Map<String, BlobMetaData> listBlobs() throws IOException;

    /**
     * Lists all child containers under this container. A child container is defined as a container whose {@link #path()} method returns
     * a path that has this containers {@link #path()} return as its prefix and has one more path element than the current
     * container's path.
     *
     * @return Map of name of the child container to child container
     * @throws IOException on failure to list child containers
     */
    Map<String, BlobContainer> children() throws IOException;

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
}
