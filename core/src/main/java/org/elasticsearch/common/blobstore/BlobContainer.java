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
     * @throws  IOException if the blob does not exist or can not be read.
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
     * @throws  IOException if the input stream could not be read, a blob by the same name already exists,
     *          or the target blob could not be written to.
     */
    void writeBlob(String blobName, InputStream inputStream, long blobSize) throws IOException;

    /**
     * Writes the input bytes to a new blob in the container with the given name.  This method assumes the
     * container does not already contain a blob of the same blobName.  If a blob by the same name already
     * exists, the operation will fail and an {@link IOException} will be thrown.
     *
     * TODO: Remove this in favor of a single {@link #writeBlob(String, InputStream, long)} method.
     *       See https://github.com/elastic/elasticsearch/issues/18528
     *
     * @param   blobName
     *          The name of the blob to write the contents of the input stream to.
     * @param   bytes
     *          The bytes to write to the blob.
     * @throws  IOException if a blob by the same name already exists, or the target blob could not be written to.
     */
    void writeBlob(String blobName, BytesReference bytes) throws IOException;

    /**
     * Deletes a blob with giving name, if the blob exists.  If the blob does not exist, this method throws an IOException.
     *
     * @param   blobName
     *          The name of the blob to delete.
     * @throws  IOException if the blob does not exist, or if the blob exists but could not be deleted.
     */
    void deleteBlob(String blobName) throws IOException;

    /**
     * Deletes blobs with the given names.  If any subset of the names do not exist in the container, this method has no
     * effect for those names, and will delete the blobs for those names that do exist.  If any of the blobs failed
     * to delete, those blobs that were processed before it and successfully deleted will remain deleted.  An exception
     * is thrown at the first blob entry that fails to delete (TODO: is this the right behavior?  Should we collect
     * all the failed deletes into a single IOException instead?)
     *
     * TODO: remove, see https://github.com/elastic/elasticsearch/issues/18529
     *
     * @param   blobNames
     *          The collection of blob names to delete from the container.
     * @throws  IOException if any of the blobs in the collection exists but could not be deleted.
     */
    void deleteBlobs(Collection<String> blobNames) throws IOException;

    /**
     * Deletes all blobs in the container that match the specified prefix.  If any of the blobs failed to delete,
     * those blobs that were processed before it and successfully deleted will remain deleted.  An exception is
     * thrown at the first blob entry that fails to delete (TODO: is this the right behavior?  Should we collect
     * all the failed deletes into a single IOException instead?)
     *
     * TODO: remove, see: https://github.com/elastic/elasticsearch/issues/18529
     *
     * @param   blobNamePrefix
     *          The prefix to match against blob names in the container.  Any blob whose name has the prefix will be deleted.
     * @throws  IOException if any of the matching blobs failed to delete.
     */
    void deleteBlobsByPrefix(String blobNamePrefix) throws IOException;

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
     * Atomically renames the source blob into the target blob.  If the source blob does not exist or the
     * target blob already exists, an exception is thrown.
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
