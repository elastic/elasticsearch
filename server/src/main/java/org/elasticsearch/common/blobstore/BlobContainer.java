/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;
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
    boolean blobExists(String blobName) throws IOException;

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
     * Creates a new {@link InputStream} that can be used to read the given blob starting from
     * a specific {@code position} in the blob. The {@code length} is an indication of the
     * number of bytes that are expected to be read from the {@link InputStream}.
     *
     * @param blobName The name of the blob to get an {@link InputStream} for.
     * @param position The position in the blob where the next byte will be read.
     * @param length   An indication of the number of bytes to be read.
     * @return The {@code InputStream} to read the blob.
     * @throws NoSuchFileException if the blob does not exist
     * @throws IOException         if the blob can not be read.
     */
    InputStream readBlob(String blobName, long position, long length) throws IOException;

    /**
     * Provides a hint to clients for a suitable length to use with {@link BlobContainer#readBlob(String, long, long)}.
     *
     * Some blob containers have nontrivial costs attached to each readBlob call, so it is a good idea for consumers to speculatively
     * request more data than they need right now and to re-use this stream for future needs if possible.
     *
     * Also, some blob containers return streams that are expensive to close before the stream has been fully consumed, and the cost may
     * depend on the length of the data that was left unconsumed. For these containers it's best to bound the cost of a partial read by
     * bounding the length of the data requested.
     *
     * @return a hint to consumers regarding the length of data to request if there is a good chance that future reads can be satisfied from
     * the same stream.
     *
     */
    default long readBlobPreferredLength() {
        throw new UnsupportedOperationException(); // NORELEASE
    }

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
     * Reads blob content from a {@link BytesReference} and writes it to the container in a new blob with the given name.
     *
     * @param   blobName
     *          The name of the blob to write the contents of the input stream to.
     * @param   bytes
     *          The bytes to write
     * @param   failIfAlreadyExists
     *          whether to throw a FileAlreadyExistsException if the given blob already exists
     * @throws  FileAlreadyExistsException if failIfAlreadyExists is true and a blob by the same name already exists
     * @throws  IOException if the input stream could not be read, or the target blob could not be written to.
     */
    default void writeBlob(String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException {
        writeBlob(blobName, bytes.streamInput(), bytes.length(), failIfAlreadyExists);
    }

    /**
     * Write a blob by providing a consumer that will write its contents to an output stream. This method allows serializing a blob's
     * contents directly to the blob store without having to materialize the serialized version in full before writing.
     *
     * @param blobName            the name of the blob to write
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob already exists
     * @param atomic              whether the write should be atomic in case the implementation supports it
     * @param writer              consumer for an output stream that will write the blob contents to the stream
     */
    void writeBlob(String blobName, boolean failIfAlreadyExists, boolean atomic,
                   CheckedConsumer<OutputStream, IOException> writer) throws IOException;

    /**
     * Reads blob content from a {@link BytesReference} and writes it to the container in a new blob with the given name,
     * using an atomic write operation if the implementation supports it.
     *
     * @param   blobName
     *          The name of the blob to write the contents of the input stream to.
     * @param   bytes
     *          The bytes to write
     * @param   failIfAlreadyExists
     *          whether to throw a FileAlreadyExistsException if the given blob already exists
     * @throws  FileAlreadyExistsException if failIfAlreadyExists is true and a blob by the same name already exists
     * @throws  IOException if the input stream could not be read, or the target blob could not be written to.
     */
    void writeBlobAtomic(String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException;

    /**
     * Deletes this container and all its contents from the repository.
     *
     * @return delete result
     * @throws IOException on failure
     */
    DeleteResult delete() throws IOException;

    /**
     * Deletes the blobs with given names. This method will not throw an exception
     * when one or multiple of the given blobs don't exist and simply ignore this case.
     *
     * @param   blobNames  the names of the blobs to delete
     * @throws  IOException if a subset of blob exists but could not be deleted.
     */
    void deleteBlobsIgnoringIfNotExists(Iterator<String> blobNames) throws IOException;

    /**
     * Lists all blobs in the container.
     *
     * @return  A map of all the blobs in the container.  The keys in the map are the names of the blobs and
     *          the values are {@link BlobMetadata}, containing basic information about each blob.
     * @throws  IOException if there were any failures in reading from the blob container.
     */
    Map<String, BlobMetadata> listBlobs() throws IOException;

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
     *          and the values are {@link BlobMetadata}, containing basic information about each blob.
     * @throws  IOException if there were any failures in reading from the blob container.
     */
    Map<String, BlobMetadata> listBlobsByPrefix(String blobNamePrefix) throws IOException;
}
