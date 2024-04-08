/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.blobstore;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.blobstore.support.BlobMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.util.Iterator;
import java.util.Map;

/**
 * An interface for managing a repository of blob entries, where each blob entry is just a named group of bytes.
 *
 * A BlobStore creates BlobContainers.
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
     * @param purpose The purpose of the operation
     * @param blobName The name of the blob whose existence is to be determined.
     * @return {@code true} if a blob exists in the {@link BlobContainer} with the given name, and {@code false} otherwise.
     */
    boolean blobExists(OperationPurpose purpose, String blobName) throws IOException;

    /**
     * Creates a new {@link InputStream} for the given blob name.
     *
     * @param purpose The purpose of the operation
     * @param blobName The name of the blob to get an {@link InputStream} for.
     * @return The {@code InputStream} to read the blob.
     * @throws NoSuchFileException if the blob does not exist
     * @throws IOException         if the blob can not be read.
     */
    InputStream readBlob(OperationPurpose purpose, String blobName) throws IOException;

    /**
     * Creates a new {@link InputStream} that can be used to read the given blob starting from
     * a specific {@code position} in the blob. The {@code length} is an indication of the
     * number of bytes that are expected to be read from the {@link InputStream}.
     *
     * @param purpose The purpose of the operation
     * @param blobName The name of the blob to get an {@link InputStream} for.
     * @param position The position in the blob where the next byte will be read.
     * @param length   An indication of the number of bytes to be read.
     * @return The {@code InputStream} to read the blob.
     * @throws NoSuchFileException if the blob does not exist
     * @throws IOException         if the blob can not be read.
     */
    InputStream readBlob(OperationPurpose purpose, String blobName, long position, long length) throws IOException;

    /**
     * Provides a hint to clients for a suitable length to use with {@link BlobContainer#readBlob(OperationPurpose, String, long, long)}.
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
     * @param purpose             The purpose of the operation
     * @param blobName            The name of the blob to write the contents of the input stream to.
     * @param inputStream         The input stream from which to retrieve the bytes to write to the blob.
     * @param blobSize            The size of the blob to be written, in bytes.  It is implementation dependent whether
     *                            this value is used in writing the blob to the repository.
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob already exists
     * @throws FileAlreadyExistsException if failIfAlreadyExists is true and a blob by the same name already exists
     * @throws IOException                if the input stream could not be read, or the target blob could not be written to.
     */
    void writeBlob(OperationPurpose purpose, String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
        throws IOException;

    /**
     * Reads blob content from a {@link BytesReference} and writes it to the container in a new blob with the given name.
     *
     * @param purpose             The purpose of the operation
     * @param blobName            The name of the blob to write the contents of the input stream to.
     * @param bytes               The bytes to write
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob already exists
     * @throws FileAlreadyExistsException if failIfAlreadyExists is true and a blob by the same name already exists
     * @throws IOException                if the input stream could not be read, or the target blob could not be written to.
     */
    default void writeBlob(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists)
        throws IOException {
        assert assertPurposeConsistency(purpose, blobName);
        writeBlob(purpose, blobName, bytes.streamInput(), bytes.length(), failIfAlreadyExists);
    }

    /**
     * Write a blob by providing a consumer that will write its contents to an output stream. This method allows serializing a blob's
     * contents directly to the blob store without having to materialize the serialized version in full before writing.
     * This method is only used for streaming serialization of repository metadata that is known to be of limited size
     * at any point in time and across all concurrent invocations of this method.
     *
     * @param purpose             The purpose of the operation
     * @param blobName            the name of the blob to write
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob already exists
     * @param atomic              whether the write should be atomic in case the implementation supports it
     * @param writer              consumer for an output stream that will write the blob contents to the stream
     */
    void writeMetadataBlob(
        OperationPurpose purpose,
        String blobName,
        boolean failIfAlreadyExists,
        boolean atomic,
        CheckedConsumer<OutputStream, IOException> writer
    ) throws IOException;

    /**
     * Reads blob content from a {@link BytesReference} and writes it to the container in a new blob with the given name,
     * using an atomic write operation if the implementation supports it.
     *
     * @param purpose             The purpose of the operation
     * @param blobName            The name of the blob to write the contents of the input stream to.
     * @param bytes               The bytes to write
     * @param failIfAlreadyExists whether to throw a FileAlreadyExistsException if the given blob already exists
     * @throws FileAlreadyExistsException if failIfAlreadyExists is true and a blob by the same name already exists
     * @throws IOException                if the input stream could not be read, or the target blob could not be written to.
     */
    void writeBlobAtomic(OperationPurpose purpose, String blobName, BytesReference bytes, boolean failIfAlreadyExists) throws IOException;

    /**
     * Deletes this container and all its contents from the repository.
     *
     * @param purpose The purpose of the operation
     * @return delete result
     * @throws IOException on failure
     */
    DeleteResult delete(OperationPurpose purpose) throws IOException;

    /**
     * Deletes the blobs with given names. This method will not throw an exception
     * when one or multiple of the given blobs don't exist and simply ignore this case.
     *
     * @param purpose The purpose of the operation
     * @param blobNames the names of the blobs to delete
     * @throws IOException if a subset of blob exists but could not be deleted.
     */
    void deleteBlobsIgnoringIfNotExists(OperationPurpose purpose, Iterator<String> blobNames) throws IOException;

    /**
     * Lists all blobs in the container.
     *
     * @return  A map of all the blobs in the container.  The keys in the map are the names of the blobs and
     *          the values are {@link BlobMetadata}, containing basic information about each blob.
     * @throws  IOException if there were any failures in reading from the blob container.
     */
    Map<String, BlobMetadata> listBlobs(OperationPurpose purpose) throws IOException;

    /**
     * Lists all child containers under this container. A child container is defined as a container whose {@link #path()} method returns
     * a path that has this containers {@link #path()} return as its prefix and has one more path element than the current
     * container's path.
     *
     * @param purpose The purpose of the operation
     * @return Map of name of the child container to child container
     * @throws IOException on failure to list child containers
     */
    Map<String, BlobContainer> children(OperationPurpose purpose) throws IOException;

    /**
     * Lists all blobs in the container that match the specified prefix.
     *
     * @param purpose The purpose of the operation
     * @param blobNamePrefix The prefix to match against blob names in the container.
     * @return A map of the matching blobs in the container.  The keys in the map are the names of the blobs
     * and the values are {@link BlobMetadata}, containing basic information about each blob.
     * @throws IOException if there were any failures in reading from the blob container.
     */
    Map<String, BlobMetadata> listBlobsByPrefix(OperationPurpose purpose, String blobNamePrefix) throws IOException;

    /**
     * Atomically sets the value stored at the given key to {@code updated} if the {@code current value == expected}.
     * Keys not yet used start at initial value 0. Returns the current value (before it was updated).
     *
     * @param purpose  The purpose of the operation
     * @param key      key of the value to update
     * @param expected the expected value
     * @param updated  the new value
     * @param listener a listener, completed with the value read from the register (before it was updated) or
     *                 {@link OptionalBytesReference#MISSING} if the value could not be read due to concurrent activity.
     */
    void compareAndExchangeRegister(
        OperationPurpose purpose,
        String key,
        BytesReference expected,
        BytesReference updated,
        ActionListener<OptionalBytesReference> listener
    );

    /**
     * Atomically sets the value stored at the given key to {@code updated} if the {@code current value == expected}.
     * Keys not yet used start at initial value 0.
     *
     * @param purpose
     * @param key      key of the value to update
     * @param expected the expected value
     * @param updated  the new value
     * @param listener a listener which is completed with {@link Boolean#TRUE} if successful, {@link Boolean#FALSE} if the expected value
     *                 did not match the updated value or the value could not be read due to concurrent activity
     */
    default void compareAndSetRegister(
        OperationPurpose purpose,
        String key,
        BytesReference expected,
        BytesReference updated,
        ActionListener<Boolean> listener
    ) {
        compareAndExchangeRegister(
            purpose,
            key,
            expected,
            updated,
            listener.map(witness -> witness.isPresent() && witness.bytesReference().equals(expected))
        );
    }

    /**
     * Gets the value set by {@link #compareAndSetRegister} or {@link #compareAndExchangeRegister} for a given key.
     * If a key has not yet been used, the initial value is an empty {@link BytesReference}.
     *
     * @param purpose The purpose of the operation
     * @param key      key of the value to get
     * @param listener a listener, completed with the value read from the register or {@code OptionalBytesReference#MISSING} if the value
     *                 could not be read due to concurrent activity.
     */
    default void getRegister(OperationPurpose purpose, String key, ActionListener<OptionalBytesReference> listener) {
        compareAndExchangeRegister(purpose, key, BytesArray.EMPTY, BytesArray.EMPTY, listener);
    }

    /**
     * Verify that the {@link OperationPurpose} is (somewhat) suitable for the name of the blob to which it applies:
     * <ul>
     * <li>{@link OperationPurpose#SNAPSHOT_DATA} is not used for blobs that look like metadata blobs.</li>
     * <li>{@link OperationPurpose#SNAPSHOT_METADATA} is not used for blobs that look like data blobs.</li>
     * </ul>
     */
    // This is fairly lenient because we use a wide variety of blob names and purposes in tests in order to get good coverage. See
    // BlobStoreRepositoryOperationPurposeIT for some stricter checks which apply during genuine snapshot operations.
    static boolean assertPurposeConsistency(OperationPurpose purpose, String blobName) {
        switch (purpose) {
            case SNAPSHOT_DATA -> {
                // must not be used for blobs with names that look like metadata blobs
                assert (blobName.startsWith(BlobStoreRepository.INDEX_FILE_PREFIX)
                    || blobName.startsWith(BlobStoreRepository.METADATA_PREFIX)
                    || blobName.startsWith(BlobStoreRepository.SNAPSHOT_PREFIX)
                    || blobName.equals(BlobStoreRepository.INDEX_LATEST_BLOB)) == false : blobName + " should not use purpose " + purpose;
            }
            case SNAPSHOT_METADATA -> {
                // must not be used for blobs with names that look like data blobs
                assert blobName.startsWith(BlobStoreRepository.UPLOADED_DATA_BLOB_PREFIX) == false
                    : blobName + " should not use purpose " + purpose;
            }
            case REPOSITORY_ANALYSIS, CLUSTER_STATE, INDICES, TRANSLOG -> {
                // no specific requirements
            }
        }
        return true;
    }
}
