/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;

/// A blob stored in the blobstore: either a [batched compound commit][BccBlobFile] or a [merge blob][MergeBlobFile].
public sealed interface BlobFile extends Writeable permits BccBlobFile, MergeBlobFile {

    /// @return the primary term of the container the blob is stored in
    long primaryTerm();

    /// @return the name of the blob in its container
    String blobName();

    /// @return the primary term of the blob container and the generation deduced from the blob name. The meaning of the generation depends
    ///         on the implementation.
    PrimaryTermAndGeneration termAndGeneration();

    /// Writes the blob name followed by the primary term and the generation. The right implementation to use is deduced from the blob name
    /// when deserializing, see [#readFrom(StreamInput)].
    @Override
    default void writeTo(StreamOutput out) throws IOException {
        out.writeString(blobName());
        termAndGeneration().writeTo(out);
    }

    /// Reads a blob written by [#writeTo(StreamOutput)].
    ///
    /// @throws IOException if the stream cannot be read, or if the generation does not match the one deduced from the blob name
    /// @throws IllegalArgumentException if the blob name is not recognized
    static BlobFile readFrom(StreamInput in) throws IOException {
        final String blobName = in.readString();
        final var termAndGeneration = new PrimaryTermAndGeneration(in);
        final BlobFile blobFile = of(termAndGeneration.primaryTerm(), blobName);
        if (blobFile.termAndGeneration().equals(termAndGeneration) == false) {
            throw new IOException("generation mismatch: " + termAndGeneration + " vs " + blobName);
        }
        return blobFile;
    }

    /// Creates the blob of the right kind for the given name, which is recognized by its prefix.
    ///
    /// @param primaryTerm the primary term of the container the blob is stored in
    /// @param blobName    the name of the blob
    /// @throws IllegalArgumentException if the blob name is not recognized or is not well-formed
    static BlobFile of(final long primaryTerm, final String blobName) {
        if (MergeBlobFile.startsWithBlobPrefix(blobName)) {
            return new MergeBlobFile(blobName, primaryTerm);
        } else if (BccBlobFile.startsWithBlobPrefix(blobName)) {
            return new BccBlobFile(blobName, primaryTerm);
        } else {
            throw new IllegalArgumentException("unrecognized blob name [" + blobName + "] with primary term [" + primaryTerm + ']');
        }
    }
}
