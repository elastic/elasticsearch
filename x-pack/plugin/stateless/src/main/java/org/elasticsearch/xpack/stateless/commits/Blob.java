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

/// A blob stored in the object store: always a [batched compound commit][BlobFile].
public sealed interface Blob extends Writeable permits BlobFile {

    /// @return the name of the blob
    String blobName();

    /// @return the primary term of the container the blob is stored in
    long primaryTerm();

    /// @return the primary term of the blob container and the generation deduced from the blob name. The meaning of the generation depends
    /// on the implementation type of the blob.
    PrimaryTermAndGeneration termAndGeneration();

    /// Writes the blob name and its term/generation. The type of the blob is not serialized: the name of the blob is what allows
    /// [#readFrom(StreamInput, Class)] to find the right implementation type when deserializing, so every implementation must have a name
    /// that is recognizable by its prefix.
    @Override
    default void writeTo(StreamOutput out) throws IOException {
        out.writeString(blobName());
        termAndGeneration().writeTo(out);
    }

    /// @return a [Writeable.Reader] of blobs that must be of the expected type, see [#readFrom(StreamInput, Class)].
    static <T extends Blob> Writeable.Reader<T> reader(Class<T> expectedType) {
        return in -> readFrom(in, expectedType);
    }

    /// Reads a [Blob] from the stream. The implementation type is recognized by the prefix of the blob name and must be an instance of the
    /// expected type, so that callers that cannot handle every kind of blob fail with an [IOException] rather than a later
    /// [ClassCastException].
    ///
    /// @param in the stream to read from
    /// @param expectedType the type of blob the caller can handle
    /// @throws IOException if the blob name is not recognized or is not well-formed, if the term/generation read from the stream does not
    /// match the blob name, or if the blob is not an instance of the expected type
    static <T extends Blob> T readFrom(StreamInput in, Class<T> expectedType) throws IOException {
        final String blobName = in.readString();
        final var termAndGeneration = new PrimaryTermAndGeneration(in);
        final Blob blob = of(blobName, termAndGeneration);
        if (expectedType.isInstance(blob) == false) {
            throw new IOException(
                "expected a ["
                    + expectedType.getSimpleName()
                    + "] but blob ["
                    + blobName
                    + "] is a ["
                    + blob.getClass().getSimpleName()
                    + ']'
            );
        }
        return expectedType.cast(blob);
    }

    /// Creates a [Blob] of the implementation type recognized by the prefix of the blob name, and checks that the given term/generation is
    /// consistent with the name.
    ///
    /// @throws IOException if the blob name is not recognized or is not well-formed, or if the term/generation does not match the name
    private static Blob of(String blobName, PrimaryTermAndGeneration termAndGeneration) throws IOException {
        final Blob blob;
        if (BatchedCompoundCommit.startsWithBlobPrefix(blobName)) {
            try {
                blob = new BlobFile(blobName, termAndGeneration.primaryTerm());
            } catch (IllegalArgumentException e) {
                throw new IOException("malformed blob name [" + blobName + ']', e);
            }
        } else {
            throw new IOException("unrecognized blob name [" + blobName + "] with " + termAndGeneration);
        }
        if (blob.termAndGeneration().equals(termAndGeneration) == false) {
            throw new IOException("term/generation mismatch: " + termAndGeneration + " vs " + blobName);
        }
        return blob;
    }
}
