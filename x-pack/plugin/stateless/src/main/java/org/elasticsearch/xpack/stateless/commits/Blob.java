/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/// A blob stored in the object store: either a [batched compound commit][BccBlob] or a [merge blob][MergeBlob].
///
/// The type of the blob is not serialized: the name of the blob is what allows [#readFrom(StreamInput, Class)] to find the right
/// implementation type when deserializing, so every implementation must have a name that is recognizable by its prefix, and must write
/// its name first in [#writeTo]. What follows the name is up to the implementation, which reads it in a static
/// `readFrom(String, StreamInput)` method: a [BccBlob] has a term and a generation, whereas a [MergeBlob] has no generation and only a
/// primary term.
public sealed interface Blob extends Writeable permits BccBlob, MergeBlob {

    /// @return the name of the blob
    String blobName();

    /// @return the primary term of the container the blob is stored in
    long primaryTerm();

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
    /// @throws IOException if the blob name is not recognized or is not well-formed, if the term/generation read from the stream of a
    /// [BccBlob] does not match the blob name, or if the blob is not an instance of the expected type
    static <T extends Blob> T readFrom(StreamInput in, Class<T> expectedType) throws IOException {
        final String blobName = in.readString();
        final Blob blob;
        if (BatchedCompoundCommit.startsWithBlobPrefix(blobName)) {
            blob = BccBlob.readFrom(blobName, in);
        } else if (MergeBlob.startsWithPrefix(blobName)) {
            blob = MergeBlob.readFrom(blobName, in);
        } else {
            throw new IOException("unrecognized blob name [" + blobName + ']');
        }
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
}
