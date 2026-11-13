/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

import java.io.IOException;

/// Represents a [BatchedCompoundCommit] blob stored in the object store.
public record BlobFile(String blobName, PrimaryTermAndGeneration termAndGeneration) implements Blob {

    /// @throws IllegalArgumentException if the blob name does not start with [BatchedCompoundCommit#PREFIX], if the generation in the blob
    /// name is not a number, or if it differs from the generation of the given term and generation
    public BlobFile {
        if (termAndGeneration.generation() != parseGenerationOrThrow(blobName)) {
            throw new IllegalArgumentException("generation mismatch: " + termAndGeneration + " vs " + blobName);
        }
    }

    /// Creates a [BatchedCompoundCommit] blob from its name and the primary term of the container it is stored in, the generation being
    /// deduced from the name.
    ///
    /// @param blobName the name of the blob, which must start with [BatchedCompoundCommit#PREFIX]
    /// @param primaryTerm the primary term of the container the blob is stored in
    /// @throws IllegalArgumentException if the blob name does not start with [BatchedCompoundCommit#PREFIX] or if the generation in the
    /// blob name is not a number
    public BlobFile(String blobName, long primaryTerm) {
        this(blobName, new PrimaryTermAndGeneration(primaryTerm, parseGenerationOrThrow(blobName)));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(blobName);
        termAndGeneration.writeTo(out);
    }

    public long primaryTerm() {
        return termAndGeneration.primaryTerm();
    }

    /// The generation of the batched compound commit, deduced from the blob name.
    public long generation() {
        return termAndGeneration.generation();
    }

    @Override
    public String toString() {
        return "BlobFile[term=" + primaryTerm() + ", name=" + blobName() + ']';
    }

    /// Parses the generation from the name of a [BatchedCompoundCommit] blob, and throws an [IllegalArgumentException] if the blob name is
    /// invalid.
    ///
    /// @param blobName the name of the blob
    /// @return the generation deduced from the blob name
    /// @throws IllegalArgumentException if the blob name does not start with [BatchedCompoundCommit#PREFIX] or if the generation in the
    /// blob name is not a number
    private static long parseGenerationOrThrow(String blobName) {
        if (BatchedCompoundCommit.startsWithBlobPrefix(blobName) == false) {
            throw new IllegalArgumentException("unrecognized blob name [" + blobName + ']');
        }
        final long generation;
        try {
            generation = BatchedCompoundCommit.parseGenerationFromBlobName(blobName);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("malformed blob name [" + blobName + "]: the generation is not a number", e);
        }
        if (generation <= 0) {
            throw new IllegalArgumentException("malformed blob name [" + blobName + "]: the generation must be positive");
        }
        return generation;
    }
}
