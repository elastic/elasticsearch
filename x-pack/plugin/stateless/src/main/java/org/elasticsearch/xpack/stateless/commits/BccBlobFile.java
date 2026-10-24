/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.xpack.stateless.engine.PrimaryTermAndGeneration;

/**
 * Represents a file (typically a {@link BatchedCompoundCommit}) stored in the blobstore.
 */
public record BccBlobFile(String blobName, PrimaryTermAndGeneration termAndGeneration) implements BlobFile {

    public static final String PREFIX = "stateless_commit_";

    public BccBlobFile {
        assert (startsWithBlobPrefix(blobName) == false && termAndGeneration.generation() == -1)
            || termAndGeneration.generation() == parseGenerationFromBlobName(blobName)
            : "generation mismatch: " + termAndGeneration + " vs " + blobName;
    }

    /// Creates a blob from the primary term of its container and its name, the generation being deduced from the name.
    ///
    /// @param blobName    the name of the blob, which must start with [#PREFIX]
    /// @param primaryTerm the primary term of the container the blob is stored in
    /// @throws IllegalArgumentException if the generation in the blob name is not a number
    public BccBlobFile(String blobName, long primaryTerm) {
        this(blobName, new PrimaryTermAndGeneration(primaryTerm, parseGenerationFromBlobName(blobName)));
    }

    /// Creates a blob from the primary term of its container and its generation, the name being deduced from the generation.
    ///
    /// @param termAndGeneration the primary term of the container the blob is stored in and the generation of the batched compound commit
    public BccBlobFile(PrimaryTermAndGeneration termAndGeneration) {
        this(blobNameFromGeneration(termAndGeneration.generation()), termAndGeneration);
    }

    @Override
    public long primaryTerm() {
        return termAndGeneration.primaryTerm();
    }

    /**
     * The generation of the blob file in case it is a stateless compound commit file, otherwise it is -1.
     */
    public long generation() {
        return termAndGeneration.generation();
    }

    @Override
    public String toString() {
        return "BccBlobFile[term=" + primaryTerm() + ", name=" + blobName() + ']';
    }

    public static boolean startsWithBlobPrefix(String name) {
        return name.startsWith(PREFIX);
    }

    public static String blobNameFromGeneration(long generation) {
        assert generation > 0 : generation;
        return PREFIX + generation;
    }

    public static long parseGenerationFromBlobName(String name) {
        assert startsWithBlobPrefix(name) : name;
        return Long.parseLong(name.substring(PREFIX.length()));
    }
}
