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
public record BlobFile(String blobName, PrimaryTermAndGeneration termAndGeneration) {

    public BlobFile {
        assert (StatelessCompoundCommit.startsWithBlobPrefix(blobName) == false && termAndGeneration.generation() == -1)
            || termAndGeneration.generation() == StatelessCompoundCommit.parseGenerationFromBlobName(blobName)
            : "generation mismatch: " + termAndGeneration + " vs " + blobName;
    }

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
        return "BlobFile{" + "primaryTerm=" + primaryTerm() + ", blobName='" + blobName + '\'' + '}';
    }

}
