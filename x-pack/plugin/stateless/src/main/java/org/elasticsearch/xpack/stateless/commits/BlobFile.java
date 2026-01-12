/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.commits;

import co.elastic.elasticsearch.stateless.engine.PrimaryTermAndGeneration;

import static co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit.startsWithBlobPrefix;

/**
 * Represents a file (typically a {@link BatchedCompoundCommit}) stored in the blobstore.
 */
public record BlobFile(String blobName, PrimaryTermAndGeneration termAndGeneration) {

    public BlobFile {
        assert (startsWithBlobPrefix(blobName) == false && termAndGeneration.generation() == -1)
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
