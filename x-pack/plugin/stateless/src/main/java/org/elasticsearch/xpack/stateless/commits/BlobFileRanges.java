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

import java.util.Objects;

/**
 * Contains the {@link BlobLocation} of a file.
 * <p>
 * In a short future we expect this class to also contain information on byte ranges that are located beyond the first cache region and
 * whose bytes are also copied in the first region, after the compound commit header and before the internal files, in order to speed up
 * the reading of those ranges.
 */
public record BlobFileRanges(BlobLocation blobLocation) {

    public BlobFileRanges(BlobLocation blobLocation) {
        this.blobLocation = Objects.requireNonNull(blobLocation);
    }

    public String blobName() {
        return blobLocation.blobName();
    }

    public PrimaryTermAndGeneration getBatchedCompoundCommitTermAndGeneration() {
        return blobLocation.getBatchedCompoundCommitTermAndGeneration();
    }

    public long primaryTerm() {
        return blobLocation.primaryTerm();
    }

    public long fileOffset() {
        return blobLocation.offset();
    }

    public long fileLength() {
        return blobLocation.fileLength();
    }
}
