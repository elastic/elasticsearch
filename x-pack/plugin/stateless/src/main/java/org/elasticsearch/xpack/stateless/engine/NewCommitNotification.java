/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.stateless.commits.BatchedCompoundCommit;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;

import java.util.Map;

/**
 * Notification of a newly created compound commit.
 *
 * @param compoundCommit                                    The new compound commit
 * @param batchedCompoundCommitGeneration                   The generation of the BCC that contains the CC. The BCC's primary term is the
 *                                                          same as the CC's primary.
 * @param latestUploadedBatchedCompoundCommitTermAndGen     The generation of latest uploaded BCC. It is null if no upload has happened.
 * @param clusterStateVersion                               The cluster state version on the node at the time the new compound commit was
 *                                                          notified
 * @param nodeId                                            The id of the node that notified the new compound commit
 * @param newMergedSegmentsSizes                         This commit's new merge-produced segments, mapped from segment name (the shared
 *                                                          file prefix, e.g. {@code _5}) to size in bytes. {@code null} when not computed
 *                                                          (feature disabled on the indexing node, or received from an older node).
 */
public record NewCommitNotification(
    StatelessCompoundCommit compoundCommit,
    long batchedCompoundCommitGeneration,
    @Nullable PrimaryTermAndGeneration latestUploadedBatchedCompoundCommitTermAndGen,
    long clusterStateVersion,
    String nodeId,
    @Nullable Map<String, Long> newMergedSegmentsSizes
) {
    public boolean isBatchedCompoundCommitUploaded() {
        return latestUploadedBatchedCompoundCommitTermAndGen != null
            && latestUploadedBatchedCompoundCommitTermAndGen.generation() == batchedCompoundCommitGeneration();
    }

    @Override
    public String toString() {
        return "NewCommitNotification{"
            + "compoundCommit="
            + compoundCommit.toShortDescription()
            + ", batchedCompoundCommitGeneration="
            + batchedCompoundCommitGeneration
            + ", latestUploadedBatchedCompoundCommitTermAndGen="
            + latestUploadedBatchedCompoundCommitTermAndGen
            + ", clusterStateVersion="
            + clusterStateVersion
            + ", nodeId='"
            + nodeId
            + '\''
            + '}';
    }

    public String toShortDescription() {
        return '['
            + BatchedCompoundCommit.blobNameFromGeneration(batchedCompoundCommitGeneration)
            + ']'
            + compoundCommit.toShortDescription()
            + '['
            + nodeId
            + "]["
            + clusterStateVersion
            + ']'
            + (isBatchedCompoundCommitUploaded() ? "[u]" : "[i]");
    }
}
