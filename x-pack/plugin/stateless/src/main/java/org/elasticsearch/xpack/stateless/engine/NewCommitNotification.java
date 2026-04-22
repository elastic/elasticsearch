/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;

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
 */
public record NewCommitNotification(
    StatelessCompoundCommit compoundCommit,
    long batchedCompoundCommitGeneration,
    @Nullable PrimaryTermAndGeneration latestUploadedBatchedCompoundCommitTermAndGen,
    long clusterStateVersion,
    String nodeId
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
}
