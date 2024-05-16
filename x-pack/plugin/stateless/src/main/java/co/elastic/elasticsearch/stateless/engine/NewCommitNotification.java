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

package co.elastic.elasticsearch.stateless.engine;

import co.elastic.elasticsearch.stateless.commits.StatelessCompoundCommit;

import org.elasticsearch.core.Nullable;

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
) {}
