/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

public class ClusterStateUpdateStatsWireSerializationTests extends AbstractWireSerializingTestCase<ClusterStateUpdateStats> {

    @Override
    protected Writeable.Reader<ClusterStateUpdateStats> instanceReader() {
        return ClusterStateUpdateStats::new;
    }

    @Override
    protected ClusterStateUpdateStats createTestInstance() {
        return new ClusterStateUpdateStats(
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );
    }

    private static long not(long l) {
        return randomValueOtherThan(l, ESTestCase::randomNonNegativeLong);
    }

    @Override
    protected ClusterStateUpdateStats mutateInstance(ClusterStateUpdateStats instance) {
        switch (between(1, 19)) {
            case 1:
                return new ClusterStateUpdateStats(
                    not(instance.getUnchangedTaskCount()),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 2:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    not(instance.getPublicationSuccessCount()),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 3:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    not(instance.getPublicationFailureCount()),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 4:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    not(instance.getUnchangedComputationElapsedMillis()),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 5:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    not(instance.getUnchangedNotificationElapsedMillis()),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 6:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    not(instance.getSuccessfulComputationElapsedMillis()),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 7:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    not(instance.getSuccessfulPublicationElapsedMillis()),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 8:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    not(instance.getSuccessfulContextConstructionElapsedMillis()),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 9:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    not(instance.getSuccessfulCommitElapsedMillis()),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 10:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    not(instance.getSuccessfulCompletionElapsedMillis()),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 11:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    not(instance.getSuccessfulMasterApplyElapsedMillis()),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 12:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    not(instance.getSuccessfulNotificationElapsedMillis()),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 13:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    not(instance.getFailedComputationElapsedMillis()),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 14:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    not(instance.getFailedPublicationElapsedMillis()),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 15:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    not(instance.getFailedContextConstructionElapsedMillis()),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 16:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    not(instance.getFailedCommitElapsedMillis()),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 17:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    not(instance.getFailedCompletionElapsedMillis()),
                    instance.getFailedMasterApplyElapsedMillis(),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 18:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    not(instance.getFailedMasterApplyElapsedMillis()),
                    instance.getFailedNotificationElapsedMillis()
                );
            case 19:
                return new ClusterStateUpdateStats(
                    instance.getUnchangedTaskCount(),
                    instance.getPublicationSuccessCount(),
                    instance.getPublicationFailureCount(),
                    instance.getUnchangedComputationElapsedMillis(),
                    instance.getUnchangedNotificationElapsedMillis(),
                    instance.getSuccessfulComputationElapsedMillis(),
                    instance.getSuccessfulPublicationElapsedMillis(),
                    instance.getSuccessfulContextConstructionElapsedMillis(),
                    instance.getSuccessfulCommitElapsedMillis(),
                    instance.getSuccessfulCompletionElapsedMillis(),
                    instance.getSuccessfulMasterApplyElapsedMillis(),
                    instance.getSuccessfulNotificationElapsedMillis(),
                    instance.getFailedComputationElapsedMillis(),
                    instance.getFailedPublicationElapsedMillis(),
                    instance.getFailedContextConstructionElapsedMillis(),
                    instance.getFailedCommitElapsedMillis(),
                    instance.getFailedCompletionElapsedMillis(),
                    instance.getFailedMasterApplyElapsedMillis(),
                    not(instance.getFailedNotificationElapsedMillis())
                );
        }
        throw new AssertionError("impossible");
    }
}
