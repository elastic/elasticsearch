/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.index.recoveries;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.cache.PersistentCacheTracker;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class OnDemandRecoveryStateTests extends ESTestCase {
    public void testSameRangeCanBeReportedMultipleTimes() {
        OnDemandRecoveryState recoveryState = createRecoveryState();
        RecoveryState.Index index = recoveryState.getIndex();

        long fileLength = randomLongBetween(0, 500);
        index.addFileDetail("file", fileLength, false);
        index.setFileDetailsComplete();

        index.addRecoveredBytesToFile("file", fileLength * 2);
        assertThat(index.getFileDetails("file").recovered(), is(fileLength));
    }

    public void testFileEvictionIsTracked() {
        OnDemandRecoveryState recoveryState = createRecoveryState();
        RecoveryState.Index index = recoveryState.getIndex();

        long fileLength = randomLongBetween(0, 500);
        index.addFileDetail("file", fileLength, false);
        index.addRecoveredBytesToFile("file", fileLength);
        assertThat(index.getFileDetails("file").recovered(), is(fileLength));

        PersistentCacheTracker tracker = (PersistentCacheTracker) index;
        tracker.trackFileEviction("file");

        assertThat(index.getFileDetails("file").recovered(), is(0L));
    }

    public void testLastStageForLazyRecoveriesIsLazyRecovery() {
        OnDemandRecoveryState recoveryState = createRecoveryState();
        recoveryState.getIndex().setFileDetailsComplete();

        recoveryState.setStage(RecoveryState.Stage.INDEX);
        recoveryState.setStage(RecoveryState.Stage.VERIFY_INDEX);
        recoveryState.setStage(RecoveryState.Stage.TRANSLOG);
        recoveryState.setStage(RecoveryState.Stage.FINALIZE);
        recoveryState.setStage(RecoveryState.Stage.DONE);

        assertThat(recoveryState.getStage(), is(RecoveryState.Stage.ON_DEMAND));
    }

    public void testTimerStillRunsAfterReachingLazyRecoveryStage() {
        OnDemandRecoveryState recoveryState = createRecoveryState();
        recoveryState.getIndex().setFileDetailsComplete();

        recoveryState.setStage(RecoveryState.Stage.INDEX);
        recoveryState.setStage(RecoveryState.Stage.VERIFY_INDEX);
        recoveryState.setStage(RecoveryState.Stage.TRANSLOG);
        recoveryState.setStage(RecoveryState.Stage.FINALIZE);
        recoveryState.setStage(RecoveryState.Stage.DONE);

        assertThat(recoveryState.getTimer().stopTime(), is(0L));
        assertThat(recoveryState.getIndex().stopTime(), is(0L));
    }

    public void testReusedIndexFileFlagIsIgnored() {
        OnDemandRecoveryState recoveryState = createRecoveryState();

        RecoveryState.Index index = recoveryState.getIndex();
        String fileName = randomAlphaOfLength(5);
        index.addFileDetail(fileName, randomNonNegativeLong(), true);

        assertThat(index.getFileDetails(fileName).reused(), is(false));
    }

    private OnDemandRecoveryState createRecoveryState() {
        ShardId shardId = new ShardId(randomAlphaOfLength(5), randomAlphaOfLength(5), randomInt());
        ShardRouting shard = ShardRouting.newUnassigned(
            shardId,
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.NEW_INDEX_RESTORED, "test")
        );
        shard = shard.initialize(randomAlphaOfLength(5), null, -1);

        return new OnDemandRecoveryState(shard, mock(DiscoveryNode.class), null);
    }
}
