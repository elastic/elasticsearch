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
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;

public class OnDemandRecoveryStateTests extends ESTestCase {

    public void testUnknownFileLengthRecoveryDetails() {
        OnDemandRecoveryState recoveryState = createRecoveryState();

        RecoveryState.Index index = recoveryState.getIndex();

        String fileName = randomAlphaOfLength(10);
        long recoveredBytes = randomLongBetween(1, 1024);
        index.addRecoveredBytesToFile(fileName, recoveredBytes);

        index.addFileDetail(fileName, recoveredBytes * 2, false);

        assertThat(index.recoveredFileCount(), is(0));
        assertThat((double) index.recoveredBytesPercent(), closeTo(50.0f, 0.0001f));

        index.addRecoveredBytesToFile(fileName, recoveredBytes);

        assertThat(index.recoveredFileCount(), is(1));
        assertThat((double) index.recoveredBytesPercent(), closeTo(100.0f, 0.0001f));
    }

    public void testFilesWithUnknownLengthDoNotCountForRecoveryStatsUntilAllLengthsAreKnown() {
        OnDemandRecoveryState recoveryState = createRecoveryState();

        RecoveryState.Index index = recoveryState.getIndex();

        Map<String, Long> recoveryFiles = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 10); i++) {
            String fileName = randomAlphaOfLength(10);
            long recoveredBytes = randomNonNegativeLong();
            recoveryFiles.put(fileName, recoveredBytes);

            index.addRecoveredBytesToFile(fileName, recoveredBytes);
        }

        assertThat(index.recoveredFileCount(), is(0));
        assertThat(index.recoveredBytesPercent(), is(0.0f));

        for (Map.Entry<String, Long> recoveredFiles : recoveryFiles.entrySet()) {
            index.addFileDetail(recoveredFiles.getKey(), recoveredFiles.getValue(), false);
        }

        assertThat(index.recoveredFileCount(), is(recoveryFiles.size()));
        assertThat((double) index.recoveredBytesPercent(), closeTo(100.0f, 0.0001f));
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
