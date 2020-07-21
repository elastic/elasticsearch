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
import org.elasticsearch.index.store.cache.CacheFile;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OnDemandRecoveryStateTests extends ESTestCase {
    public void testRecoveredBytesIsBackedByCacheFiles() {
        OnDemandRecoveryState recoveryState = createRecoveryState();
        OnDemandRecoveryState.Index index = (OnDemandRecoveryState.Index) recoveryState.getIndex();
        long fileLength = randomLongBetween(0, 500);
        index.addFileDetail("file", fileLength, false);
        index.setFileDetailsComplete();

        CacheFile cacheFile = mock(CacheFile.class);
        when(cacheFile.getLength()).thenReturn(fileLength);
        index.addCacheFileDetail("file", cacheFile);

        // This call is a no-op
        index.addRecoveredBytesToFile("file", randomInt(1024));

        assertThat(index.getFileDetails("file").recovered(), is(fileLength));
    }

    public void testFileEvictionIsTracked() {
        OnDemandRecoveryState recoveryState = createRecoveryState();
        OnDemandRecoveryState.Index index = (OnDemandRecoveryState.Index) recoveryState.getIndex();

        long fileLength = randomLongBetween(0, 500);
        index.addFileDetail("file", fileLength, false);
        index.setFileDetailsComplete();

        CacheFile cacheFile = mock(CacheFile.class);
        when(cacheFile.getLength()).thenReturn(fileLength);
        index.addCacheFileDetail("file", cacheFile);

        assertThat(index.getFileDetails("file").recovered(), is(fileLength));

        index.removeCacheFileDetail("file", cacheFile);

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

    public void testAddSnapshotFilesMultipleTimes() {
        OnDemandRecoveryState recoveryState = createRecoveryState();
        RecoveryState.Index index = recoveryState.getIndex();

        long fileLength = randomLongBetween(0, 500);
        index.addFileDetail("file", fileLength, false);
        index.addFileDetail("file", fileLength, false);

        assertThat(index.getFileDetails("file").reused(), is(false));
        assertThat(index.getFileDetails("file").length(), is(fileLength));
    }

    public void testClearKeepsFileDetails() {
        OnDemandRecoveryState recoveryState = createRecoveryState();
        RecoveryState.Index index = recoveryState.getIndex();

        long fileLength = randomLongBetween(0, 500);
        index.addFileDetail("file", fileLength, false);
        index.addFileDetail("another_file", randomLongBetween(0, 500), true);

        assertThat(index.getFileDetails("file"), is(notNullValue()));
        assertThat(index.getFileDetails("another_file"), is(notNullValue()));

        recoveryState.getIndex().reset();

        assertThat(index.getFileDetails("file"), is(notNullValue()));
        assertThat(index.getFileDetails("another_file"), is(notNullValue()));
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
