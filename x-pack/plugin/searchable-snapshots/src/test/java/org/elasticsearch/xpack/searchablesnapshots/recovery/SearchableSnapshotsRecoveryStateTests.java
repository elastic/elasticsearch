/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.recovery;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class SearchableSnapshotsRecoveryStateTests extends ESTestCase {
    public void testStageDoesNotTransitionToDoneUntilPreWarmingHasFinished() {
        SearchableSnapshotRecoveryState recoveryState = createRecoveryState();

        recoveryState.setStage(RecoveryState.Stage.INIT)
            .setStage(RecoveryState.Stage.INDEX)
            .setStage(RecoveryState.Stage.VERIFY_INDEX)
            .setStage(RecoveryState.Stage.TRANSLOG);
        recoveryState.getIndex().setFileDetailsComplete();
        recoveryState.setStage(RecoveryState.Stage.FINALIZE).setStage(RecoveryState.Stage.DONE);

        assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.FINALIZE));
    }

    public void testsetStageThrowsAnExceptionOnInvalidTransitions() {
        SearchableSnapshotRecoveryState recoveryState = createRecoveryState();
        expectThrows(AssertionError.class, () -> recoveryState.setStage(RecoveryState.Stage.DONE));
    }

    public void testStageTransitionsToDoneOncePreWarmingHasFinished() {
        SearchableSnapshotRecoveryState recoveryState = createRecoveryState();
        assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.INIT));
        recoveryState.setPreWarmComplete();

        assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.INIT));

        recoveryState.setStage(RecoveryState.Stage.INDEX).setStage(RecoveryState.Stage.VERIFY_INDEX).setStage(RecoveryState.Stage.TRANSLOG);
        recoveryState.getIndex().setFileDetailsComplete();
        recoveryState.setStage(RecoveryState.Stage.FINALIZE).setStage(RecoveryState.Stage.DONE);

        assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));
    }

    public void testStageTransitionsToDoneOncePreWarmingFinishesOnShardStartedStage() {
        SearchableSnapshotRecoveryState recoveryState = createRecoveryState();

        recoveryState.setStage(RecoveryState.Stage.INDEX).setStage(RecoveryState.Stage.VERIFY_INDEX).setStage(RecoveryState.Stage.TRANSLOG);
        recoveryState.getIndex().setFileDetailsComplete();
        recoveryState.setStage(RecoveryState.Stage.FINALIZE);

        recoveryState.setPreWarmComplete();

        recoveryState.setStage(RecoveryState.Stage.DONE);

        assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));

        assertThat(recoveryState.getTimer().stopTime(), greaterThan(0L));
    }

    public void testStageTransitionsToDoneOncePreWarmingFinishesOnHoldShardStartedStage() {
        SearchableSnapshotRecoveryState recoveryState = createRecoveryState();

        recoveryState.setStage(RecoveryState.Stage.INDEX).setStage(RecoveryState.Stage.VERIFY_INDEX).setStage(RecoveryState.Stage.TRANSLOG);
        recoveryState.getIndex().setFileDetailsComplete();
        recoveryState.setStage(RecoveryState.Stage.FINALIZE).setStage(RecoveryState.Stage.DONE);

        recoveryState.setPreWarmComplete();

        assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.DONE));

        assertThat(recoveryState.getTimer().stopTime(), greaterThan(0L));
    }

    public void testIndexTimerIsStartedDuringConstruction() {
        SearchableSnapshotRecoveryState recoveryState = createRecoveryState();

        assertThat(recoveryState.getIndex().startTime(), not(equalTo(0L)));
    }

    public void testIndexTimerMethodsAreBypassed() {
        SearchableSnapshotRecoveryState recoveryState = createRecoveryState();

        RecoveryState.Index index = recoveryState.getIndex();
        long initialStartTime = index.startTime();
        assertThat(initialStartTime, not(equalTo(0L)));

        index.reset();

        assertThat(index.startTime(), equalTo(initialStartTime));

        index.start();

        assertThat(index.startTime(), equalTo(initialStartTime));

        assertThat(index.stopTime(), equalTo(0L));

        index.stop();

        assertThat(index.stopTime(), equalTo(0L));
    }

    public void testIndexTimerIsStoppedOncePreWarmingFinishes() {
        SearchableSnapshotRecoveryState recoveryState = createRecoveryState();
        assertThat(recoveryState.getIndex().stopTime(), equalTo(0L));

        recoveryState.setPreWarmComplete();

        assertThat(recoveryState.getIndex().stopTime(), greaterThan(0L));
    }

    public void testFilesAreIgnored() {
        SearchableSnapshotRecoveryState recoveryState = createRecoveryState();
        recoveryState.ignoreFile("non_pre_warmed_file");
        recoveryState.getIndex().addFileDetail("non_pre_warmed_file", 100, false);

        assertThat(recoveryState.getIndex().getFileDetails("non_pre_warmed_file"), is(nullValue()));
    }

    public void testResetAfterRemoteTranslogIsSetResetsFlag() {
        SearchableSnapshotRecoveryState recoveryState = createRecoveryState();
        recoveryState.getIndex().setFileDetailsComplete();

        recoveryState.setStage(RecoveryState.Stage.INDEX).setStage(RecoveryState.Stage.VERIFY_INDEX).setRemoteTranslogStage();

        assertThat(recoveryState.getStage(), equalTo(RecoveryState.Stage.FINALIZE));
        assertThat(recoveryState.isRemoteTranslogSet(), equalTo(true));

        recoveryState.setStage(RecoveryState.Stage.INIT);
        assertThat(recoveryState.isRemoteTranslogSet(), equalTo(false));
    }

    private SearchableSnapshotRecoveryState createRecoveryState() {
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            randomAlphaOfLength(10),
            0,
            randomAlphaOfLength(10),
            true,
            ShardRoutingState.INITIALIZING
        );
        DiscoveryNode targetNode = DiscoveryNodeUtils.create("local");
        return new SearchableSnapshotRecoveryState(shardRouting, targetNode, null);
    }
}
