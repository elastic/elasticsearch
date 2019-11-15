/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.snapshots.SnapshotShardFailure;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotRetentionConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SnapshotRetentionConfigurationTests extends ESTestCase {

    private static final String REPO = "repo";

    public void testConflictingSettings() {
        IllegalArgumentException e;
        e = expectThrows(IllegalArgumentException.class, () -> new SnapshotRetentionConfiguration(null, 0, null));
        assertThat(e.getMessage(), containsString("minimum snapshot count must be at least 1, but was: 0"));
        e = expectThrows(IllegalArgumentException.class, () -> new SnapshotRetentionConfiguration(null, -2, null));
        assertThat(e.getMessage(), containsString("minimum snapshot count must be at least 1, but was: -2"));
        e = expectThrows(IllegalArgumentException.class, () -> new SnapshotRetentionConfiguration(null, null, 0));
        assertThat(e.getMessage(), containsString("maximum snapshot count must be at least 1, but was: 0"));
        e = expectThrows(IllegalArgumentException.class, () -> new SnapshotRetentionConfiguration(null, null, -2));
        assertThat(e.getMessage(), containsString("maximum snapshot count must be at least 1, but was: -2"));
        e = expectThrows(IllegalArgumentException.class, () -> new SnapshotRetentionConfiguration(null, 3, 1));
        assertThat(e.getMessage(), containsString("minimum snapshot count 3 cannot be larger than maximum snapshot count 1"));
    }

    public void testExpireAfter() {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(
            () -> TimeValue.timeValueDays(1).millis() + 1,
            TimeValue.timeValueDays(1), null, null);
        SnapshotInfo oldInfo = makeInfo(0);
        assertThat(conf.getSnapshotDeletionPredicate(Collections.singletonList(oldInfo)).test(oldInfo), equalTo(true));

        SnapshotInfo newInfo = makeInfo(1);
        assertThat(conf.getSnapshotDeletionPredicate(Collections.singletonList(newInfo)).test(newInfo), equalTo(false));

        List<SnapshotInfo> infos = new ArrayList<>();
        infos.add(newInfo);
        infos.add(oldInfo);
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(newInfo), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(oldInfo), equalTo(true));
    }

    public void testExpiredWithMinimum() {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(() -> TimeValue.timeValueDays(1).millis() + 1,
            TimeValue.timeValueDays(1), 2, null);
        SnapshotInfo oldInfo = makeInfo(0);
        SnapshotInfo newInfo = makeInfo(1);

        List<SnapshotInfo> infos = new ArrayList<>();
        infos.add(newInfo);
        infos.add(oldInfo);
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(newInfo), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(oldInfo), equalTo(false));

        conf = new SnapshotRetentionConfiguration(() -> TimeValue.timeValueDays(1).millis() + 1,
            TimeValue.timeValueDays(1), 1, null);
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(newInfo), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(oldInfo), equalTo(true));
    }

    public void testMaximum() {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(() -> 1, null, 2, 5);
        SnapshotInfo s1 = makeInfo(1);
        SnapshotInfo s2 = makeInfo(2);
        SnapshotInfo s3 = makeInfo(3);
        SnapshotInfo s4 = makeInfo(4);
        SnapshotInfo s5 = makeInfo(5);
        SnapshotInfo s6 = makeInfo(6);
        SnapshotInfo s7 = makeInfo(7);
        SnapshotInfo s8 = makeInfo(8);
        SnapshotInfo s9 = makeInfo(9);

        List<SnapshotInfo> infos = Arrays.asList(s1 , s2, s3, s4, s5, s6, s7, s8, s9);
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s1), equalTo(true));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s2), equalTo(true));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s3), equalTo(true));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s4), equalTo(true));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s5), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s6), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s7), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s8), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s9), equalTo(false));
    }

    public void testFailuresDeletedIfExpired() {
        assertUnsuccessfulDeletedIfExpired(true);
    }

    public void testPartialsDeletedIfExpired() {
        assertUnsuccessfulDeletedIfExpired(false);
    }

    private void assertUnsuccessfulDeletedIfExpired(boolean failure) {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(
            () -> TimeValue.timeValueDays(1).millis() + 1,
            TimeValue.timeValueDays(1), null, null);
        SnapshotInfo oldInfo = makeFailureOrPartial(0, failure);
        assertThat(conf.getSnapshotDeletionPredicate(Collections.singletonList(oldInfo)).test(oldInfo), equalTo(true));

        SnapshotInfo newInfo = makeFailureOrPartial(1, failure);
        assertThat(conf.getSnapshotDeletionPredicate(Collections.singletonList(newInfo)).test(newInfo), equalTo(false));

        List<SnapshotInfo> infos = new ArrayList<>();
        infos.add(newInfo);
        infos.add(oldInfo);
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(newInfo), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(oldInfo), equalTo(true));
    }

    public void testFailuresDeletedIfNoExpiryAndMoreRecentSuccessExists() {
        assertUnsuccessfulDeletedIfNoExpiryAndMoreRecentSuccessExists(true);
    }

    public void testPartialsDeletedIfNoExpiryAndMoreRecentSuccessExists() {
        assertUnsuccessfulDeletedIfNoExpiryAndMoreRecentSuccessExists(false);
    }

    private void assertUnsuccessfulDeletedIfNoExpiryAndMoreRecentSuccessExists(boolean failure) {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(() -> 1, null, 2, 5);
        SnapshotInfo s1 = makeInfo(1);
        SnapshotInfo s2 = makeInfo(2);
        SnapshotInfo s3 = makeFailureOrPartial(3, failure);
        SnapshotInfo s4 = makeInfo(4);

        List<SnapshotInfo> infos = Arrays.asList(s1 , s2, s3, s4);
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s1), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s2), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s3), equalTo(true));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s4), equalTo(false));
    }

    public void testFailuresKeptIfNoExpiryAndNoMoreRecentSuccess() {
        assertUnsuccessfulKeptIfNoExpiryAndNoMoreRecentSuccess(true);
    }

    public void testPartialsKeptIfNoExpiryAndNoMoreRecentSuccess() {
        assertUnsuccessfulKeptIfNoExpiryAndNoMoreRecentSuccess(false);
    }

    private void assertUnsuccessfulKeptIfNoExpiryAndNoMoreRecentSuccess(boolean failure) {
        // Also tests that failures are not counted towards the maximum
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(() -> 1, null, 2, 3);
        SnapshotInfo s1 = makeInfo(1);
        SnapshotInfo s2 = makeInfo(2);
        SnapshotInfo s3 = makeInfo(3);
        SnapshotInfo s4 = makeFailureOrPartial(4, failure);

        List<SnapshotInfo> infos = Arrays.asList(s1 , s2, s3, s4);
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s1), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s2), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s3), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s4), equalTo(false));
    }

    public void testFailuresNotCountedTowardsMaximum() {
        assertUnsuccessfulNotCountedTowardsMaximum(true);
    }

    public void testPartialsNotCountedTowardsMaximum() {
        assertUnsuccessfulNotCountedTowardsMaximum(false);
    }

    private void assertUnsuccessfulNotCountedTowardsMaximum(boolean failure) {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(() -> 1, TimeValue.timeValueDays(1), 2, 2);
        SnapshotInfo s1 = makeInfo(1);
        SnapshotInfo s2 = makeFailureOrPartial(2, failure);
        SnapshotInfo s3 = makeFailureOrPartial(3, failure);
        SnapshotInfo s4 = makeFailureOrPartial(4, failure);
        SnapshotInfo s5 = makeInfo(5);

        List<SnapshotInfo> infos = Arrays.asList(s1 , s2, s3, s4, s5);
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s1), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s2), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s3), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s4), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s5), equalTo(false));
    }

    public void testFailuresNotCountedTowardsMinimum() {
        assertUnsuccessfulNotCountedTowardsMinimum(true);
    }

    public void testPartialsNotCountedTowardsMinimum() {
        assertUnsuccessfulNotCountedTowardsMinimum(false);
    }

    private void assertUnsuccessfulNotCountedTowardsMinimum(boolean failure) {
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(() -> TimeValue.timeValueDays(1).millis() + 1,
            TimeValue.timeValueDays(1), 2, null);
        SnapshotInfo oldInfo = makeInfo(0);
        SnapshotInfo failureInfo = makeFailureOrPartial(1, failure);
        SnapshotInfo newInfo = makeInfo(2);

        List<SnapshotInfo> infos = new ArrayList<>();
        infos.add(newInfo);
        infos.add(failureInfo);
        infos.add(oldInfo);
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(newInfo), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(failureInfo), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(oldInfo), equalTo(false));

        conf = new SnapshotRetentionConfiguration(() -> TimeValue.timeValueDays(1).millis() + 2,
            TimeValue.timeValueDays(1), 1, null);
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(newInfo), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(failureInfo), equalTo(true));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(oldInfo), equalTo(true));
    }


    public void testMostRecentSuccessfulTimestampIsUsed() {
        boolean failureBeforePartial = randomBoolean();
        SnapshotRetentionConfiguration conf = new SnapshotRetentionConfiguration(() -> 1, null, 2, 2);
        SnapshotInfo s1 = makeInfo(1);
        SnapshotInfo s2 = makeInfo(2);
        SnapshotInfo s3 = makeFailureOrPartial(3, failureBeforePartial);
        SnapshotInfo s4 = makeFailureOrPartial(4, failureBeforePartial == false);

        List<SnapshotInfo> infos = Arrays.asList(s1 , s2, s3, s4);
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s1), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s2), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s3), equalTo(false));
        assertThat(conf.getSnapshotDeletionPredicate(infos).test(s4), equalTo(false));
    }

    private SnapshotInfo makeInfo(long startTime) {
        final Map<String, Object> meta = new HashMap<>();
        meta.put(SnapshotLifecyclePolicy.POLICY_ID_METADATA_FIELD, REPO);
        final int totalShards = between(1,20);
        SnapshotInfo snapInfo = new SnapshotInfo(new SnapshotId("snap-" + randomAlphaOfLength(3), "uuid"),
            Collections.singletonList("foo"),
            startTime,
            null,
            startTime + between(1, 10000),
            totalShards,
            new ArrayList<>(),
            false,
            meta);
        assertThat(snapInfo.state(), equalTo(SnapshotState.SUCCESS));
        return snapInfo;
    }

    private SnapshotInfo makeFailureOrPartial(long startTime, boolean failure) {
        if (failure) {
            return makeFailureInfo(startTime);
        } else {
            return makePartialInfo(startTime);
        }
    }

    private SnapshotInfo makeFailureInfo(long startTime) {
        final Map<String, Object> meta = new HashMap<>();
        meta.put(SnapshotLifecyclePolicy.POLICY_ID_METADATA_FIELD, REPO);
        final int totalShards = between(1,20);
        final List<SnapshotShardFailure> failures = new ArrayList<>();
        final int failureCount = between(1,totalShards);
        for (int i = 0; i < failureCount; i++) {
            failures.add(new SnapshotShardFailure("nodeId", new ShardId("index-name", "index-uuid", i), "failed"));
        }
        assert failureCount == failures.size();
        SnapshotInfo snapInfo = new SnapshotInfo(new SnapshotId("snap-fail-" + randomAlphaOfLength(3), "uuid-fail"),
            Collections.singletonList("foo-fail"),
            startTime,
            "forced-failure",
            startTime + between(1, 10000),
            totalShards,
            failures,
            randomBoolean(),
            meta);
        assertThat(snapInfo.state(), equalTo(SnapshotState.FAILED));
        return snapInfo;
    }

    private SnapshotInfo makePartialInfo(long startTime) {
        final Map<String, Object> meta = new HashMap<>();
        meta.put(SnapshotLifecyclePolicy.POLICY_ID_METADATA_FIELD, REPO);
        final int totalShards = between(2,20);
        final List<SnapshotShardFailure> failures = new ArrayList<>();
        final int failureCount = between(1,totalShards - 1);
        for (int i = 0; i < failureCount; i++) {
            failures.add(new SnapshotShardFailure("nodeId", new ShardId("index-name", "index-uuid", i), "failed"));
        }
        assert failureCount == failures.size();
        SnapshotInfo snapInfo = new SnapshotInfo(new SnapshotId("snap-fail-" + randomAlphaOfLength(3), "uuid-fail"),
            Collections.singletonList("foo-fail"),
            startTime,
            null,
            startTime + between(1, 10000),
            totalShards,
            failures,
            randomBoolean(),
            meta);
        assertThat(snapInfo.state(), equalTo(SnapshotState.PARTIAL));
        return snapInfo;
    }
}
