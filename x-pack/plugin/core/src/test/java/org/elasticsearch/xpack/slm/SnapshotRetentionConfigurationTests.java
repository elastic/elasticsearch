/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;
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

    private SnapshotInfo makeInfo(long startTime) {
        final Map<String, Object> meta = new HashMap<>();
        meta.put(SnapshotLifecyclePolicy.POLICY_ID_METADATA_FIELD, REPO);
        return new SnapshotInfo(new SnapshotId("snap-" + randomAlphaOfLength(3), "uuid"),
            Collections.singletonList("foo"), startTime, false, meta);
    }
}
