/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotRequest;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotRequest;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.equalTo;

/**
 * Tests for the {@link Snapshot} class.
 */
public class SnapshotTests extends ESTestCase {

    public void testSnapshotEquals() {
        final SnapshotId snapshotId = new SnapshotId("snap", UUIDs.randomBase64UUID());
        final Snapshot original = new Snapshot("repo", snapshotId);
        final Snapshot expected = new Snapshot(original.getRepository(), original.getSnapshotId());
        assertThat(expected, equalTo(original));
        assertThat(expected.getRepository(), equalTo(original.getRepository()));
        assertThat(expected.getSnapshotId(), equalTo(original.getSnapshotId()));
        assertThat(expected.getSnapshotId().getName(), equalTo(original.getSnapshotId().getName()));
        assertThat(expected.getSnapshotId().getUUID(), equalTo(original.getSnapshotId().getUUID()));
    }

    public void testSerialization() throws IOException {
        final SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(randomIntBetween(2, 8)), UUIDs.randomBase64UUID());
        final Snapshot original = new Snapshot(randomAlphaOfLength(randomIntBetween(2, 8)), snapshotId);
        final BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        assertThat(new Snapshot(out.bytes().streamInput()), equalTo(original));
    }

    public void testCreateSnapshotRequestDescrptions() {
        CreateSnapshotRequest createSnapshotRequest = new CreateSnapshotRequest();
        createSnapshotRequest.snapshot("snapshot_name");
        createSnapshotRequest.repository("repo_name");
        assertEquals("snapshot [repo_name:snapshot_name]", createSnapshotRequest.getDescription());
    }

    public void testRestoreSnapshotRequestDescrptions() {
        RestoreSnapshotRequest restoreSnapshotRequest = new RestoreSnapshotRequest();
        restoreSnapshotRequest.snapshot("snapshot_name");
        restoreSnapshotRequest.repository("repo_name");
        assertEquals("snapshot [repo_name:snapshot_name]", restoreSnapshotRequest.getDescription());
    }

}
