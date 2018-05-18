/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
