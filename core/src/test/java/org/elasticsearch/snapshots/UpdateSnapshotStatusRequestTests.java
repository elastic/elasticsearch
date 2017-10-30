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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;

public class UpdateSnapshotStatusRequestTests extends ESTestCase {

    public void testSerializeForOlderVersion() throws Exception {
        UpdateSnapshotStatusRequest originalRequest = new UpdateSnapshotStatusRequest(randomSnapshot(), randomShardId(), randomStatus());
        Version bwcVersion = randomFrom(Version.V_5_1_2, Version.V_5_4_1, Version.V_6_0_0_alpha1);
        final UpdateSnapshotStatusRequest cloneRequest = new UpdateSnapshotStatusRequest();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(bwcVersion);
            originalRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(bwcVersion);
                cloneRequest.readFrom(in);
            }
        }
        assertThat(cloneRequest.snapshot(), equalTo(originalRequest.snapshot()));
        assertThat(cloneRequest.shardId(), equalTo(originalRequest.shardId()));
        assertThat(cloneRequest.status(), equalTo(originalRequest.status()));
    }

    public void testSerializeForCurrentVersion() throws Exception {
        UpdateSnapshotStatusRequest originalRequest = new UpdateSnapshotStatusRequest(randomSnapshot(), randomShardId(), randomStatus());
        final UpdateSnapshotStatusRequest cloneRequest = new UpdateSnapshotStatusRequest();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            originalRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                cloneRequest.readFrom(in);
            }
        }
        assertThat(cloneRequest.snapshot(), equalTo(originalRequest.snapshot()));
        assertThat(cloneRequest.shardId(), equalTo(originalRequest.shardId()));
        assertThat(cloneRequest.status(), equalTo(originalRequest.status()));
    }

    private ShardId randomShardId() {
        return new ShardId(new Index(randomAlphaOfLength(10), UUID.randomUUID().toString()), between(0, 1000));
    }

    private Snapshot randomSnapshot() {
        SnapshotId snapshotId = new SnapshotId(randomAlphaOfLength(10), UUID.randomUUID().toString());
        return new Snapshot(randomAlphaOfLength(10), snapshotId);
    }

    private SnapshotsInProgress.ShardSnapshotStatus randomStatus() {
        return new SnapshotsInProgress.ShardSnapshotStatus("node-" + between(0, 100),
            randomFrom(SnapshotsInProgress.State.values()), randomAlphaOfLength(20));
    }
}
