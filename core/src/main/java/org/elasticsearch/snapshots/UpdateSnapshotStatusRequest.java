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
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * Internal request that is used to send changes in snapshot status to master
 */
class UpdateSnapshotStatusRequest extends MasterNodeRequest<UpdateSnapshotStatusRequest> {
    private Snapshot snapshot;
    private ShardId shardId;
    private SnapshotsInProgress.ShardSnapshotStatus status;

    UpdateSnapshotStatusRequest() {

    }

    UpdateSnapshotStatusRequest(Snapshot snapshot, ShardId shardId, SnapshotsInProgress.ShardSnapshotStatus status) {
        this.snapshot = snapshot;
        this.shardId = shardId;
        this.status = status;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        // To keep BWC, we have to deserialize a MasterNodeRequest from a TransportRequest from older versions.
        if (in.getVersion().before(Version.V_7_0_0_alpha1)) {
            super.readFromAsTransportRequest(in);
        } else {
            super.readFrom(in);
        }
        snapshot = new Snapshot(in);
        shardId = ShardId.readShardId(in);
        status = new SnapshotsInProgress.ShardSnapshotStatus(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // To keep BWC, we have to serialize a MasterNodeRequest as a TransportRequest for older versions.
        if (out.getVersion().before(Version.V_7_0_0_alpha1)) {
            super.writeToAsTransportRequest(out);
        } else {
            super.writeTo(out);
        }
        snapshot.writeTo(out);
        shardId.writeTo(out);
        status.writeTo(out);
    }

    Snapshot snapshot() {
        return snapshot;
    }

    ShardId shardId() {
        return shardId;
    }

    SnapshotsInProgress.ShardSnapshotStatus status() {
        return status;
    }

    @Override
    public String toString() {
        return snapshot + ", shardId [" + shardId + "], status [" + status.state() + "]";
    }
}
