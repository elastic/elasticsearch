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

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Snapshot status response
 */
public class SnapshotsStatusResponse extends ActionResponse implements ToXContentObject {

    private List<SnapshotStatus> snapshots = Collections.emptyList();

    SnapshotsStatusResponse() {
    }

    SnapshotsStatusResponse(List<SnapshotStatus> snapshots) {
        this.snapshots = snapshots;
    }

    /**
     * Returns the list of snapshots
     *
     * @return the list of snapshots
     */
    public List<SnapshotStatus> getSnapshots() {
        return snapshots;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        List<SnapshotStatus> builder = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            builder.add(SnapshotStatus.readSnapshotStatus(in));
        }
        snapshots = Collections.unmodifiableList(builder);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(snapshots.size());
        for (SnapshotStatus snapshotInfo : snapshots) {
            snapshotInfo.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startArray("snapshots");
        for (SnapshotStatus snapshot : snapshots) {
            snapshot.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

}
