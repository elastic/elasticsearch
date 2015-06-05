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

import com.google.common.collect.ImmutableList;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;

/**
 * Snapshot status response
 */
public class SnapshotsStatusResponse extends ActionResponse implements ToXContent {

    private ImmutableList<SnapshotStatus> snapshots = ImmutableList.of();

    SnapshotsStatusResponse() {
    }

    SnapshotsStatusResponse(ImmutableList<SnapshotStatus> snapshots) {
        this.snapshots = snapshots;
    }

    /**
     * Returns the list of snapshots
     *
     * @return the list of snapshots
     */
    public ImmutableList<SnapshotStatus> getSnapshots() {
        return snapshots;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int size = in.readVInt();
        ImmutableList.Builder<SnapshotStatus> builder = ImmutableList.builder();
        for (int i = 0; i < size; i++) {
            builder.add(SnapshotStatus.readSnapshotStatus(in));
        }
        snapshots = builder.build();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(snapshots.size());
        for (SnapshotStatus snapshotInfo : snapshots) {
            snapshotInfo.writeTo(out);
        }
    }

    static final class Fields {
        static final XContentBuilderString SNAPSHOTS = new XContentBuilderString("snapshots");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startArray(Fields.SNAPSHOTS);
        for (SnapshotStatus snapshot : snapshots) {
            snapshot.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

}
