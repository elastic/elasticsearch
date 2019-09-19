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
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Snapshot status response
 */
public class SnapshotsStatusResponse extends ActionResponse implements ToXContentObject {

    private List<SnapshotStatus> snapshots = Collections.emptyList();

    public SnapshotsStatusResponse(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        List<SnapshotStatus> builder = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            builder.add(new SnapshotStatus(in));
        }
        snapshots = Collections.unmodifiableList(builder);
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
    public void writeTo(StreamOutput out) throws IOException {
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

    private static final ConstructingObjectParser<SnapshotsStatusResponse, Void> PARSER = new ConstructingObjectParser<>(
        "snapshots_status_response", true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked") List<SnapshotStatus> snapshots = (List<SnapshotStatus>) parsedObjects[0];
            return new SnapshotsStatusResponse(snapshots);
        }
    );
    static {
        PARSER.declareObjectArray(constructorArg(), SnapshotStatus.PARSER, new ParseField("snapshots"));
    }

    public static SnapshotsStatusResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SnapshotsStatusResponse response = (SnapshotsStatusResponse) o;

        return snapshots != null ? snapshots.equals(response.snapshots) : response.snapshots == null;
    }

    @Override
    public int hashCode() {
        return snapshots != null ? snapshots.hashCode() : 0;
    }
}
