/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Get snapshots response
 */
public class GetSnapshotsResponse extends ActionResponse implements ToXContentObject {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<GetSnapshotsResponse, Void> GET_SNAPSHOT_PARSER =
        new ConstructingObjectParser<>(GetSnapshotsResponse.class.getName(), true,
            (args) -> new GetSnapshotsResponse((List<SnapshotInfo>) args[0]));

    static {
        GET_SNAPSHOT_PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(),
            (p, c) -> SnapshotInfo.SNAPSHOT_INFO_PARSER.apply(p, c).build(), new ParseField("snapshots"));
    }

    private final List<SnapshotInfo> snapshots;

    public GetSnapshotsResponse(List<SnapshotInfo> snapshots) {
        this.snapshots = Collections.unmodifiableList(snapshots);
    }

    GetSnapshotsResponse(StreamInput in) throws IOException {
        super(in);
        snapshots = Collections.unmodifiableList(in.readList(SnapshotInfo::new));
    }

    /**
     * Returns the list of snapshots
     *
     * @return the list of snapshots
     */
    public List<SnapshotInfo> getSnapshots() {
        return snapshots;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeList(snapshots);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startArray("snapshots");
        for (SnapshotInfo snapshotInfo : snapshots) {
            snapshotInfo.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static GetSnapshotsResponse fromXContent(XContentParser parser) throws IOException {
        return GET_SNAPSHOT_PARSER.parse(parser, null);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetSnapshotsResponse that = (GetSnapshotsResponse) o;
        return Objects.equals(snapshots, that.snapshots);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshots);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
