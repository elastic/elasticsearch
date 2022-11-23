/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.StreamSupport;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Snapshot status response
 */
public class SnapshotsStatusResponse extends ActionResponse implements ChunkedToXContent {

    private final List<SnapshotStatus> snapshots;

    public SnapshotsStatusResponse(StreamInput in) throws IOException {
        super(in);
        snapshots = in.readImmutableList(SnapshotStatus::new);
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
        out.writeList(snapshots);
    }

    private static final ConstructingObjectParser<SnapshotsStatusResponse, Void> PARSER = new ConstructingObjectParser<>(
        "snapshots_status_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
            List<SnapshotStatus> snapshots = (List<SnapshotStatus>) parsedObjects[0];
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

        return Objects.equals(snapshots, ((SnapshotsStatusResponse) o).snapshots);
    }

    @Override
    public int hashCode() {
        return snapshots != null ? snapshots.hashCode() : 0;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked() {
        return Iterators.concat(
            Iterators.single((ToXContent) (b, p) -> b.startObject().startArray("snapshots")),
            snapshots.stream()
                .flatMap(s -> StreamSupport.stream(Spliterators.spliteratorUnknownSize(s.toXContentChunked(), Spliterator.ORDERED), false))
                .iterator(),
            Iterators.single((b, p) -> b.endArray().endObject())
        );
    }
}
