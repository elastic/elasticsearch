/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Get snapshots response
 */
public class GetSnapshotsResponse extends ActionResponse implements ChunkedToXContentObject {

    private final List<SnapshotInfo> snapshots;

    @Nullable
    private final String next;

    private final int total;

    private final int remaining;

    public GetSnapshotsResponse(List<SnapshotInfo> snapshots, @Nullable String next, final int total, final int remaining) {
        this.snapshots = List.copyOf(snapshots);
        this.next = next;
        this.total = total;
        this.remaining = remaining;
    }

    public GetSnapshotsResponse(StreamInput in) throws IOException {
        this.snapshots = in.readCollectionAsImmutableList(SnapshotInfo::readFrom);
        if (in.getTransportVersion().before(TransportVersions.REMOVE_SNAPSHOT_FAILURES)
            && in.getTransportVersion().isPatchFrom(TransportVersions.V_9_0_0) == false) {
            // Deprecated `failures` field
            in.readMap(StreamInput::readException);
        }
        this.next = in.readOptionalString();
        this.total = in.readVInt();
        this.remaining = in.readVInt();
    }

    /**
     * Returns the list of snapshots
     *
     * @return the list of snapshots
     */
    public List<SnapshotInfo> getSnapshots() {
        return snapshots;
    }

    @Nullable
    public String next() {
        return next;
    }

    public int totalCount() {
        return total;
    }

    public int remaining() {
        return remaining;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(snapshots);
        if (out.getTransportVersion().before(TransportVersions.REMOVE_SNAPSHOT_FAILURES)
            && out.getTransportVersion().isPatchFrom(TransportVersions.V_9_0_0) == false) {
            // Deprecated `failures` field
            out.writeMap(Map.of(), StreamOutput::writeException);
        }
        out.writeOptionalString(next);
        out.writeVInt(total);
        out.writeVInt(remaining);
    }

    @Override
    public Iterator<ToXContent> toXContentChunked(ToXContent.Params params) {
        return Iterators.concat(Iterators.single((b, p) -> {
            b.startObject();
            b.startArray("snapshots");
            return b;
        }), Iterators.map(getSnapshots().iterator(), snapshotInfo -> snapshotInfo::toXContentExternal), Iterators.single((b, p) -> {
            b.endArray();
            if (next != null) {
                b.field("next", next);
            }
            if (total >= 0) {
                b.field("total", total);
            }
            if (remaining >= 0) {
                b.field("remaining", remaining);
            }
            b.endObject();
            return b;
        }));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetSnapshotsResponse that = (GetSnapshotsResponse) o;
        return Objects.equals(snapshots, that.snapshots) && Objects.equals(next, that.next);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshots, next);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
