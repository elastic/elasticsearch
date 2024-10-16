/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.snapshots.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.snapshots.SnapshotInfo;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Get snapshots response
 */
public class GetSnapshotsResponse extends ActionResponse implements ChunkedToXContentObject {

    private final List<SnapshotInfo> snapshots;

    @UpdateForV9(owner = UpdateForV9.Owner.DISTRIBUTED_COORDINATION) // always empty, can be dropped
    private final Map<String, ElasticsearchException> failures;

    @Nullable
    private final String next;

    private final int total;

    private final int remaining;

    public GetSnapshotsResponse(
        List<SnapshotInfo> snapshots,
        Map<String, ElasticsearchException> failures,
        @Nullable String next,
        final int total,
        final int remaining
    ) {
        this.snapshots = List.copyOf(snapshots);
        this.failures = failures == null ? Map.of() : Map.copyOf(failures);
        this.next = next;
        this.total = total;
        this.remaining = remaining;
    }

    public GetSnapshotsResponse(StreamInput in) throws IOException {
        this.snapshots = in.readCollectionAsImmutableList(SnapshotInfo::readFrom);
        this.failures = Collections.unmodifiableMap(in.readMap(StreamInput::readException));
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

    /**
     * Returns a map of repository name to {@link ElasticsearchException} for each unsuccessful response.
     */
    public Map<String, ElasticsearchException> getFailures() {
        return failures;
    }

    @Nullable
    public String next() {
        return next;
    }

    /**
     * Returns true if there is at least one failed response.
     */
    public boolean isFailed() {
        return failures.isEmpty() == false;
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
        out.writeMap(failures, StreamOutput::writeException);
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
            if (failures.isEmpty() == false) {
                b.startObject("failures");
                for (Map.Entry<String, ElasticsearchException> error : failures.entrySet()) {
                    b.field(error.getKey(), (bb, pa) -> {
                        bb.startObject();
                        error.getValue().toXContent(bb, pa);
                        bb.endObject();
                        return bb;
                    });
                }
                b.endObject();
            }
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
        return Objects.equals(snapshots, that.snapshots) && Objects.equals(failures, that.failures) && Objects.equals(next, that.next);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshots, failures, next);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
