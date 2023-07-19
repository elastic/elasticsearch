/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cache.action;

import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.Iterators;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.common.xcontent.ChunkedToXContentObject;
import org.elasticsearch.xcontent.ToXContent;

import java.io.IOException;
import java.util.Iterator;
import java.util.Objects;

public class ClearBlobCacheNodeResponse extends BaseNodeResponse implements ChunkedToXContent, ChunkedToXContentObject {
    private final long timestamp;
    private final int evictions;

    public ClearBlobCacheNodeResponse(StreamInput in) throws IOException {
        super(in);
        timestamp = in.readVLong();
        evictions = in.readInt();
    }

    public ClearBlobCacheNodeResponse(DiscoveryNode node, long timestamp, int evictions) {
        super(node);
        this.timestamp = timestamp;
        this.evictions = evictions;
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params outerParams) {
        return Iterators.single((builder, params) -> {
            builder.field("node_id", getNode().getId());
            builder.field("timestamp", getTimestamp());
            builder.field("evictions", evictions);
            return builder;
        });
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(timestamp);
        out.writeInt(evictions);
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    public int getEvictions() {
        return evictions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClearBlobCacheNodeResponse that = (ClearBlobCacheNodeResponse) o;
        return Objects.equals(getNode().getId(), that.getNode().getId())
            && Objects.equals(evictions, that.evictions)
            && Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(evictions, timestamp);
    }
}
