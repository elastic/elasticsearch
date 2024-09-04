/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Allows for shard level components to be injected with the shard id.
 */
public class ShardId implements Comparable<ShardId>, ToXContentFragment, Writeable {

    private final Index index;
    private final int shardId;
    private final int hashCode;

    public ShardId(Index index, int shardId) {
        this.index = Objects.requireNonNull(index);
        this.shardId = shardId;
        this.hashCode = computeHashCode();
    }

    public ShardId(String index, String indexUUID, int shardId) {
        this(new Index(index, indexUUID), shardId);
    }

    public ShardId(StreamInput in) throws IOException {
        index = new Index(in);
        shardId = in.readVInt();
        hashCode = computeHashCode();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        index.writeTo(out);
        out.writeVInt(shardId);
    }

    public Index getIndex() {
        return index;
    }

    public String getIndexName() {
        return index.getName();
    }

    public int id() {
        return this.shardId;
    }

    public int getId() {
        return id();
    }

    @Override
    public String toString() {
        return "[" + index.getName() + "][" + shardId + "]";
    }

    /**
     * Parse the string representation of this shardId back to an object.
     * We lose index uuid information here, but since we use toString in
     * rest responses, this is the best we can do to reconstruct the object
     * on the client side.
     */
    public static ShardId fromString(String shardIdString) {
        int splitPosition = shardIdString.indexOf("][");
        if (splitPosition <= 0 || shardIdString.charAt(0) != '[' || shardIdString.charAt(shardIdString.length() - 1) != ']') {
            throw new IllegalArgumentException("Unexpected shardId string format, expected [indexName][shardId] but got " + shardIdString);
        }
        String indexName = shardIdString.substring(1, splitPosition);
        int shardId = Integer.parseInt(shardIdString.substring(splitPosition + 2, shardIdString.length() - 1));
        return new ShardId(new Index(indexName, IndexMetadata.INDEX_UUID_NA_VALUE), shardId);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShardId shardId1 = (ShardId) o;
        return shardId == shardId1.shardId && index.equals(shardId1.index);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int computeHashCode() {
        int result = index != null ? index.hashCode() : 0;
        result = 31 * result + shardId;
        return result;
    }

    @Override
    public int compareTo(ShardId o) {
        final int res = Integer.compare(shardId, o.shardId);
        if (res != 0) {
            return res;
        }
        final Index index = this.index;
        final Index otherIndex = o.index;
        int compare = index.getName().compareTo(otherIndex.getName());
        if (compare != 0) {
            return compare;
        }
        return index.getUUID().compareTo(otherIndex.getUUID());
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.value(toString());
    }
}
