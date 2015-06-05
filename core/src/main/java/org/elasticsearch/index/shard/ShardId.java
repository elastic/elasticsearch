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

package org.elasticsearch.index.shard;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.io.Serializable;

/**
 * Allows for shard level components to be injected with the shard id.
 */
public class ShardId implements Serializable, Streamable, Comparable<ShardId> {

    private Index index;

    private int shardId;

    private int hashCode;

    private ShardId() {
    }

    public ShardId(String index, int shardId) {
        this(new Index(index), shardId);
    }

    public ShardId(Index index, int shardId) {
        this.index = index;
        this.shardId = shardId;
        this.hashCode = computeHashCode();
    }

    public Index index() {
        return this.index;
    }

    public String getIndex() {
        return index().name();
    }

    public int id() {
        return this.shardId;
    }

    public int getId() {
        return id();
    }

    @Override
    public String toString() {
        return "[" + index.name() + "][" + shardId + "]";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null) return false;
        ShardId shardId1 = (ShardId) o;
        return shardId == shardId1.shardId && index.name().equals(shardId1.index.name());
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

    public static ShardId readShardId(StreamInput in) throws IOException {
        ShardId shardId = new ShardId();
        shardId.readFrom(in);
        return shardId;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        index = Index.readIndexName(in);
        shardId = in.readVInt();
        hashCode = computeHashCode();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        index.writeTo(out);
        out.writeVInt(shardId);
    }

    @Override
    public int compareTo(ShardId o) {
        if (o.getId() == shardId) {
            return index.name().compareTo(o.getIndex());
        }
        return Integer.compare(shardId, o.getId());
    }
}