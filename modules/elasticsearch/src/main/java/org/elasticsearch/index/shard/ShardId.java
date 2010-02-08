/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.elasticsearch.index.Index;
import org.elasticsearch.util.concurrent.Immutable;
import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Allows for shard level components to be injected with the shard id.
 *
 * @author kimchy (Shay Banon)
 */
@Immutable
public class ShardId implements Serializable, Streamable {

    private Index index;

    private int shardId;

    private ShardId() {

    }

    public ShardId(String index, int shardId) {
        this(new Index(index), shardId);
    }

    public ShardId(Index index, int shardId) {
        this.index = index;
        this.shardId = shardId;
    }

    public Index index() {
        return this.index;
    }

    public int id() {
        return this.shardId;
    }

    @Override public String toString() {
        return "Index Shard [" + index.name() + "][" + shardId + "]";
    }

    @Override public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ShardId shardId1 = (ShardId) o;

        if (shardId != shardId1.shardId) return false;
        if (index != null ? !index.equals(shardId1.index) : shardId1.index != null) return false;

        return true;
    }

    @Override public int hashCode() {
        int result = index != null ? index.hashCode() : 0;
        result = 31 * result + shardId;
        return result;
    }

    public static ShardId readShardId(DataInput in) throws IOException, ClassNotFoundException {
        ShardId shardId = new ShardId();
        shardId.readFrom(in);
        return shardId;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        index = Index.readIndexName(in);
        shardId = in.readInt();
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        index.writeTo(out);
        out.writeInt(shardId);
    }
}