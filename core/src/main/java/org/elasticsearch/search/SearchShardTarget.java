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

package org.elasticsearch.search;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;

import java.io.IOException;

/**
 * The target that the search request was executed on.
 */
public class SearchShardTarget implements Writeable, Comparable<SearchShardTarget> {

    private Text nodeId;
    private Text index;
    private ShardId shardId;

    public SearchShardTarget(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            nodeId = in.readText();
        }
        shardId = ShardId.readShardId(in);
        index = new Text(shardId.getIndexName());
    }

    public SearchShardTarget(String nodeId, ShardId shardId) {
        this.nodeId = nodeId == null ? null : new Text(nodeId);
        this.index = new Text(shardId.getIndexName());
        this.shardId = shardId;
    }

    public SearchShardTarget(String nodeId, Index index, int shardId) {
        this(nodeId,  new ShardId(index, shardId));
    }

    @Nullable
    public String nodeId() {
        return nodeId.string();
    }

    @Nullable
    public String getNodeId() {
        return nodeId();
    }

    public Text nodeIdText() {
        return this.nodeId;
    }

    public String index() {
        return index.string();
    }

    public String getIndex() {
        return index();
    }

    public Text indexText() {
        return this.index;
    }

    public ShardId shardId() {
        return shardId;
    }

    public ShardId getShardId() {
        return shardId;
    }

    @Override
    public int compareTo(SearchShardTarget o) {
        int i = index.string().compareTo(o.index());
        if (i == 0) {
            i = shardId.getId() - o.shardId.id();
        }
        return i;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (nodeId == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeText(nodeId);
        }
        shardId.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchShardTarget that = (SearchShardTarget) o;
        if (shardId.equals(that.shardId) == false) return false;
        if (nodeId != null ? !nodeId.equals(that.nodeId) : that.nodeId != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = nodeId != null ? nodeId.hashCode() : 0;
        result = 31 * result + (index != null ? index.hashCode() : 0);
        result = 31 * result + shardId.hashCode();
        return result;
    }

    @Override
    public String toString() {
        if (nodeId == null) {
            return "[_na_]" + shardId;
        }
        return "[" + nodeId + "]" + shardId;
    }
}
