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

import org.elasticsearch.Version;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.transport.RemoteClusterAware;

import java.io.IOException;

/**
 * The target that the search request was executed on.
 */
public final class SearchShardTarget implements Writeable, Comparable<SearchShardTarget> {

    private final Text nodeId;
    private final ShardId shardId;
    //original indices and cluster alias are only needed in the coordinating node throughout the search request execution.
    //no need to serialize them as part of SearchShardTarget.
    private final transient OriginalIndices originalIndices;
    private final String clusterAlias;

    public SearchShardTarget(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            nodeId = in.readText();
        } else {
            nodeId = null;
        }
        shardId = ShardId.readShardId(in);
        this.originalIndices = null;
        if (in.getVersion().onOrAfter(Version.V_5_6_0)) {
            clusterAlias = in.readOptionalString();
        } else {
            clusterAlias = null;
        }
    }

    public SearchShardTarget(String nodeId, ShardId shardId, String clusterAlias, OriginalIndices originalIndices) {
        this.nodeId = nodeId == null ? null : new Text(nodeId);
        this.shardId = shardId;
        this.originalIndices = originalIndices;
        this.clusterAlias = clusterAlias;
    }

    //this constructor is only used in tests
    public SearchShardTarget(String nodeId, Index index, int shardId, String clusterAlias) {
        this(nodeId,  new ShardId(index, shardId), clusterAlias, OriginalIndices.NONE);
    }

    @Nullable
    public String getNodeId() {
        return nodeId.string();
    }

    public Text getNodeIdText() {
        return this.nodeId;
    }

    public String getIndex() {
        return shardId.getIndexName();
    }

    public ShardId getShardId() {
        return shardId;
    }

    public OriginalIndices getOriginalIndices() {
        return originalIndices;
    }

    public String getClusterAlias() {
        return clusterAlias;
    }

    @Override
    public int compareTo(SearchShardTarget o) {
        int i = shardId.getIndexName().compareTo(o.getIndex());
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
        if (out.getVersion().onOrAfter(Version.V_5_6_0)) {
            out.writeOptionalString(clusterAlias);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchShardTarget that = (SearchShardTarget) o;
        if (shardId.equals(that.shardId) == false) return false;
        if (nodeId != null ? !nodeId.equals(that.nodeId) : that.nodeId != null) return false;
        if (clusterAlias != null ? !clusterAlias.equals(that.clusterAlias) : that.clusterAlias != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = nodeId != null ? nodeId.hashCode() : 0;
        result = 31 * result + (shardId.getIndexName() != null ? shardId.getIndexName().hashCode() : 0);
        result = 31 * result + shardId.hashCode();
        result = 31 * result + (clusterAlias != null ? clusterAlias.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        String shardToString = "[" + RemoteClusterAware.buildRemoteIndexName(clusterAlias, shardId.getIndexName()) + "][" + shardId.getId()
            + "]";
        if (nodeId == null) {
            return "[_na_]" + shardToString;
        }
        return "[" + nodeId + "]" + shardToString;
    }
}
