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
import java.util.Objects;

/**
 * The target that the search request was executed on.
 */
public final class SearchShardTarget implements Writeable, Comparable<SearchShardTarget> {

    private final Text nodeId;
    private final ShardId shardId;
    //original indices are only needed in the coordinating node throughout the search request execution.
    //no need to serialize them as part of SearchShardTarget.
    private final transient OriginalIndices originalIndices;
    private final CCSInfo ccsInfo;

    public SearchShardTarget(StreamInput in) throws IOException {
        if (in.readBoolean()) {
            nodeId = in.readText();
        } else {
            nodeId = null;
        }
        shardId = ShardId.readShardId(in);
        this.originalIndices = null;
        ccsInfo = CCSInfo.read(in);
    }

    public SearchShardTarget(String nodeId, ShardId shardId, @Nullable CCSInfo ccsInfo, OriginalIndices originalIndices) {
        this.nodeId = nodeId == null ? null : new Text(nodeId);
        this.shardId = shardId;
        this.originalIndices = originalIndices;
        this.ccsInfo = ccsInfo;
    }

    //this constructor is only used in tests
    public SearchShardTarget(String nodeId, Index index, int shardId, String clusterAlias) {
        this(nodeId, new ShardId(index, shardId), CCSInfo.fromClusterAlias(clusterAlias), OriginalIndices.NONE);
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

    /**
     * Returns the prefix to be used for the index names of each search result coming from this shard target, to indicate which cluster
     * returned it. <code>null</code> means that no prefix is needed, meaning that we are not executing a cross-cluster search request.
     */
    @Nullable
    public String getHitIndexPrefix() {
        return ccsInfo == null ? null : ccsInfo.getHitIndexPrefix();
    }

    /**
     * Returns the cluster alias needed to lookup the connection when sending shard level requests in the context of this shard target.
     * <code>null</code> indicates that the shard is local, meaning that either we are not executing a cross-cluster search request,
     * or we are but each cluster performs its own reduction.
     */
    @Nullable
    public String getConnectionAlias() {
        return ccsInfo == null ? null : ccsInfo.getConnectionAlias();
    }

    /**
     * Returns the fully qualified index name, including the index prefix that indicates which cluster results come from.
     */
    public String getFullyQualifiedIndexName() {
        return RemoteClusterAware.buildRemoteIndexName(getHitIndexPrefix(), getIndex());
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
        CCSInfo.write(ccsInfo, out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SearchShardTarget that = (SearchShardTarget) o;
        return Objects.equals(nodeId, that.nodeId) &&
            Objects.equals(shardId, that.shardId) &&
            Objects.equals(ccsInfo, that.ccsInfo);
    }

    @Override
    public int hashCode() {
        return Objects.hash(nodeId, shardId, ccsInfo);
    }

    @Override
    public String toString() {
        String shardToString = "[" + RemoteClusterAware.buildRemoteIndexName(
            getHitIndexPrefix(), shardId.getIndexName()) + "][" + shardId.getId() + "]";
        if (nodeId == null) {
            return "[_na_]" + shardToString;
        }
        return "[" + nodeId + "]" + shardToString;
    }
}
