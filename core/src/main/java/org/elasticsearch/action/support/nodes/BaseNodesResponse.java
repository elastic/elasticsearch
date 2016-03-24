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

package org.elasticsearch.action.support.nodes;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public abstract class BaseNodesResponse<TNodeResponse extends BaseNodeResponse> extends ActionResponse implements Iterable<TNodeResponse> {

    private ClusterName clusterName;
    protected TNodeResponse[] nodes;
    private Map<String, TNodeResponse> nodesMap;

    protected BaseNodesResponse() {
    }

    protected BaseNodesResponse(ClusterName clusterName, TNodeResponse[] nodes) {
        this.clusterName = clusterName;
        this.nodes = nodes;
    }

    /**
     * The failed nodes, if set to be captured.
     */
    @Nullable
    public FailedNodeException[] failures() {
        return null;
    }

    public ClusterName getClusterName() {
        return this.clusterName;
    }

    public String getClusterNameAsString() {
        return this.clusterName.value();
    }

    public TNodeResponse[] getNodes() {
        return nodes;
    }

    public TNodeResponse getAt(int position) {
        return nodes[position];
    }

    @Override
    public Iterator<TNodeResponse> iterator() {
        return getNodesMap().values().iterator();
    }

    public Map<String, TNodeResponse> getNodesMap() {
        if (nodesMap == null) {
            nodesMap = new HashMap<>();
            for (TNodeResponse nodeResponse : nodes) {
                nodesMap.put(nodeResponse.getNode().id(), nodeResponse);
            }
        }
        return nodesMap;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        clusterName = ClusterName.readClusterName(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        clusterName.writeTo(out);
    }
}
