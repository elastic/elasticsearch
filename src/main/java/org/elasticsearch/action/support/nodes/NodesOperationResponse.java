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

import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 *
 */
public abstract class NodesOperationResponse<NodeResponse extends NodeOperationResponse> extends ActionResponse implements Iterable<NodeResponse> {

    private ClusterName clusterName;
    protected NodeResponse[] nodes;
    private Map<String, NodeResponse> nodesMap;

    protected NodesOperationResponse() {
    }

    protected NodesOperationResponse(ClusterName clusterName, NodeResponse[] nodes) {
        this.clusterName = clusterName;
        this.nodes = nodes;
    }

    public ClusterName getClusterName() {
        return this.clusterName;
    }

    public String getClusterNameAsString() {
        return this.clusterName.value();
    }

    public NodeResponse[] getNodes() {
        return nodes;
    }

    public NodeResponse getAt(int position) {
        return nodes[position];
    }

    @Override
    public Iterator<NodeResponse> iterator() {
        return getNodesMap().values().iterator();
    }

    public Map<String, NodeResponse> getNodesMap() {
        if (nodesMap == null) {
            nodesMap = Maps.newHashMap();
            for (NodeResponse nodeResponse : nodes) {
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
