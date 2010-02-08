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

package org.elasticsearch.action.support.nodes;

import com.google.common.collect.Maps;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

/**
 * @author kimchy (Shay Banon)
 */
public abstract class NodesOperationResponse<NodeResponse extends NodeOperationResponse> implements ActionResponse, Iterable<NodeResponse> {

    private ClusterName clusterName;

    protected NodeResponse[] nodes;

    private Map<String, NodeResponse> nodesMap;

    protected NodesOperationResponse() {
    }

    protected NodesOperationResponse(ClusterName clusterName, NodeResponse[] nodes) {
        this.clusterName = clusterName;
        this.nodes = nodes;
    }

    public ClusterName clusterName() {
        return this.clusterName;
    }

    public NodeResponse[] nodes() {
        return nodes;
    }

    @Override public Iterator<NodeResponse> iterator() {
        return nodesMap().values().iterator();
    }

    public Map<String, NodeResponse> nodesMap() {
        if (nodesMap == null) {
            nodesMap = Maps.newHashMap();
            for (NodeResponse nodeResponse : nodes) {
                nodesMap.put(nodeResponse.node().id(), nodeResponse);
            }
        }
        return nodesMap;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        clusterName = ClusterName.readClusterName(in);
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        clusterName.writeTo(out);
    }
}
