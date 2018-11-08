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
package org.elasticsearch.action.admin.cluster.bootstrap;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.bootstrap.BootstrapConfiguration.NodeDescription;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Response to {@link GetDiscoveredNodesRequest}, containing the set of master-eligible nodes that were discovered.
 */
public class GetDiscoveredNodesResponse extends ActionResponse {
    private final Set<DiscoveryNode> nodes;

    public GetDiscoveredNodesResponse(Set<DiscoveryNode> nodes) {
        this.nodes = Collections.unmodifiableSet(new HashSet<>(nodes));
    }

    public GetDiscoveredNodesResponse(StreamInput in) throws IOException {
        super(in);
        nodes = Collections.unmodifiableSet(in.readSet(DiscoveryNode::new));
    }

    /**
     * @return the set of nodes that were discovered.
     */
    public Set<DiscoveryNode> getNodes() {
        return nodes;
    }

    /**
     * @return a bootstrap configuration constructed from the set of nodes that were discovered, in order to make a
     *         {@link BootstrapClusterRequest}.
     */
    public BootstrapConfiguration getBootstrapConfiguration() {
        return new BootstrapConfiguration(nodes.stream().map(NodeDescription::new).collect(Collectors.toList()));
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("usage of Streamable is to be replaced by Writeable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeCollection(nodes, (o, v) -> v.writeTo(o));
    }
}
