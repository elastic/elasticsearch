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
package org.elasticsearch.action.admin.cluster.configuration;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfigExclusion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A request to add voting config exclusions for certain master-eligible nodes, and wait for these nodes to be removed from the voting
 * configuration.
 */
public class AddVotingConfigExclusionsRequest extends MasterNodeRequest<AddVotingConfigExclusionsRequest> {
    private final String[] nodeDescriptions;
    private final TimeValue timeout;

    /**
     * Construct a request to add voting config exclusions for master-eligible nodes matching the given descriptions, and wait for a
     * default 30 seconds for these exclusions to take effect, removing the nodes from the voting configuration.
     * @param nodeDescriptions Descriptions of the nodes to add - see {@link DiscoveryNodes#resolveNodes(String...)}
     */
    public AddVotingConfigExclusionsRequest(String[] nodeDescriptions) {
        this(nodeDescriptions, TimeValue.timeValueSeconds(30));
    }

    /**
     * Construct a request to add voting config exclusions for master-eligible nodes matching the given descriptions, and wait for these
     * nodes to be removed from the voting configuration.
     * @param nodeDescriptions Descriptions of the nodes whose exclusions to add - see {@link DiscoveryNodes#resolveNodes(String...)}.
     * @param timeout How long to wait for the added exclusions to take effect and be removed from the voting configuration.
     */
    public AddVotingConfigExclusionsRequest(String[] nodeDescriptions, TimeValue timeout) {
        if (timeout.compareTo(TimeValue.ZERO) < 0) {
            throw new IllegalArgumentException("timeout [" + timeout + "] must be non-negative");
        }
        this.nodeDescriptions = nodeDescriptions;
        this.timeout = timeout;
    }

    public AddVotingConfigExclusionsRequest(StreamInput in) throws IOException {
        super(in);
        nodeDescriptions = in.readStringArray();
        timeout = in.readTimeValue();
    }

    Set<VotingConfigExclusion> resolveVotingConfigExclusions(ClusterState currentState) {
        final DiscoveryNodes allNodes = currentState.nodes();
        final Set<VotingConfigExclusion> resolvedNodes = Arrays.stream(allNodes.resolveNodes(nodeDescriptions))
                .map(allNodes::get).filter(DiscoveryNode::isMasterNode).map(VotingConfigExclusion::new).collect(Collectors.toSet());

        if (resolvedNodes.isEmpty()) {
            throw new IllegalArgumentException("add voting config exclusions request for " + Arrays.asList(nodeDescriptions)
                + " matched no master-eligible nodes");
        }

        resolvedNodes.removeIf(n -> currentState.getVotingConfigExclusions().contains(n));
        return resolvedNodes;
    }

    Set<VotingConfigExclusion> resolveVotingConfigExclusionsAndCheckMaximum(ClusterState currentState, int maxExclusionsCount,
                                                                            String maximumSettingKey) {
        final Set<VotingConfigExclusion> resolvedExclusions = resolveVotingConfigExclusions(currentState);

        final int oldExclusionsCount = currentState.getVotingConfigExclusions().size();
        final int newExclusionsCount = resolvedExclusions.size();
        if (oldExclusionsCount + newExclusionsCount > maxExclusionsCount) {
            throw new IllegalArgumentException("add voting config exclusions request for " + Arrays.asList(nodeDescriptions)
                + " would add [" + newExclusionsCount + "] exclusions to the existing [" + oldExclusionsCount
                + "] which would exceed the maximum of [" + maxExclusionsCount + "] set by ["
                + maximumSettingKey + "]");
        }
        return resolvedExclusions;
    }

    /**
     * @return descriptions of the nodes for whom to add voting config exclusions.
     */
    public String[] getNodeDescriptions() {
        return nodeDescriptions;
    }

    /**
     * @return how long to wait after adding the exclusions for the nodes to be removed from the voting configuration.
     */
    public TimeValue getTimeout() {
        return timeout;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeStringArray(nodeDescriptions);
        out.writeTimeValue(timeout);
    }

    @Override
    public String toString() {
        return "AddVotingConfigExclusionsRequest{" +
            "nodeDescriptions=" + Arrays.asList(nodeDescriptions) +
            ", timeout=" + timeout +
            '}';
    }
}
