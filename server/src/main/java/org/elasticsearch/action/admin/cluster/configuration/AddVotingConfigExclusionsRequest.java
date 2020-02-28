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

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfigExclusion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.iterable.Iterables;
import org.elasticsearch.common.util.set.Sets;

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
    private String[] nodeIds = null;
    private String[] nodeNames = null;
    private final TimeValue timeout;

    /**
     * Construct a request to add voting config exclusions for master-eligible nodes matching the given descriptions, and wait for a
     * default 30 seconds for these exclusions to take effect, removing the nodes from the voting configuration.
     * @param nodeDescriptions Descriptions of the nodes to add - see {@link DiscoveryNodes#resolveNodes(String...)}
     */
    public AddVotingConfigExclusionsRequest(String[] nodeDescriptions) {
        this(nodeDescriptions, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY, TimeValue.timeValueSeconds(30));
    }

    /**
     * Construct a request to add voting config exclusions for master-eligible nodes matching the given descriptions, and wait for these
     * nodes to be removed from the voting configuration.
     * @param nodeDescriptions Descriptions of the nodes whose exclusions to add - see {@link DiscoveryNodes#resolveNodes(String...)}.
     * @param timeout How long to wait for the added exclusions to take effect and be removed from the voting configuration.
     */
    public AddVotingConfigExclusionsRequest(String[] nodeDescriptions, String[] nodeIds, String[] nodeNames, TimeValue timeout) {
        if (timeout.compareTo(TimeValue.ZERO) < 0) {
            throw new IllegalArgumentException("timeout [" + timeout + "] must be non-negative");
        }
        this.nodeDescriptions = nodeDescriptions;
        this.nodeIds = nodeIds;
        this.nodeNames = nodeNames;
        this.timeout = timeout;
    }

    public AddVotingConfigExclusionsRequest(StreamInput in) throws IOException {
        super(in);
        // TODO should this be removed in the latest version where nodeIds and nodeNames are used?
        nodeDescriptions = in.readStringArray();
        // TODO which version to use here?
        if (in.getVersion() == Version.V_EMPTY) {
            nodeIds = in.readStringArray();
            nodeNames = in.readStringArray();
        }
        timeout = in.readTimeValue();
    }

    Set<VotingConfigExclusion> resolveVotingConfigExclusions(ClusterState currentState) {
        final DiscoveryNodes allNodes = currentState.nodes();
        Set<VotingConfigExclusion> allProcessedNodes = null;

        if (nodeDescriptions.length >= 1) {
            allProcessedNodes = Arrays.stream(allNodes.resolveNodes(nodeDescriptions)).map(allNodes::get)
                                    .filter(DiscoveryNode::isMasterNode).map(VotingConfigExclusion::new).collect(Collectors.toSet());

            if (allProcessedNodes.isEmpty()) {
                throw new IllegalArgumentException("add voting config exclusions request for " + Arrays.asList(nodeDescriptions)
                    + " matched no master-eligible nodes");
            }
        }
        else {
            DiscoveryNodes.NodeResolutionResults nodeResolutionResults;
            Set<VotingConfigExclusion> resolvedNodes;
            Set<VotingConfigExclusion> unresolvedNodes;

            if (nodeIds.length >= 1) {
                nodeResolutionResults = allNodes.resolveNodesExact(true, nodeIds);

                unresolvedNodes = Arrays.stream(nodeResolutionResults.getUnresolvedNodes())
                    .map(nodeId -> new VotingConfigExclusion(nodeId, "")).collect(Collectors.toSet());
            }
            else {
                nodeResolutionResults = allNodes.resolveNodesExact(false, nodeNames);

                unresolvedNodes = Arrays.stream(nodeResolutionResults.getUnresolvedNodes())
                    .map(nodeName -> new VotingConfigExclusion("", nodeName)).collect(Collectors.toSet());
            }

            resolvedNodes = Arrays.stream(nodeResolutionResults.getResolvedNodes())
                .map(allNodes::get).filter(DiscoveryNode::isMasterNode).map(VotingConfigExclusion::new).collect(Collectors.toSet());

            allProcessedNodes = Sets.newHashSet(Iterables.concat(resolvedNodes, unresolvedNodes));

            if (allProcessedNodes.isEmpty()) {
                throw new IllegalArgumentException("add voting config exclusions request for nodeIds " + Arrays.asList(nodeIds) +
                    " or nodeNames " + Arrays.asList(nodeNames) + " matched no master-eligible nodes or absent nodes");
            }
        }

        allProcessedNodes.removeIf(n -> currentState.getVotingConfigExclusions().contains(n));
        return allProcessedNodes;
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
     * @return ids of the nodes for whom to add voting config exclusions.
     */
    public String[] getNodeIds() {
        return nodeIds;
    }

    /**
     * @return names of the nodes for whom to add voting config exclusions.
     */
    public String[] getNodeNames() {
        return nodeNames;
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
        // TODO should this be removed in the latest version where nodeIds and nodeNames are used?
        out.writeStringArray(nodeDescriptions);
        // TODO which version to use here?
        if (out.getVersion() == Version.V_EMPTY) {
            out.writeStringArray(nodeIds);
            out.writeStringArray(nodeNames);

        }
        out.writeTimeValue(timeout);
    }

    @Override
    public String toString() {
        return "AddVotingConfigExclusionsRequest{" +
            "nodeDescriptions=" + Arrays.asList(nodeDescriptions) + ", " +
            "nodeIds=" + Arrays.asList(nodeIds) + ", " +
            "nodeNames=" + Arrays.asList(nodeNames) + ", " +
            "timeout=" + timeout +
            '}';
    }
}
