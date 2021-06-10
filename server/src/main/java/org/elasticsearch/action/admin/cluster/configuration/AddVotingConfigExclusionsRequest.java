/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.configuration;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.master.MasterNodeRequest;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A request to add voting config exclusions for certain master-eligible nodes, and wait for these nodes to be removed from the voting
 * configuration.
 */
public class AddVotingConfigExclusionsRequest extends MasterNodeRequest<AddVotingConfigExclusionsRequest> {
    private final String[] nodeIds;
    private final String[] nodeNames;
    private final TimeValue timeout;

    /**
     * Construct a request to add voting config exclusions for master-eligible nodes matching the given node names, and wait for a
     * default 30 seconds for these exclusions to take effect, removing the nodes from the voting configuration.
     *
     * @param nodeNames Names of the nodes to add - see {@link AddVotingConfigExclusionsRequest#resolveVotingConfigExclusions(ClusterState)}
     */
    public AddVotingConfigExclusionsRequest(String... nodeNames) {
        this(Strings.EMPTY_ARRAY, nodeNames, TimeValue.timeValueSeconds(30));
    }

    /**
     * Construct a request to add voting config exclusions for master-eligible nodes matching the given descriptions, and wait for these
     * nodes to be removed from the voting configuration.
     *
     * @param nodeIds   Ids of the nodes whose exclusions to add - see
     *                  {@link AddVotingConfigExclusionsRequest#resolveVotingConfigExclusions(ClusterState)}.
     * @param nodeNames Names of the nodes whose exclusions to add - see
     *                  {@link AddVotingConfigExclusionsRequest#resolveVotingConfigExclusions(ClusterState)}.
     * @param timeout   How long to wait for the added exclusions to take effect and be removed from the voting configuration.
     */
    public AddVotingConfigExclusionsRequest(String[] nodeIds, String[] nodeNames, TimeValue timeout) {
        if (timeout.compareTo(TimeValue.ZERO) < 0) {
            throw new IllegalArgumentException("timeout [" + timeout + "] must be non-negative");
        }

        if ((nodeIds.length > 0) == (nodeNames.length > 0)) {
            throw new IllegalArgumentException("You must set [node_names] or [node_ids] but not both");
        }

        this.nodeIds = nodeIds;
        this.nodeNames = nodeNames;
        this.timeout = timeout;
    }

    public AddVotingConfigExclusionsRequest(StreamInput in) throws IOException {
        super(in);
        if (in.getVersion().before(Version.V_8_0_0)) {
            final String[] legacyNodeDescriptions = in.readStringArray();
            if (legacyNodeDescriptions.length > 0) {
                throw new IllegalArgumentException("legacy [node_name] field was deprecated and must be empty");
            }
        }
        nodeIds = in.readStringArray();
        nodeNames = in.readStringArray();
        timeout = in.readTimeValue();
        assert (nodeIds.length > 0) != (nodeNames.length > 0);
    }

    Set<VotingConfigExclusion> resolveVotingConfigExclusions(ClusterState currentState) {
        final DiscoveryNodes allNodes = currentState.nodes();
        Set<VotingConfigExclusion> newVotingConfigExclusions = new HashSet<>();

        if (nodeIds.length > 0) {
            for (String nodeId : nodeIds) {
                if (allNodes.nodeExists(nodeId)) {
                    DiscoveryNode discoveryNode = allNodes.get(nodeId);
                    if (discoveryNode.isMasterNode()) {
                        newVotingConfigExclusions.add(new VotingConfigExclusion(discoveryNode));
                    }
                } else {
                    newVotingConfigExclusions.add(new VotingConfigExclusion(nodeId, VotingConfigExclusion.MISSING_VALUE_MARKER));
                }
            }
        } else {
            assert nodeNames.length > 0;
            Map<String, DiscoveryNode> existingNodes = StreamSupport.stream(allNodes.spliterator(), false)
                .collect(Collectors.toMap(DiscoveryNode::getName, Function.identity()));

            for (String nodeName : nodeNames) {
                if (existingNodes.containsKey(nodeName)) {
                    DiscoveryNode discoveryNode = existingNodes.get(nodeName);
                    if (discoveryNode.isMasterNode()) {
                        newVotingConfigExclusions.add(new VotingConfigExclusion(discoveryNode));
                    }
                } else {
                    newVotingConfigExclusions.add(new VotingConfigExclusion(VotingConfigExclusion.MISSING_VALUE_MARKER, nodeName));
                }
            }
        }

        newVotingConfigExclusions.removeIf(n -> currentState.getVotingConfigExclusions().contains(n));
        return newVotingConfigExclusions;
    }

    Set<VotingConfigExclusion> resolveVotingConfigExclusionsAndCheckMaximum(ClusterState currentState, int maxExclusionsCount,
                                                                            String maximumSettingKey) {
        final Set<VotingConfigExclusion> resolvedExclusions = resolveVotingConfigExclusions(currentState);

        final int oldExclusionsCount = currentState.getVotingConfigExclusions().size();
        final int newExclusionsCount = resolvedExclusions.size();
        if (oldExclusionsCount + newExclusionsCount > maxExclusionsCount) {
            throw new IllegalArgumentException("add voting config exclusions request for "
                + (nodeNames.length > 0
                    ? "nodes named " + Arrays.asList(nodeNames)
                    : "nodes with ids " + Arrays.asList(nodeIds))
                + " would add [" + newExclusionsCount + "] exclusions to the existing [" + oldExclusionsCount
                + "] which would exceed the maximum of [" + maxExclusionsCount + "] set by ["
                + maximumSettingKey + "]");
        }
        return resolvedExclusions;
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
        if (out.getVersion().before(Version.V_8_0_0)) {
            out.writeStringArray(Strings.EMPTY_ARRAY);
        }
        out.writeStringArray(nodeIds);
        out.writeStringArray(nodeNames);
        out.writeTimeValue(timeout);
    }

    @Override
    public String toString() {
        return "AddVotingConfigExclusionsRequest{" +
            "nodeIds=" + Arrays.asList(nodeIds) + ", " +
            "nodeNames=" + Arrays.asList(nodeNames) + ", " +
            "timeout=" + timeout +
            '}';
    }
}
