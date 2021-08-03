/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.service;

import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.support.nodes.BaseNodeResponse;
import org.elasticsearch.action.support.nodes.BaseNodesResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Unlike index-backed service account tokens, file-backed tokens are local to the node.
 * This response is to fetch information about them from each node. Note the class is
 * more generically named for the possible future expansion to cover other types of credentials
 * that are local to the node.
 */
public class GetServiceAccountCredentialsNodesResponse extends BaseNodesResponse<GetServiceAccountCredentialsNodesResponse.Node> {

    public GetServiceAccountCredentialsNodesResponse(ClusterName clusterName,
                                                     List<GetServiceAccountCredentialsNodesResponse.Node> nodes,
                                                     List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    public GetServiceAccountCredentialsNodesResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected List<GetServiceAccountCredentialsNodesResponse.Node> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(GetServiceAccountCredentialsNodesResponse.Node::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<GetServiceAccountCredentialsNodesResponse.Node> nodes) throws IOException {
        out.writeList(nodes);
    }

    public List<TokenInfo> getFileTokenInfos() {
        final Map<String, Set<String>> fileTokenDistribution = new HashMap<>();
        for (GetServiceAccountCredentialsNodesResponse.Node node: getNodes()) {
            if (node.fileTokenNames == null) {
                continue;
            }
            Arrays.stream(node.fileTokenNames).forEach(name -> {
                final Set<String> distribution = fileTokenDistribution.computeIfAbsent(name, k -> new HashSet<>());
                distribution.add(node.getNode().getName());
            });
        }
        return fileTokenDistribution.entrySet().stream()
            .map(entry -> TokenInfo.fileToken(entry.getKey(), entry.getValue().stream().sorted().collect(Collectors.toList())))
            .collect(Collectors.toList());
    }

    public static class Node extends BaseNodeResponse {

        public final String[] fileTokenNames;

        public Node(StreamInput in) throws IOException {
            super(in);
            this.fileTokenNames = in.readStringArray();
        }

        public Node(DiscoveryNode node, String[] fileTokenNames) {
            super(node);
            this.fileTokenNames = fileTokenNames;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(fileTokenNames);
        }
    }
}
