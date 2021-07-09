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

public class GetServiceAccountFileTokensResponse extends BaseNodesResponse<GetServiceAccountFileTokensResponse.Node> {

    public GetServiceAccountFileTokensResponse(ClusterName clusterName,
                                               List<GetServiceAccountFileTokensResponse.Node> nodes,
                                               List<FailedNodeException> failures) {
        super(clusterName, nodes, failures);
    }

    public GetServiceAccountFileTokensResponse(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected List<GetServiceAccountFileTokensResponse.Node> readNodesFrom(StreamInput in) throws IOException {
        return in.readList(GetServiceAccountFileTokensResponse.Node::new);
    }

    @Override
    protected void writeNodesTo(StreamOutput out, List<GetServiceAccountFileTokensResponse.Node> nodes) throws IOException {
        out.writeList(nodes);
    }

    public List<TokenInfo> getTokenInfos() {
        final Map<String, Set<String>> fileTokenDistribution = new HashMap<>();
        for (GetServiceAccountFileTokensResponse.Node node: getNodes()) {
            if (node.tokenNames == null) {
                continue;
            }
            Arrays.stream(node.tokenNames).forEach(name -> {
                final Set<String> distribution = fileTokenDistribution.computeIfAbsent(name, k -> new HashSet<>());
                distribution.add(node.getNode().getName());
            });
        }
        return fileTokenDistribution.entrySet().stream()
            .map(entry -> TokenInfo.fileToken(entry.getKey(), entry.getValue().stream().sorted().collect(Collectors.toUnmodifiableList())))
            .collect(Collectors.toUnmodifiableList());
    }

    public static class Node extends BaseNodeResponse {

        public final String[] tokenNames;

        public Node(StreamInput in) throws IOException {
            super(in);
            this.tokenNames = in.readStringArray();
        }

        public Node(DiscoveryNode node, String[] tokenNames) {
            super(node);
            this.tokenNames = tokenNames;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(tokenNames);
        }
    }
}
