package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;

import java.util.Optional;

public class JoinRequest {

    private final DiscoveryNode sourceNode;

    private final Optional<Join> optionalJoin;

    public JoinRequest(DiscoveryNode sourceNode, Optional<Join> optionalJoin) {
        this.sourceNode = sourceNode;
        this.optionalJoin = optionalJoin;
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public Optional<Join> getOptionalJoin() {
        return optionalJoin;
    }

}
