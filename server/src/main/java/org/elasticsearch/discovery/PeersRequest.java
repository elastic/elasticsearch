/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.discovery;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class PeersRequest extends TransportRequest {
    private final DiscoveryNode sourceNode;
    private final List<DiscoveryNode> knownPeers;

    public PeersRequest(DiscoveryNode sourceNode, List<DiscoveryNode> knownPeers) {
        assert knownPeers.contains(sourceNode) == false : "local node is not a peer";
        this.sourceNode = sourceNode;
        this.knownPeers = knownPeers;
    }

    public PeersRequest(StreamInput in) throws IOException {
        super(in);
        sourceNode = new DiscoveryNode(in);
        knownPeers = in.readList(DiscoveryNode::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        sourceNode.writeTo(out);
        out.writeList(knownPeers);
    }

    public List<DiscoveryNode> getKnownPeers() {
        return knownPeers;
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    @Override
    public String toString() {
        return "PeersRequest{" +
            "sourceNode=" + sourceNode +
            ", knownPeers=" + knownPeers +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PeersRequest that = (PeersRequest) o;
        return Objects.equals(sourceNode, that.sourceNode) &&
            Objects.equals(knownPeers, that.knownPeers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceNode, knownPeers);
    }
}
