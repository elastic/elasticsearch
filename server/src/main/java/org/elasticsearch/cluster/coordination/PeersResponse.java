/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class PeersResponse extends TransportResponse {
    private final Optional<DiscoveryNode> masterNode;
    private final List<DiscoveryNode> knownPeers;
    private final long term;

    public PeersResponse(Optional<DiscoveryNode> masterNode, List<DiscoveryNode> knownPeers, long term) {
        assert masterNode.isPresent() == false || knownPeers.isEmpty();
        this.masterNode = masterNode;
        this.knownPeers = knownPeers;
        this.term = term;
    }

    public PeersResponse(StreamInput in) throws IOException {
        masterNode = Optional.ofNullable(in.readOptionalWriteable(DiscoveryNode::new));
        knownPeers = in.readList(DiscoveryNode::new);
        term = in.readLong();
        assert masterNode.isPresent() == false || knownPeers.isEmpty();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(masterNode.orElse(null));
        out.writeList(knownPeers);
        out.writeLong(term);
    }

    /**
     * @return the node that is currently leading, according to the responding node.
     */
    public Optional<DiscoveryNode> getMasterNode() {
        return masterNode;
    }

    /**
     * @return the collection of known peers of the responding node, or an empty collection if the responding node believes there
     * is currently a leader.
     */
    public List<DiscoveryNode> getKnownPeers() {
        return knownPeers;
    }

    /**
     * @return the current term of the responding node. If the responding node is the leader then this is the term in which it is
     * currently leading.
     */
    public long getTerm() {
        return term;
    }

    @Override
    public String toString() {
        return "PeersResponse{" + "masterNode=" + masterNode + ", knownPeers=" + knownPeers + ", term=" + term + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PeersResponse that = (PeersResponse) o;
        return term == that.term && Objects.equals(masterNode, that.masterNode) && Objects.equals(knownPeers, that.knownPeers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(masterNode, knownPeers, term);
    }
}
