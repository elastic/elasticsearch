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
        assert masterNode.isPresent() == knownPeers.isEmpty();
        this.masterNode = masterNode;
        this.knownPeers = knownPeers;
        this.term = term;
    }

    public PeersResponse(StreamInput in) throws IOException {
        masterNode = Optional.ofNullable(in.readOptionalWriteable(DiscoveryNode::new));
        knownPeers = in.readList(DiscoveryNode::new);
        term = in.readLong();
        assert masterNode.isPresent() == knownPeers.isEmpty();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalWriteable(masterNode.orElse(null));
        out.writeList(knownPeers);
        out.writeLong(term);
    }

    public Optional<DiscoveryNode> getMasterNode() {
        return masterNode;
    }

    public List<DiscoveryNode> getKnownPeers() {
        return knownPeers;
    }

    public long getTerm() {
        return term;
    }

    @Override
    public String toString() {
        return "PeersResponse{" +
            "masterNode=" + masterNode +
            ", knownPeers=" + knownPeers +
            ", term=" + term +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PeersResponse that = (PeersResponse) o;
        return term == that.term &&
            Objects.equals(masterNode, that.masterNode) &&
            Objects.equals(knownPeers, that.knownPeers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(masterNode, knownPeers, term);
    }
}
