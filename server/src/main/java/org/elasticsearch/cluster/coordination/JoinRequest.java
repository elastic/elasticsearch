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
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Optional;

public class JoinRequest extends TransportRequest {

    private final DiscoveryNode sourceNode;

    private final Optional<Join> optionalJoin;

    public JoinRequest(DiscoveryNode sourceNode, Optional<Join> optionalJoin) {
        assert optionalJoin.isPresent() == false || optionalJoin.get().getSourceNode().equals(sourceNode);
        this.sourceNode = sourceNode;
        this.optionalJoin = optionalJoin;
    }

    public JoinRequest(StreamInput in) throws IOException {
        super(in);
        sourceNode = new DiscoveryNode(in);
        optionalJoin = Optional.ofNullable(in.readOptionalWriteable(Join::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        sourceNode.writeTo(out);
        out.writeOptionalWriteable(optionalJoin.orElse(null));
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public Optional<Join> getOptionalJoin() {
        return optionalJoin;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof JoinRequest)) return false;

        JoinRequest that = (JoinRequest) o;

        if (!sourceNode.equals(that.sourceNode)) return false;
        return optionalJoin.equals(that.optionalJoin);
    }

    @Override
    public int hashCode() {
        int result = sourceNode.hashCode();
        result = 31 * result + optionalJoin.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "JoinRequest{" +
            "sourceNode=" + sourceNode +
            ", optionalJoin=" + optionalJoin +
            '}';
    }
}
