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
import java.util.Objects;

public class PreVoteRequest extends TransportRequest {

    private final DiscoveryNode sourceNode;
    private final long currentTerm;

    public PreVoteRequest(DiscoveryNode sourceNode, long currentTerm) {
        this.sourceNode = sourceNode;
        this.currentTerm = currentTerm;
    }

    public PreVoteRequest(StreamInput in) throws IOException {
        super(in);
        sourceNode = new DiscoveryNode(in);
        currentTerm = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        sourceNode.writeTo(out);
        out.writeLong(currentTerm);
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    @Override
    public String toString() {
        return "PreVoteRequest{" +
            "sourceNode=" + sourceNode +
            ", currentTerm=" + currentTerm +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PreVoteRequest that = (PreVoteRequest) o;
        return currentTerm == that.currentTerm &&
            Objects.equals(sourceNode, that.sourceNode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceNode, currentTerm);
    }
}
