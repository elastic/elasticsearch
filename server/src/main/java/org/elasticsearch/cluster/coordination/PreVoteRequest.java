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
        return "PreVoteRequest{" + "sourceNode=" + sourceNode + ", currentTerm=" + currentTerm + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PreVoteRequest that = (PreVoteRequest) o;
        return currentTerm == that.currentTerm && Objects.equals(sourceNode, that.sourceNode);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sourceNode, currentTerm);
    }
}
