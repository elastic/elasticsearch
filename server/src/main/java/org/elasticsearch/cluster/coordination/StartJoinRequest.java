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

/**
 * Represents the action of requesting a join vote (see {@link Join}) from a node.
 * The source node represents the node that is asking for join votes.
 */
public class StartJoinRequest extends TransportRequest {

    private final DiscoveryNode sourceNode;

    private final long term;

    public StartJoinRequest(DiscoveryNode sourceNode, long term) {
        this.sourceNode = sourceNode;
        this.term = term;
    }

    public StartJoinRequest(StreamInput input) throws IOException {
        super(input);
        this.sourceNode = new DiscoveryNode(input);
        this.term = input.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        sourceNode.writeTo(out);
        out.writeLong(term);
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public long getTerm() {
        return term;
    }

    @Override
    public String toString() {
        return "StartJoinRequest{" + "term=" + term + ",node=" + sourceNode + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof StartJoinRequest) == false) return false;

        StartJoinRequest that = (StartJoinRequest) o;

        if (term != that.term) return false;
        return sourceNode.equals(that.sourceNode);
    }

    @Override
    public int hashCode() {
        int result = sourceNode.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        return result;
    }
}
