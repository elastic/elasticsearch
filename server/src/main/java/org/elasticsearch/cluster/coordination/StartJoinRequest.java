/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.AbstractTransportRequest;

import java.io.IOException;

/**
 * Represents the action of requesting a join vote (see {@link Join}) from a node.
 * <p>
 * A {@link StartJoinRequest} is broadcast to each node in the cluster, requesting
 * that each node join the new cluster formed around the master candidate node in a
 * new term. The sender is either the new master candidate or the current master
 * abdicating to another eligible node in the cluster.
 */
public class StartJoinRequest extends AbstractTransportRequest {

    private final DiscoveryNode masterCandidateNode;

    private final long term;

    public StartJoinRequest(DiscoveryNode masterCandidateNode, long term) {
        this.masterCandidateNode = masterCandidateNode;
        this.term = term;
    }

    public StartJoinRequest(StreamInput input) throws IOException {
        super(input);
        this.masterCandidateNode = new DiscoveryNode(input);
        this.term = input.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        masterCandidateNode.writeTo(out);
        out.writeLong(term);
    }

    public DiscoveryNode getMasterCandidateNode() {
        return masterCandidateNode;
    }

    public long getTerm() {
        return term;
    }

    @Override
    public String toString() {
        return "StartJoinRequest{" + "term=" + term + ",node=" + masterCandidateNode + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof StartJoinRequest) == false) return false;

        StartJoinRequest that = (StartJoinRequest) o;

        if (term != that.term) return false;
        return masterCandidateNode.equals(that.masterCandidateNode);
    }

    @Override
    public int hashCode() {
        int result = masterCandidateNode.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        return result;
    }
}
