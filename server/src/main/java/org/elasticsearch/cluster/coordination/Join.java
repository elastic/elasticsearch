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
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Triggered by a {@link StartJoinRequest}, instances of this class represent join votes,
 * and have a source and target node. The source node is the node that provides the vote,
 * and the target node is the node for which this vote is cast. A node will only cast
 * a single vote per term, and this for a unique target node. The vote also carries
 * information about the current state of the node that provided the vote, so that
 * the receiver of the vote can determine if it has a more up-to-date state than the
 * source node.
 */
public class Join implements Writeable {
    private final DiscoveryNode sourceNode;
    private final DiscoveryNode targetNode;
    private final long term;
    private final long lastAcceptedTerm;
    private final long lastAcceptedVersion;

    public Join(DiscoveryNode sourceNode, DiscoveryNode targetNode, long term, long lastAcceptedTerm, long lastAcceptedVersion) {
        assert term >= 0;
        assert lastAcceptedTerm >= 0;
        assert lastAcceptedVersion >= 0;

        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        this.term = term;
        this.lastAcceptedTerm = lastAcceptedTerm;
        this.lastAcceptedVersion = lastAcceptedVersion;
    }

    public Join(StreamInput in) throws IOException {
        sourceNode = new DiscoveryNode(in);
        targetNode = new DiscoveryNode(in);
        term = in.readLong();
        lastAcceptedTerm = in.readLong();
        lastAcceptedVersion = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        sourceNode.writeTo(out);
        targetNode.writeTo(out);
        out.writeLong(term);
        out.writeLong(lastAcceptedTerm);
        out.writeLong(lastAcceptedVersion);
    }

    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public DiscoveryNode getTargetNode() {
        return targetNode;
    }

    public boolean targetMatches(DiscoveryNode matchingNode) {
        return targetNode.getId().equals(matchingNode.getId());
    }

    public long getLastAcceptedVersion() {
        return lastAcceptedVersion;
    }

    public long getTerm() {
        return term;
    }

    public long getLastAcceptedTerm() {
        return lastAcceptedTerm;
    }

    @Override
    public String toString() {
        return "Join{"
            + "term="
            + term
            + ", lastAcceptedTerm="
            + lastAcceptedTerm
            + ", lastAcceptedVersion="
            + lastAcceptedVersion
            + ", sourceNode="
            + sourceNode
            + ", targetNode="
            + targetNode
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Join join = (Join) o;

        if (sourceNode.equals(join.sourceNode) == false) return false;
        if (targetNode.equals(join.targetNode) == false) return false;
        if (lastAcceptedVersion != join.lastAcceptedVersion) return false;
        if (term != join.term) return false;
        return lastAcceptedTerm == join.lastAcceptedTerm;
    }

    @Override
    public int hashCode() {
        int result = (int) (lastAcceptedVersion ^ (lastAcceptedVersion >>> 32));
        result = 31 * result + sourceNode.hashCode();
        result = 31 * result + targetNode.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (lastAcceptedTerm ^ (lastAcceptedTerm >>> 32));
        return result;
    }
}
