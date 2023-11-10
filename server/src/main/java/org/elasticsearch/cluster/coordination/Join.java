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
 * and have a voting and master-candidate node. The voting node is the node that provides
 * the vote, and the master-candidate node is the node for which this vote is cast. A join
 * vote is cast to reform the cluster around a particular master-eligible node, to elect
 * that node as the new master in a new term.
 *
 * A voting node will only cast a single vote per term. The vote includes information about
 * the current state of the node casting the vote, so that the candidate for the vote can
 * determine whether it has a more up-to-date state than the voting node.
 */
public class Join implements Writeable {
    // The node casting a vote for a master candidate.
    private final DiscoveryNode votingNode;

    // The master candidate node receiving the vote for election.
    private final DiscoveryNode masterCandidateNode;

    private final long term;
    private final long lastAcceptedTerm;
    private final long lastAcceptedVersion;

    public Join(DiscoveryNode votingNode, DiscoveryNode masterCandidateNode, long term, long lastAcceptedTerm, long lastAcceptedVersion) {
        assert term >= 0;
        assert lastAcceptedTerm >= 0;
        assert lastAcceptedVersion >= 0;

        this.votingNode = votingNode;
        this.masterCandidateNode = masterCandidateNode;
        this.term = term;
        this.lastAcceptedTerm = lastAcceptedTerm;
        this.lastAcceptedVersion = lastAcceptedVersion;
    }

    public Join(StreamInput in) throws IOException {
        votingNode = new DiscoveryNode(in);
        masterCandidateNode = new DiscoveryNode(in);
        term = in.readLong();
        lastAcceptedTerm = in.readLong();
        lastAcceptedVersion = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        votingNode.writeTo(out);
        masterCandidateNode.writeTo(out);
        out.writeLong(term);
        out.writeLong(lastAcceptedTerm);
        out.writeLong(lastAcceptedVersion);
    }

    public DiscoveryNode getVotingNode() {
        return votingNode;
    }

    public DiscoveryNode getMasterCandidateNode() {
        return masterCandidateNode;
    }

    /**
     * Temporary compatibility with serverless code repository.
     */
    public DiscoveryNode getSourceNode() {
        return masterCandidateNode;
    }

    public boolean masterCandidateMatches(DiscoveryNode matchingNode) {
        return masterCandidateNode.getId().equals(matchingNode.getId());
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
            + ", votingNode="
            + votingNode
            + ", masterCandidateNode="
            + masterCandidateNode
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Join join = (Join) o;

        if (votingNode.equals(join.votingNode) == false) return false;
        if (masterCandidateNode.equals(join.masterCandidateNode) == false) return false;
        if (lastAcceptedVersion != join.lastAcceptedVersion) return false;
        if (term != join.term) return false;
        return lastAcceptedTerm == join.lastAcceptedTerm;
    }

    @Override
    public int hashCode() {
        int result = (int) (lastAcceptedVersion ^ (lastAcceptedVersion >>> 32));
        result = 31 * result + votingNode.hashCode();
        result = 31 * result + masterCandidateNode.hashCode();
        result = 31 * result + (int) (term ^ (term >>> 32));
        result = 31 * result + (int) (lastAcceptedTerm ^ (lastAcceptedTerm >>> 32));
        return result;
    }
}
