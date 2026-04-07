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
 *
 * @param votingNode The node casting a vote for a master candidate.
 * @param masterCandidateNode The master candidate node receiving the vote for election.
 * @param term
 * @param lastAcceptedTerm
 * @param lastAcceptedVersion
 */
public record Join(DiscoveryNode votingNode, DiscoveryNode masterCandidateNode, long term, long lastAcceptedTerm, long lastAcceptedVersion)
    implements
        Writeable {
    public Join {
        assert term >= 0;
        assert lastAcceptedTerm >= 0;
        assert lastAcceptedVersion >= 0;
    }

    public Join(StreamInput in) throws IOException {
        this(new DiscoveryNode(in), new DiscoveryNode(in), in.readLong(), in.readLong(), in.readLong());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        votingNode.writeTo(out);
        masterCandidateNode.writeTo(out);
        out.writeLong(term);
        out.writeLong(lastAcceptedTerm);
        out.writeLong(lastAcceptedVersion);
    }

    public boolean masterCandidateMatches(DiscoveryNode matchingNode) {
        return masterCandidateNode.getId().equals(matchingNode.getId());
    }
}
