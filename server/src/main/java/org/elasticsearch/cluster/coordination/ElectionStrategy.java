/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.coordination.CoordinationState.VoteCollection;
import org.elasticsearch.cluster.node.DiscoveryNode;

/**
 * Allows plugging in a custom election strategy, restricting the notion of an election quorum.
 * Custom additional quorum restrictions can be defined by implementing the {@link #satisfiesAdditionalQuorumConstraints} method.
 */
public abstract class ElectionStrategy {

    public static final ElectionStrategy DEFAULT_INSTANCE = new ElectionStrategy() {
        @Override
        protected boolean satisfiesAdditionalQuorumConstraints(
            DiscoveryNode localNode,
            long localCurrentTerm,
            long localAcceptedTerm,
            long localAcceptedVersion,
            VotingConfiguration lastCommittedConfiguration,
            VotingConfiguration lastAcceptedConfiguration,
            VoteCollection joinVotes
        ) {
            return true;
        }
    };

    /**
     * Whether there is an election quorum from the point of view of the given local node under the provided voting configurations
     */
    public boolean isElectionQuorum(
        DiscoveryNode localNode,
        long localCurrentTerm,
        long localAcceptedTerm,
        long localAcceptedVersion,
        VotingConfiguration lastCommittedConfiguration,
        VotingConfiguration lastAcceptedConfiguration,
        VoteCollection joinVotes
    ) {
        return joinVotes.isQuorum(lastCommittedConfiguration)
            && joinVotes.isQuorum(lastAcceptedConfiguration)
            && satisfiesAdditionalQuorumConstraints(
                localNode,
                localCurrentTerm,
                localAcceptedTerm,
                localAcceptedVersion,
                lastCommittedConfiguration,
                lastAcceptedConfiguration,
                joinVotes
            );
    }

    public boolean isPublishQuorum(
        VoteCollection voteCollection,
        VotingConfiguration lastCommittedConfiguration,
        VotingConfiguration latestPublishedConfiguration
    ) {
        return voteCollection.isQuorum(lastCommittedConfiguration) && voteCollection.isQuorum(latestPublishedConfiguration);
    }

    /**
     * The extension point to be overridden by plugins. Defines additional constraints on the election quorum.
     * @param localNode                  the local node for the election quorum
     * @param localCurrentTerm           the current term of the local node
     * @param localAcceptedTerm          the last accepted term of the local node
     * @param localAcceptedVersion       the last accepted version of the local node
     * @param lastCommittedConfiguration the last committed configuration for the election quorum
     * @param lastAcceptedConfiguration  the last accepted configuration for the election quorum
     * @param joinVotes                  the votes that were provided so far
     * @return true iff the additional quorum constraints are satisfied
     */
    protected abstract boolean satisfiesAdditionalQuorumConstraints(
        DiscoveryNode localNode,
        long localCurrentTerm,
        long localAcceptedTerm,
        long localAcceptedVersion,
        VotingConfiguration lastCommittedConfiguration,
        VotingConfiguration lastAcceptedConfiguration,
        VoteCollection joinVotes
    );

    public void onNewElection(DiscoveryNode candidateMasterNode, long proposedTerm, ActionListener<StartJoinRequest> listener) {
        listener.onResponse(new StartJoinRequest(candidateMasterNode, proposedTerm));
    }

    public boolean isInvalidReconfiguration(
        ClusterState clusterState,
        VotingConfiguration lastAcceptedConfiguration,
        VotingConfiguration lastCommittedConfiguration
    ) {
        return clusterState.getLastAcceptedConfiguration().equals(lastAcceptedConfiguration) == false
            && lastCommittedConfiguration.equals(lastAcceptedConfiguration) == false;
    }

    public void beforeCommit(long term, long version, ActionListener<Void> listener) {
        listener.onResponse(null);
    }
}
