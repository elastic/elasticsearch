/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.CoordinationStateRejectedException;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.coordination.StartJoinRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.blobstore.BlobContainer;

import java.util.OptionalLong;

public class StatelessElectionStrategy extends ElectionStrategy {
    private static final String LEASE_BLOB = "lease";
    private final BlobContainer blobContainer;

    StatelessElectionStrategy(BlobContainer blobContainer) {
        this.blobContainer = blobContainer;
    }

    @Override
    protected boolean satisfiesAdditionalQuorumConstraints(
        DiscoveryNode localNode,
        long localCurrentTerm,
        long localAcceptedTerm,
        long localAcceptedVersion,
        CoordinationMetadata.VotingConfiguration lastCommittedConfiguration,
        CoordinationMetadata.VotingConfiguration lastAcceptedConfiguration,
        CoordinationState.VoteCollection joinVotes
    ) {
        return true;
    }

    @Override
    public boolean isPublishQuorum(
        CoordinationState.VoteCollection voteCollection,
        CoordinationMetadata.VotingConfiguration lastCommittedConfiguration,
        CoordinationMetadata.VotingConfiguration latestPublishedConfiguration
    ) {
        assert lastCommittedConfiguration.isEmpty() == false;
        assert latestPublishedConfiguration.isEmpty() == false;

        return voteCollection.isQuorum(latestPublishedConfiguration);
    }

    @Override
    public boolean isElectionQuorum(
        DiscoveryNode localNode,
        long localCurrentTerm,
        long localAcceptedTerm,
        long localAcceptedVersion,
        CoordinationMetadata.VotingConfiguration lastCommittedConfiguration,
        CoordinationMetadata.VotingConfiguration lastAcceptedConfiguration,
        CoordinationState.VoteCollection joinVotes
    ) {
        assert lastAcceptedConfiguration.getNodeIds().size() == 1;

        return joinVotes.containsVoteFor(localNode);
    }

    @Override
    public void onNewElection(DiscoveryNode candidateMasterNode, long proposedTerm, ActionListener<StartJoinRequest> listener) {
        getCurrentLeaseTerm(listener.delegateFailure((delegate, currentLeaseTermOpt) -> {
            long currentLeaseTerm = currentLeaseTermOpt.orElse(0);
            final long electionTerm = Math.max(proposedTerm, currentLeaseTerm + 1);

            blobContainer.compareAndSetRegister(
                LEASE_BLOB,
                currentLeaseTerm,
                electionTerm,
                delegate.delegateFailure((delegate2, termGranted) -> {
                    if (termGranted) {
                        listener.onResponse(new StartJoinRequest(candidateMasterNode, electionTerm));
                    } else {
                        listener.onFailure(
                            new CoordinationStateRejectedException("Term " + proposedTerm + " already claimed by a different node")
                        );
                    }
                })
            );
        }));
    }

    @Override
    public void beforeCommit(long term, long version, ActionListener<Void> listener) {
        getCurrentLeaseTerm(listener.map(currentTerm -> {
            if (currentTerm.isEmpty()) {
                throw new IllegalStateException("Unexpected empty claimed term");
            }

            if (currentTerm.getAsLong() == term) {
                return null;
            } else {
                assert term < currentTerm.getAsLong() : term + " vs " + currentTerm;
                throw new CoordinationStateRejectedException("Term " + term + " already claimed by another node");
            }
        }));
    }

    @Override
    public boolean isInvalidReconfiguration(
        ClusterState clusterState,
        CoordinationMetadata.VotingConfiguration lastAcceptedConfiguration,
        CoordinationMetadata.VotingConfiguration lastCommittedConfiguration
    ) {
        return false;
    }

    // Visible for testing
    void getCurrentLeaseTerm(ActionListener<OptionalLong> listener) {
        blobContainer.getRegister(LEASE_BLOB, listener);
    }
}
