/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.CoordinationStateRejectedException;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.coordination.StartJoinRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.Supplier;

public class StatelessElectionStrategy extends ElectionStrategy {
    public static final String NAME = "stateless_election_strategy";
    private static final String LEASE_BLOB = "lease";
    static final TimeValue READ_CURRENT_LEASE_TERM_RETRY_DELAY = TimeValue.timeValueMillis(200);
    static final int MAX_READ_CURRENT_LEASE_TERM_RETRIES = 4;
    private final Supplier<BlobContainer> blobContainerSupplier;
    private final ThreadPool threadPool;

    public StatelessElectionStrategy(Supplier<BlobContainer> blobContainerSupplier, ThreadPool threadPool) {
        this.blobContainerSupplier = blobContainerSupplier;
        this.threadPool = threadPool;
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
            final long currentLeaseTerm = currentLeaseTermOpt.orElse(0L);
            final long electionTerm = Math.max(proposedTerm, currentLeaseTerm + 1);

            blobContainer().compareAndSetRegister(
                LEASE_BLOB,
                bytesFromLong(currentLeaseTerm),
                bytesFromLong(electionTerm),
                delegate.delegateFailure((delegate2, termGranted) -> {
                    if (termGranted) {
                        listener.onResponse(new StartJoinRequest(candidateMasterNode, electionTerm));
                    } else {
                        listener.onFailure(
                            new CoordinationStateRejectedException(
                                Strings.format("term [%d] already claimed by a different node", electionTerm)
                            )
                        );
                    }
                })
            );
        }));
    }

    @Override
    public void beforeCommit(long term, long version, ActionListener<Void> listener) {
        doBeforeCommit(term, version, 0, listener);
    }

    private void doBeforeCommit(long term, long version, int retryCount, ActionListener<Void> listener) {
        getCurrentLeaseTerm(listener.delegateFailure((delegate, currentTerm) -> {
            if (currentTerm.isEmpty()) {
                if (retryCount < MAX_READ_CURRENT_LEASE_TERM_RETRIES) {
                    threadPool.schedule(
                        () -> doBeforeCommit(term, version, retryCount + 1, delegate),
                        READ_CURRENT_LEASE_TERM_RETRY_DELAY,
                        ThreadPool.Names.SAME
                    );
                } else {
                    delegate.onFailure(new IllegalStateException(Strings.format("""
                        failing commit of cluster state version [%d] in term [%d] after [%d] failed attempts to verify the \
                        current term""", version, term, retryCount)));
                }
                return;
            }

            if (currentTerm.getAsLong() == term) {
                delegate.onResponse(null);
            } else {
                assert term < currentTerm.getAsLong() : term + " vs " + currentTerm;
                delegate.onFailure(
                    new CoordinationStateRejectedException(
                        Strings.format(
                            "failing commit of cluster state version [%d] in term [%d] since current term is now [%d]",
                            version,
                            term,
                            currentTerm.getAsLong()
                        )
                    )
                );
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

    public void getCurrentLeaseTerm(ActionListener<OptionalLong> listener) {
        threadPool.executor(getExecutorName())
            .execute(ActionRunnable.wrap(listener, l -> blobContainer().getRegister(LEASE_BLOB, l.map(optionalBytesReference -> {
                if (optionalBytesReference.isPresent()) {
                    return OptionalLong.of(longFromBytes(optionalBytesReference.bytesReference()));
                } else {
                    return OptionalLong.empty();
                }
            }))));
    }

    protected String getExecutorName() {
        return ThreadPool.Names.SNAPSHOT_META;
    }

    private BlobContainer blobContainer() {
        return Objects.requireNonNull(blobContainerSupplier.get());
    }

    static long longFromBytes(BytesReference bytesReference) {
        if (bytesReference.length() == 0) {
            return 0L;
        } else if (bytesReference.length() == Long.BYTES) {
            try (var baos = new ByteArrayOutputStream(Long.BYTES)) {
                bytesReference.writeTo(baos);
                final var bytes = baos.toByteArray();
                assert bytes.length == Long.BYTES;
                return ByteUtils.readLongBE(bytes, 0);
            } catch (IOException e) {
                assert false : "no IO takes place";
                throw new IllegalStateException("unexpected conversion error", e);
            }
        } else {
            throw new IllegalArgumentException("cannot read long from BytesReference of length " + bytesReference.length());
        }
    }

    static BytesReference bytesFromLong(long value) {
        if (value == 0L) {
            return BytesArray.EMPTY;
        } else {
            final var bytes = new byte[Long.BYTES];
            ByteUtils.writeLongBE(value, bytes, 0);
            return new BytesArray(bytes);
        }
    }
}
