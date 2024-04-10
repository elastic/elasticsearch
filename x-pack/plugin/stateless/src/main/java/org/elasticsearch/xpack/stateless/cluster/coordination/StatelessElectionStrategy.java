/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.cluster.coordination;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationState;
import org.elasticsearch.cluster.coordination.CoordinationStateRejectedException;
import org.elasticsearch.cluster.coordination.ElectionStrategy;
import org.elasticsearch.cluster.coordination.StartJoinRequest;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.shutdown.PluginShutdownService;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.Executor;
import java.util.function.Supplier;

public class StatelessElectionStrategy extends ElectionStrategy {
    private static final Logger logger = LogManager.getLogger(StatelessElectionStrategy.class);

    public static final String NAME = "stateless_election_strategy";
    private static final String LEASE_BLOB = "lease";
    static final TimeValue READ_CURRENT_LEASE_TERM_RETRY_DELAY = TimeValue.timeValueMillis(200);
    static final int MAX_READ_CURRENT_LEASE_TERM_RETRIES = 4;
    private final Supplier<BlobContainer> blobContainerSupplier;
    private final ThreadPool threadPool;
    private final Executor snapshotMetaExecutor;

    public StatelessElectionStrategy(Supplier<BlobContainer> blobContainerSupplier, ThreadPool threadPool) {
        this.blobContainerSupplier = blobContainerSupplier;
        this.threadPool = threadPool;
        this.snapshotMetaExecutor = threadPool.executor(ThreadPool.Names.SNAPSHOT_META);
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
        readLease(listener.delegateFailure((delegate, currentLeaseOpt) -> {
            final Lease currentLease = currentLeaseOpt.orElse(Lease.ZERO);
            final Lease newLease = new Lease(Math.max(proposedTerm, currentLease.currentTerm + 1), 0);

            blobContainer().compareAndSetRegister(
                OperationPurpose.CLUSTER_STATE,
                LEASE_BLOB,
                currentLease.asBytes(),
                newLease.asBytes(),
                delegate.delegateFailure((delegate2, termGranted) -> {
                    if (termGranted) {
                        delegate2.onResponse(new StartJoinRequest(candidateMasterNode, newLease.currentTerm));
                    } else {
                        delegate2.onFailure(
                            new CoordinationStateRejectedException(
                                Strings.format("term [%d] already claimed by a different node", newLease.currentTerm)
                            )
                        );
                    }
                })
            );
        }));
    }

    public void onNodeLeft(long expectedTerm, long nodeLeftGeneration) {
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        readLease(future.delegateFailureAndWrap((delegate, currentLeaseOpt) -> {
            final Lease currentLease = currentLeaseOpt.orElse(Lease.ZERO);
            if (currentLease.currentTerm != expectedTerm) {
                delegate.onFailure(
                    new CoordinationStateRejectedException(
                        "expected term [" + expectedTerm + "] but saw [" + currentLease.currentTerm + "]"
                    )
                );
                return;
            }
            final Lease newLease = new Lease(currentLease.currentTerm, nodeLeftGeneration);
            if (nodeLeftGeneration <= currentLease.nodeLeftGeneration) {
                assert nodeLeftGeneration == currentLease.nodeLeftGeneration
                    : "tried to set [" + newLease + "] after [" + currentLease + "]";
                delegate.onResponse(null);
                return;
            }
            blobContainer().compareAndSetRegister(
                OperationPurpose.CLUSTER_STATE,
                LEASE_BLOB,
                currentLease.asBytes(),
                newLease.asBytes(),
                delegate.delegateFailure((delegate2, updated) -> {
                    if (updated) {
                        delegate2.onResponse(null);
                    } else {
                        delegate2.onFailure(new CoordinationStateRejectedException("unexpected concurrent modification of lease"));
                    }
                })
            );
        }));
        FutureUtils.get(future);
    }

    @Override
    public void beforeCommit(long term, long version, ActionListener<Void> listener) {
        doBeforeCommit(term, version, 0, listener);
    }

    private void doBeforeCommit(long term, long version, int retryCount, ActionListener<Void> listener) {
        readLease(listener.delegateFailure((delegate, currentTerm) -> {
            if (currentTerm.isEmpty()) {
                if (retryCount < MAX_READ_CURRENT_LEASE_TERM_RETRIES) {
                    threadPool.schedule(
                        () -> doBeforeCommit(term, version, retryCount + 1, delegate),
                        READ_CURRENT_LEASE_TERM_RETRY_DELAY,
                        EsExecutors.DIRECT_EXECUTOR_SERVICE
                    );
                } else {
                    delegate.onFailure(new IllegalStateException(Strings.format("""
                        failing commit of cluster state version [%d] in term [%d] after [%d] failed attempts to verify the \
                        current term""", version, term, retryCount)));
                }
                return;
            }

            final long foundTerm = currentTerm.get().currentTerm;
            if (foundTerm == term) {
                delegate.onResponse(null);
            } else {
                assert term < foundTerm : term + " vs " + currentTerm;
                delegate.onFailure(
                    new CoordinationStateRejectedException(
                        Strings.format(
                            "failing commit of cluster state version [%d] in term [%d] since current term is now [%d]",
                            version,
                            term,
                            foundTerm
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
        readLease(listener.map(terms -> terms.map(value -> OptionalLong.of(value.currentTerm)).orElseGet(OptionalLong::empty)));
    }

    public void readLease(ActionListener<Optional<Lease>> listener) {
        getExecutor().execute(
            ActionRunnable.wrap(
                listener,
                l -> blobContainer().getRegister(OperationPurpose.CLUSTER_STATE, LEASE_BLOB, l.map(optionalBytesReference -> {
                    if (optionalBytesReference.isPresent()) {
                        Lease result;
                        BytesReference bytesReference = optionalBytesReference.bytesReference();
                        if (bytesReference.length() == 0) {
                            result = Lease.ZERO;
                        } else if (bytesReference.length() == Long.BYTES) {
                            result = new Lease(Long.reverseBytes(bytesReference.getLongLE(0)), Lease.UNSUPPORTED);
                        } else if (bytesReference.length() == 2 * Long.BYTES) {
                            result = new Lease(
                                Long.reverseBytes(bytesReference.getLongLE(0)),
                                Long.reverseBytes(bytesReference.getLongLE(Long.BYTES))
                            );
                        } else {
                            throw new IllegalArgumentException(
                                "cannot read terms from BytesReference of length " + bytesReference.length()
                            );
                        }
                        return Optional.of(result);
                    } else {
                        return Optional.empty();
                    }
                }))
            )
        );
    }

    protected Executor getExecutor() {
        return snapshotMetaExecutor;
    }

    private BlobContainer blobContainer() {
        return Objects.requireNonNull(blobContainerSupplier.get());
    }

    public record Lease(long currentTerm, long nodeLeftGeneration) implements Comparable<Lease> {
        private static final long UNSUPPORTED = -1L;

        private static final Lease ZERO = new Lease(0, 0);

        BytesReference asBytes() {
            if (currentTerm == 0) {
                return BytesArray.EMPTY;
            }
            final byte[] bytes;
            // If node left generation is unsupported, this lease was written by a cluster with a node whose version is prior to the time
            // when node left generation was introduced. Do not introduce the value until the entire cluster is upgraded and a node-left
            // event occurs.
            if (nodeLeftGeneration == UNSUPPORTED) {
                bytes = new byte[Long.BYTES];
                ByteUtils.writeLongBE(currentTerm, bytes, 0);
            } else {
                bytes = new byte[Long.BYTES * 2];
                ByteUtils.writeLongBE(currentTerm, bytes, 0);
                ByteUtils.writeLongBE(nodeLeftGeneration, bytes, Long.BYTES);
            }
            return new BytesArray(bytes);
        }

        @Override
        public int compareTo(Lease that) {
            int result = Long.compare(this.currentTerm, that.currentTerm);
            if (result == 0) {
                result = Long.compare(this.nodeLeftGeneration, that.nodeLeftGeneration);
            }
            return result;
        }
    }

    @Override
    public NodeEligibility nodeMayWinElection(ClusterState lastAcceptedState, DiscoveryNode node) {
        /*
         * Refuse to participate in elections if we are marked for shutdown.
         *
         * Note that this only really works in serverless where it's possible to stand up a new master-eligible node, with no shutdown
         * marker, that will read the latest state from the blob store. In stateful ES this is dangerous because the shutdown markers are
         * part of the persistent cluster state and if all master nodes are marked for shutdown then no elections will take place. If we
         * want something similar in stateful ES then we'd need to allow the shutting-down nodes to win elections if there are no other
         * viable nodes.
         */
        if (PluginShutdownService.shutdownNodes(lastAcceptedState).contains(node.getId())) {
            return new NodeEligibility(false, "node is ineligible for election during shutdown");
        }
        return super.nodeMayWinElection(lastAcceptedState, node);
    }
}
