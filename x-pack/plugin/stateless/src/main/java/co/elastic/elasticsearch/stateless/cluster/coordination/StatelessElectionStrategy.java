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
import org.elasticsearch.TransportVersion;
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
    private static final TransportVersion STATELESS_LEASE_BLOB_V1_FORMAT = TransportVersion.fromName("stateless_lease_blob_v1_format");

    public static final String NAME = "stateless_election_strategy";
    public static final String LEASE_BLOB = "lease";
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
            final StatelessLease currentLease = currentLeaseOpt.orElse(StatelessLease.ZERO);
            // Use the same lease format version as the current lease since it is possible to have a mixed cluster and
            // we don't have a way to check using th cluster state MinVersion here
            final StatelessLease newLease = new StatelessLease(
                currentLease.formatVersion(),
                Math.max(proposedTerm, currentLease.currentTerm() + 1),
                0,
                // TODO: projectsUnderDeletedGeneration in the cluster state is ephemeral and is lost on a full cluster restart.
                // we need to either reset projectsUnderDeletedGeneration too upon master election or persist it in the cluster metadata.
                // https://elasticco.atlassian.net/browse/ES-12451
                currentLease.projectsUnderDeletedGeneration()
            );

            blobContainer().compareAndSetRegister(
                OperationPurpose.CLUSTER_STATE,
                LEASE_BLOB,
                currentLease.asBytes(),
                newLease.asBytes(),
                delegate.delegateFailure((delegate2, termGranted) -> {
                    if (termGranted) {
                        delegate2.onResponse(new StartJoinRequest(candidateMasterNode, newLease.currentTerm()));
                    } else {
                        delegate2.onFailure(
                            new CoordinationStateRejectedException(
                                Strings.format("term [%d] already claimed by a different node", newLease.currentTerm())
                            )
                        );
                    }
                })
            );
        }));
    }

    public void updateLease(ClusterState clusterState) {
        long expectedTerm = clusterState.term();
        long nodeLeftGeneration = clusterState.nodes().getNodeLeftGeneration();
        long projectsMarkedForDeletionGeneration = StatelessLease.getProjectsMarkedForDeletionGeneration(clusterState);
        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        readLease(future.delegateFailureAndWrap((delegate, currentLeaseOpt) -> {
            final StatelessLease currentLease = currentLeaseOpt.orElse(StatelessLease.ZERO);
            if (currentLease.currentTerm() != expectedTerm) {
                delegate.onFailure(
                    new CoordinationStateRejectedException(
                        "expected term [" + expectedTerm + "] but saw [" + currentLease.currentTerm() + "]"
                    )
                );
                return;
            }
            final StatelessLease newLease;
            if (currentLease.formatVersion() == StatelessLease.LEGACY_FORMAT_VERSION
                && clusterState.getMinTransportVersion().supports(STATELESS_LEASE_BLOB_V1_FORMAT) == false) {
                newLease = new StatelessLease(StatelessLease.LEGACY_FORMAT_VERSION, currentLease.currentTerm(), nodeLeftGeneration, 0);
            } else {
                newLease = new StatelessLease(currentLease.currentTerm(), nodeLeftGeneration, projectsMarkedForDeletionGeneration);
            }
            if (nodeLeftGeneration <= currentLease.nodeLeftGeneration()
                && projectsMarkedForDeletionGeneration <= currentLease.projectsUnderDeletedGeneration()) {
                assert nodeLeftGeneration == currentLease.nodeLeftGeneration()
                    && projectsMarkedForDeletionGeneration == currentLease.projectsUnderDeletedGeneration()
                    : "tried to set [" + newLease + "] after [" + currentLease + "]";
                delegate.onResponse(null);
                return;
            }
            final var newLeaseBytes = newLease.asBytes();
            blobContainer().compareAndSetRegister(
                OperationPurpose.CLUSTER_STATE,
                LEASE_BLOB,
                currentLease.asBytes(),
                newLeaseBytes,
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

            final long foundTerm = currentTerm.get().currentTerm();
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
        readLease(listener.map(terms -> terms.map(value -> OptionalLong.of(value.currentTerm())).orElseGet(OptionalLong::empty)));
    }

    public void readLease(ActionListener<Optional<StatelessLease>> listener) {
        getExecutor().execute(
            ActionRunnable.wrap(
                listener,
                l -> blobContainer().getRegister(OperationPurpose.CLUSTER_STATE, LEASE_BLOB, l.map(StatelessLease::fromBytes))
            )
        );
    }

    protected Executor getExecutor() {
        return snapshotMetaExecutor;
    }

    private BlobContainer blobContainer() {
        return Objects.requireNonNull(blobContainerSupplier.get());
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
