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
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;

public class StatelessClusterConsistencyService {

    private final Logger logger = LogManager.getLogger(StatelessClusterConsistencyService.class);

    private final ClusterService clusterService;
    private final StatelessElectionStrategy electionStrategy;

    public StatelessClusterConsistencyService(final ClusterService clusterService, final StatelessElectionStrategy electionStrategy) {
        this.clusterService = clusterService;
        this.electionStrategy = electionStrategy;
    }

    /**
     * This method will read the root blob lease from the object store. If the root blob indicates a different term or node left generation
     * than the current cluster state, the this method will wait for a new cluster state that matches the read root blob lease.
     * <p>
     * This method should be used when it is important to ensure that the local cluster state is consistent with the root blob.
     */
    public void ensureClusterStateConsistentWithRootBlob(ActionListener<Void> listener, final TimeValue timeout) {
        final var startingClusterState = clusterService.state();
        final var startingStateLease = new StatelessElectionStrategy.Lease(
            startingClusterState.term(),
            startingClusterState.nodes().getNodeLeftGeneration()
        );
        final var startingClusterStateVersion = startingClusterState.version();
        electionStrategy.readLease(listener.delegateFailureAndWrap((delegate, optionalLease) -> {
            if (optionalLease.isEmpty()) {
                // TODO: Re-enable assertion or remove once that core cause is identified
                // assert false : "We should not be validating cluster state before root blob written";
                throw new IllegalStateException("No root blob to validate cluster state.");
            }

            StatelessElectionStrategy.Lease lease = optionalLease.get();
            if (lease.compareTo(startingStateLease) <= 0) {
                assert lease.compareTo(startingStateLease) == 0 : lease + " vs " + startingStateLease;
                listener.onResponse(null);
            } else {
                ClusterStateObserver observer = new ClusterStateObserver(
                    startingClusterStateVersion,
                    clusterService.getClusterApplierService(),
                    timeout,
                    logger,
                    clusterService.threadPool().getThreadContext()
                );
                observer.waitForNextChange(new ClusterStateObserver.Listener() {
                    @Override
                    public void onNewClusterState(ClusterState state) {
                        listener.onResponse(null);
                    }

                    @Override
                    public void onClusterServiceClose() {
                        listener.onFailure(new NodeClosedException(clusterService.localNode()));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        listener.onFailure(
                            new ElasticsearchTimeoutException(
                                Strings.format(
                                    "Timed out while verifying node membership, assuming node left the cluster "
                                        + "[timeout=%s, term=%s, nodeLeftGeneration=%s].",
                                    timeout,
                                    lease.currentTerm(),
                                    lease.nodeLeftGeneration()
                                )
                            )
                        );
                    }
                }, clusterState -> {
                    final var newStateLease = new StatelessElectionStrategy.Lease(
                        clusterState.term(),
                        clusterState.nodes().getNodeLeftGeneration()
                    );
                    assert startingStateLease.compareTo(newStateLease) <= 0 : startingStateLease + " vs " + newStateLease;
                    return lease.compareTo(newStateLease) <= 0;
                });
            }
        }));
    }

    public ClusterState state() {
        return clusterService.state();
    }
}
