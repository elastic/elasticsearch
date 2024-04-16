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
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;

public class StatelessClusterConsistencyService extends AbstractLifecycleComponent {

    private final Logger logger = LogManager.getLogger(StatelessClusterConsistencyService.class);
    public static final Setting<TimeValue> DELAYED_CLUSTER_CONSISTENCY_INTERVAL_SETTING = Setting.timeSetting(
        "stateless.translog.delayed_cluster_consistency_check.interval",
        timeValueSeconds(5),
        timeValueMillis(100),
        Setting.Property.NodeScope
    );

    private final ClusterService clusterService;
    private final StatelessElectionStrategy electionStrategy;
    private final ThreadPool threadPool;

    private final Queue<ActionListener<Void>> delayedListeners = new ConcurrentLinkedQueue<>();
    private volatile boolean pendingDelayedListeners = false;

    private final TimeValue delayedClusterConsistencyCheckInterval;
    private volatile Scheduler.Cancellable delayedClusterConsistencyListenersMonitor;

    public StatelessClusterConsistencyService(
        final ClusterService clusterService,
        final StatelessElectionStrategy electionStrategy,
        final ThreadPool threadPool,
        final Settings settings
    ) {
        this.clusterService = clusterService;
        this.electionStrategy = electionStrategy;
        this.threadPool = threadPool;
        this.delayedClusterConsistencyCheckInterval = DELAYED_CLUSTER_CONSISTENCY_INTERVAL_SETTING.get(settings);
    }

    @Override
    protected void doStart() {
        delayedClusterConsistencyListenersMonitor = threadPool.scheduleWithFixedDelay(new AbstractRunnable() {

            @Override
            protected void doRun() throws Exception {
                // Only run the check if there no ensureClusterStateConsistentWithRootBlob in progress
                if (pendingDelayedListeners == false && delayedListeners.isEmpty() == false) {
                    pendingDelayedListeners = true;
                } else if (pendingDelayedListeners) {
                    ensureClusterStateConsistentWithRootBlob(ActionListener.noop(), TimeValue.MAX_VALUE);
                }
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Unexpected exception when checking cluster state consistency");
            }
        }, delayedClusterConsistencyCheckInterval, threadPool.generic());
    }

    @Override
    protected void doStop() {
        if (delayedClusterConsistencyListenersMonitor != null) {
            delayedClusterConsistencyListenersMonitor.cancel();
        }
    }

    @Override
    protected void doClose() throws IOException {

    }

    /**
     * Defer a call to {@link StatelessClusterConsistencyService#ensureClusterStateConsistentWithRootBlob(ActionListener, TimeValue)}.
     * The listener will eventually called by sync call on the indexing path or the background delayed listener monitor.
     */
    public void delayedEnsureClusterStateConsistentWithRootBlob(ActionListener<Void> listener) {
        delayedListeners.add(listener);
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
        // Complete the immediate check listener along with the delayed listeners that were submitted before the check
        List<ActionListener<Void>> listenersToCall = new ArrayList<>(delayedListeners.size() + 1);
        listenersToCall.add(listener);
        ActionListener<Void> l;
        while ((l = delayedListeners.poll()) != null) {
            listenersToCall.add(l);
        }
        pendingDelayedListeners = false;
        ReadLease readLease = new ReadLease(timeout, listener.delegateFailureAndWrap((delegate, lease) -> {
            if (lease.compareTo(startingStateLease) <= 0) {
                assert lease.compareTo(startingStateLease) == 0 : lease + " vs " + startingStateLease;
                ActionListener.onResponse(listenersToCall, null);
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
                        threadPool.generic().execute(() -> ActionListener.onResponse(listenersToCall, null));
                    }

                    @Override
                    public void onClusterServiceClose() {
                        var e = new NodeClosedException(clusterService.localNode());
                        threadPool.generic().execute(() -> ActionListener.onFailure(listenersToCall, e));
                    }

                    @Override
                    public void onTimeout(TimeValue timeout) {
                        var e = new ElasticsearchTimeoutException(
                            Strings.format(
                                "Timed out while verifying node membership, assuming node left the cluster "
                                    + "[timeout=%s, term=%s, nodeLeftGeneration=%s].",
                                timeout,
                                lease.currentTerm(),
                                lease.nodeLeftGeneration()
                            )
                        );
                        threadPool.generic().execute(() -> ActionListener.onFailure(listenersToCall, e));
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
        readLease.run();
    }

    private class ReadLease extends RetryableAction<StatelessElectionStrategy.Lease> {

        private ReadLease(TimeValue timeoutValue, ActionListener<StatelessElectionStrategy.Lease> listener) {
            super(
                logger,
                clusterService.threadPool(),
                TimeValue.timeValueMillis(5),
                timeoutValue,
                listener,
                EsExecutors.DIRECT_EXECUTOR_SERVICE
            );
        }

        @Override
        public void tryAction(ActionListener<StatelessElectionStrategy.Lease> listener) {
            electionStrategy.readLease(listener.delegateFailureAndWrap((delegate, optionalLease) -> {
                if (optionalLease.isEmpty()) {
                    listener.onFailure(new ConcurrentReadLeaseException());
                } else {
                    listener.onResponse(optionalLease.get());
                }
            }));
        }

        @Override
        public boolean shouldRetry(Exception e) {
            return e instanceof ConcurrentReadLeaseException;
        }

        private static class ConcurrentReadLeaseException extends RuntimeException {}
    }

    public ClusterState state() {
        return clusterService.state();
    }

    public ClusterService clusterService() {
        return clusterService;
    }
}
