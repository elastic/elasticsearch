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

import co.elastic.elasticsearch.stateless.objectstore.ObjectStoreService;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.RetryableAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.List;

/**
 * Listens for cluster state changes reflecting the local node has become master and, if such a
 * change occurs, schedules a delayed cleanup task to delete previous master state from the remote
 * blob storage.
 *
 * The blob store holds cluster state in per term directories. So a new master will initialize a
 * new term directory and the old term directories can be safely deleted.
 */
public class StatelessClusterStateCleanupService implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(StatelessClusterStateCleanupService.class);

    // Previous term cluster state is not deleted immediately in order to accommodate a previous
    // master node not promptly learning of a new master node. We hope to ensure that any concurrent
    // cluster state writes to previous master/term data directories have ceased by waiting a little
    // time. It is okay if a previous master does write to the blob store concurrently with or after
    // the cleanup task runs: only the previous term state will be potentially retained and the next
    // master will eventually run another cleanup task. A 1-minute delay is arbitrarily chosen.
    public static final Setting<TimeValue> CLUSTER_STATE_CLEANUP_DELAY_SETTING = Setting.timeSetting(
        "stateless.cluster.cluster_state_cleanup.delay",
        TimeValue.timeValueMinutes(1), // Default value
        TimeValue.timeValueSeconds(1), // Min value
        TimeValue.timeValueMinutes(5), // Max value
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );
    private volatile TimeValue clusterStateCleanupDelay;

    /**
     * NB: setting a {@link #RETRY_INITIAL_DELAY_SETTING} value > the {@link #RETRY_MAX_DELAY_SETTING}
     * value will cause an IllegalArgumentException in {@link RetryableAction#RetryableAction}.
     */
    public static final Setting<TimeValue> RETRY_INITIAL_DELAY_SETTING = Setting.timeSetting(
        "stateless.cluster.cluster_state_cleanup.retry_initial_delay",
        TimeValue.timeValueSeconds(1) /* Default value */,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> RETRY_MAX_DELAY_SETTING = Setting.timeSetting(
        "stateless.cluster.cluster_state_cleanup.retry_max_delay",
        TimeValue.timeValueSeconds(10) /* Default value */,
        Setting.Property.NodeScope
    );
    public static final Setting<TimeValue> RETRY_TIMEOUT_SETTING = Setting.timeSetting(
        "stateless.cluster.cluster_state_cleanup.retry_timeout",
        TimeValue.timeValueMinutes(2) /* Default value */,
        Setting.Property.NodeScope
    );
    private final TimeValue retryInitialDelayOnFirstRetry;
    private final TimeValue retryMaxDelayBetweenRetries;
    private final TimeValue retryFinalTimeout;

    private final ThreadPool threadPool;
    private final ObjectStoreService objectStoreService;
    private final ClusterService clusterService;
    private boolean scheduleCleanup = false;

    public StatelessClusterStateCleanupService(
        ThreadPool threadPool,
        ObjectStoreService objectStoreService,
        ClusterService clusterService
    ) {
        this.threadPool = threadPool;
        this.objectStoreService = objectStoreService;
        this.clusterService = clusterService;
        this.retryInitialDelayOnFirstRetry = RETRY_INITIAL_DELAY_SETTING.get(clusterService.getSettings());
        this.retryMaxDelayBetweenRetries = RETRY_MAX_DELAY_SETTING.get(clusterService.getSettings());
        this.retryFinalTimeout = RETRY_TIMEOUT_SETTING.get(clusterService.getSettings());

        // Set up a listener to apply dynamic setting changes to this.clusterStateCleanupDelay.
        clusterService.getClusterSettings()
            .initializeAndWatch(CLUSTER_STATE_CLEANUP_DELAY_SETTING, value -> this.clusterStateCleanupDelay = value);

        // Create a RetryableAction momentarily to verify that the Settings passed as parameters into its constructor are valid.
        new ClusterStateCleanupRetryableAction(-1);
    }

    /**
     * Schedules a delayed task to clean up previous term/master cluster state in the remote
     * blob store, iff this node has newly become the master of the cluster.
     *
     * @param event information about the new and previous cluster state
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (event.localNodeMaster() == false) {
            if (scheduleCleanup) {
                scheduleCleanup = false;
                logger.debug("Cancelling cluster state cleanup task because no longer master.");
            }
            return;
        }

        // The local node has become master, a cleanup task should be scheduled to remove old cluster state.
        if (event.localNodeMaster() && event.nodesDelta().masterNodeChanged()) {
            scheduleCleanup = true;
        }

        // If the node is still in a recovering state, then the cluster state settings have not yet been applied.
        // So the cleanup task will not be scheduled until after possible changes to CLUSTER_STATE_CLEANUP_DELAY_SETTING
        // have been applied. There will be a future clusterChanged event for the application of the settings.
        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            return;
        }

        if (scheduleCleanup) {
            this.threadPool.schedule(() -> {
                // Retries cleanup with incremental backoff on error until a final timeout is reached.
                new ClusterStateCleanupRetryableAction(event.state().term()).run();
            }, clusterStateCleanupDelay, threadPool.generic());

            scheduleCleanup = false;
        }
    }

    /**
     * Attempts to delete cluster state associated with master terms earlier than the given term.
     */
    private void removePreviousTermsClusterStateFromBlobStore(long currentTerm) throws IOException {
        assert currentTerm > 0;

        if (clusterService.state().term() > currentTerm) {
            logger.info("Cancelling cluster state cleanup task because there is a newer master term.");
            return;
        }

        // The cluster state directory in the blob store is expected to contain a 'lease' file blob and 'heartbeat/'
        // directory, and then monotonically increasing numbered, per term, directories (1/, 2/, etc.) for each master
        // term's cluster state. So we'll filter out everything except the term directories earlier than the local master
        // election term that scheduled this cleanup task. Note: the 'lease' file blob is a file, not a child directory.
        List<Long> oldClusterStateTerms = this.objectStoreService.getClusterStateBlobContainer()
            .children(OperationPurpose.CLUSTER_STATE)
            .keySet()
            .stream()
            .filter(key -> key.equals(StatelessHeartbeatStore.HEARTBEAT_BLOB) == false)
            .map(Long::parseLong)
            .filter(term -> term < currentTerm)
            .toList();

        logger.debug("Attempting to delete old cluster state for terms earlier than term " + currentTerm);
        for (var oldClusterStateTerm : oldClusterStateTerms) {
            this.objectStoreService.getClusterStateBlobContainerForTerm(oldClusterStateTerm).delete(OperationPurpose.CLUSTER_STATE);
        }
    }

    /**
     * Retries {@link #removePreviousTermsClusterStateFromBlobStore(long)} in case of failure.
     * Continues to retry using an incremental backoff delay until a final timeout is reached.
     */
    private class ClusterStateCleanupRetryableAction extends RetryableAction<Void> {
        final long currentTerm;

        ClusterStateCleanupRetryableAction(long currentTerm) {
            super(
                org.apache.logging.log4j.LogManager.getLogger(StatelessClusterStateCleanupService.class),
                threadPool,
                retryInitialDelayOnFirstRetry,
                retryMaxDelayBetweenRetries,
                retryFinalTimeout,
                new ActionListener<Void>() { /* listener for when there is either success or final failure after timeout */
                    @Override
                    public void onResponse(Void unused) {
                        logger.debug("Successfully deleted old cluster state for terms earlier than term " + currentTerm);
                    }

                    @Override
                    public void onFailure(Exception e) {
                        logger.warn("Failed to delete old cluster state for terms earlier than term " + currentTerm, e);
                    }
                },
                threadPool.generic()
            );
            this.currentTerm = currentTerm;
        }

        @Override
        public void tryAction(ActionListener<Void> listener) {
            ActionListener.completeWith(listener, () -> {
                removePreviousTermsClusterStateFromBlobStore(this.currentTerm);
                return null;
            });
        }

        @Override
        public boolean shouldRetry(Exception e) {
            // Always retry on errors. This is only a single thread that retries with delays and will eventually time out, so load is
            // negligible. Ideally this would only retry on IO errors, but not all transient blob store errors are caught and converted to
            // IOException.
            return true;
        }
    }
}
