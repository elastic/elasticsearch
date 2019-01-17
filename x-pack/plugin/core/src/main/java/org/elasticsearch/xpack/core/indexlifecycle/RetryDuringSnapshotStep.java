package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.repositories.IndexId;

import java.util.function.Consumer;
import java.util.function.Predicate;

public abstract class RetryDuringSnapshotStep extends AsyncActionStep {

    public RetryDuringSnapshotStep(StepKey key, StepKey nextStepKey, Client client) {
        super(key, nextStepKey, client);
    }

    @Override
    public void performAction(IndexMetaData indexMetaData, ClusterState currentClusterState, ClusterStateObserver observer, Listener listener) {
        SnapshotExceptionListener snapshotExceptionListener = new SnapshotExceptionListener(indexMetaData.getIndex(), listener, observer);
        performDuringNoSnapshot(indexMetaData, currentClusterState, snapshotExceptionListener);
    }

    /**
     * Method to be performed during which no snapshots for the index are already underway.
     */
    abstract void performDuringNoSnapshot(IndexMetaData indexMetaData, ClusterState currentClusterState, Listener listener);

    /**
     * SnapshotExceptionListener is an injected listener wrapper that checks to see if a particular
     * action failed due to a {@code SnapshotInProgressException}. If it did, then it registers a
     * ClusterStateObserver listener waiting for the next time the snapshot is not running,
     * re-running the step's {@link #performAction(IndexMetaData, ClusterState, ClusterStateObserver, Listener)}
     * method when the snapshot is no longer running.
     */
    class SnapshotExceptionListener implements AsyncActionStep.Listener {
        private final Index index;
        private final Listener originalListener;
        private final ClusterStateObserver observer;

        SnapshotExceptionListener(Index index, Listener originalListener, ClusterStateObserver observer) {
            this.index = index;
            this.originalListener = originalListener;
            this.observer = observer;
        }

        @Override
        public void onResponse(boolean complete) {
            originalListener.onResponse(complete);
        }

        @Override
        public void onFailure(Exception e) {
//            if (e instanceof SnapshotInProgressException) {
            if (e instanceof IllegalArgumentException) {
                observer.waitForNextChange(
                    new NoSnapshotRunningListener(state -> {
                        IndexMetaData idxMeta = state.metaData().index(index);
                        if (idxMeta == null) {
                            // The index has since been deleted, mission accomplished!
                            originalListener.onResponse(true);
                        }
                        // Re-invoke the performAction method with the new state
                        performAction(idxMeta, state, observer, originalListener);
                    }, originalListener::onFailure),
                    new WaitForNoSnapshotPredicate(index.getName()));
            }
        }
    }

    /**
     * A {@link ClusterStateObserver.Listener} that invokes the given function with the new state,
     * passing any exceptions to the original exception listener if they occur.
     */
    class NoSnapshotRunningListener implements ClusterStateObserver.Listener {

        private final Consumer<ClusterState> reRun;
        private final Consumer<Exception> listener;

        NoSnapshotRunningListener(Consumer<ClusterState> reRun, Consumer<Exception> listener) {
            this.reRun = reRun;
            this.listener = listener;
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            try {
                reRun.accept(state);
            } catch (Exception e) {
                listener.accept(e);
            }
        }

        @Override
        public void onClusterServiceClose() {
            // This means the cluster is being shut down, so nothing to do here
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            listener.accept(new IllegalStateException("step timed out while waiting for snapshots to complete"));
        }
    }

    /**
     * A predicate waiting for no snapshot to be executing for a given index name
     */
    class WaitForNoSnapshotPredicate implements Predicate<ClusterState> {
        private final String indexName;

        WaitForNoSnapshotPredicate(String indexName) {
            this.indexName = indexName;
        }

        @Override
        public boolean test(ClusterState state) {
            SnapshotsInProgress snapshotsInProgress = state.custom(SnapshotsInProgress.TYPE);
            if (snapshotsInProgress == null || snapshotsInProgress.entries().isEmpty()) {
                // No snapshots are running, new state is acceptable to proceed
                return true;
            }

            for (SnapshotsInProgress.Entry snapshot : snapshotsInProgress.entries()) {
                if (snapshot.indices().stream()
                    .map(IndexId::getName)
                    .anyMatch(name -> name.equals(indexName))) {
                    // There is a snapshot running with this index name
                    return false;
                }
            }
            // There are snapshots, but none for this index, so it's okay to proceed with this state
            return true;
        }
    }
}
